package turing

import (
	"errors"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble"

	"github.com/256dpi/turing/coding"
	"github.com/256dpi/turing/tape"
)

// TODO: Can a transaction be used concurrently?

var userPrefix = []byte("#")

type closerFunc func() error

func (f closerFunc) Close() error {
	return f()
}

var noopCloser = closerFunc(func() error {
	return nil
})

// ErrReadOnly is returned by by a transaction on write operations if the
// instruction has been flagged as read only.
var ErrReadOnly = errors.New("turing: read only")

// ErrMaxEffect is returned by a transaction if the effect limit has been
// reached. The instruction should return with this error to have the current
// changes persistent and be executed again to persist the remaining changes.
var ErrMaxEffect = errors.New("turing: max effect")

// Transaction is used by an instruction to perform changes to the database.
type Transaction struct {
	registry  *registry
	current   Instruction
	reader    pebble.Reader
	writer    pebble.Writer
	closers   int
	iterators int
	effect    int
}

// Execute will execute the specified instruction as part of this transaction.
// It will return whether the instruction maxed the transaction effect. If so,
// the calling instruction should return ErrMaxEffect and call the instruction
// again in the following execution.
func (t *Transaction) Execute(ins Instruction) (bool, error) {
	// set instruction
	t.current = ins

	// execute transaction
	var effectMaxed bool
	err := ins.Execute(t)
	if err == ErrMaxEffect {
		effectMaxed = true
	} else if err != nil {
		return false, err
	}

	// check closers
	if t.closers != 0 {
		return false, fmt.Errorf("turing: unclosed closers after instruction execution")
	}

	// check iterators
	if t.iterators != 0 {
		return false, fmt.Errorf("turing: unclosed iterators after instruction execution")
	}

	return effectMaxed, nil
}

// Get will lookup the specified key. The returned slice must not be modified by
// the caller. A closer is returned that must be closed once the value is not
// used anymore. Consider using Use() if the value is only used temporarily.
func (t *Transaction) Get(key []byte) ([]byte, bool, io.Closer, error) {
	// prefix key
	pk, pkr := prefixUserKey(key)
	defer pkr.Release()

	// get value
	bytes, closer, err := t.reader.Get(pk)
	if err == pebble.ErrNotFound {
		return nil, false, noopCloser, nil
	} else if err != nil {
		return nil, false, nil, err
	}

	// decode cell
	var cell tape.Cell
	err = cell.Decode(bytes, false)
	if err != nil {
		_ = closer.Close()
		return nil, false, nil, err
	}

	// directly return raw cell
	if cell.Type == tape.RawCell {
		// increment closers
		t.closers++

		// wrap closer
		wrappedCloser := closerFunc(func() error {
			t.closers--
			return closer.Close()
		})

		return cell.Value, true, wrappedCloser, nil
	}

	// cell is a stack cell

	// ensure close
	defer closer.Close()

	// resolve cell
	computer := newComputer(t.registry)
	result, ref, err := computer.resolve(cell)
	if err != nil {
		return nil, false, nil, err
	}

	// increment closers
	t.closers++

	// prepare ref closer
	refCloser := closerFunc(func() error {
		t.closers--
		ref.Release()
		return nil
	})

	return result.Value, true, refCloser, nil
}

// Use will lookup the specified key and yield it to the provided function if it
// exists.
func (t *Transaction) Use(key []byte, fn func(value []byte) error) error {
	// get value
	value, found, closer, err := t.Get(key)
	if err != nil {
		return err
	} else if !found {
		return nil
	}

	// yield value
	err = fn(value)
	if err != nil {
		_ = closer.Close()
		return err
	}

	// close value
	err = closer.Close()
	if err != nil {
		return err
	}

	return err
}

// Set will set the specified key to the new value. This operation will count as
// one towards the effect of the transaction.
func (t *Transaction) Set(key, value []byte) error {
	// check writer
	if t.writer == nil {
		return ErrReadOnly
	}

	// check effect
	if t.effect >= MaxEffect {
		return ErrMaxEffect
	}

	// prepare cell
	cell := tape.Cell{
		Type:  tape.RawCell,
		Value: value,
	}

	// encode cell
	cellValue, cellRef, err := cell.Encode(true)
	if err != nil {
		return err
	}

	// ensure release
	defer cellRef.Release()

	// prefix key
	pk, pkr := prefixUserKey(key)
	defer pkr.Release()

	// set value
	err = t.writer.Set(pk, cellValue, nil)
	if err != nil {
		return err
	}

	// increment effect
	t.effect++

	return nil
}

// Unset will remove the specified key. This operation will count as one towards
// the effect of the transaction.
func (t *Transaction) Unset(key []byte) error {
	// check writer
	if t.writer == nil {
		return ErrReadOnly
	}

	// check effect
	if t.effect >= MaxEffect {
		return ErrMaxEffect
	}

	// prefix key
	pk, pkr := prefixUserKey(key)
	defer pkr.Release()

	// delete key
	err := t.writer.Delete(pk, nil)
	if err != nil {
		return err
	}

	// increment effect
	t.effect++

	return nil
}

// Delete deletes all of the keys in the range [start, end] (inclusive on start,
// exclusive on end). This operation will count as one towards the effect of the
// transaction.
func (t *Transaction) Delete(start, end []byte) error {
	// check writer
	if t.writer == nil {
		return ErrReadOnly
	}

	// check effect
	if t.effect >= MaxEffect {
		return ErrMaxEffect
	}

	// prefix keys
	sk, skr := prefixUserKey(start)
	ek, ekr := prefixUserKey(end)
	defer skr.Release()
	defer ekr.Release()

	// delete range
	err := t.writer.DeleteRange(sk, ek, nil)
	if err != nil {
		return err
	}

	// increment effect
	t.effect++

	return nil
}

// Merge merges existing values with the provided value using the specified
// operator.
func (t *Transaction) Merge(key, value []byte, operator *Operator) error {
	// check writer
	if t.writer == nil {
		return ErrReadOnly
	}

	// check effect
	if t.effect >= MaxEffect {
		return ErrMaxEffect
	}

	// check registry
	if t.registry.ops[operator.Name] == nil {
		return fmt.Errorf("turing: unknown operator: %s", operator.Name)
	}

	// check registration
	var registered bool
	for _, op := range t.current.Describe().Operators {
		if op == operator {
			registered = true
		}
	}
	if !registered {
		return fmt.Errorf("turing: unregistered operator: %s", operator.Name)
	}

	// prepare stack
	stack := tape.Stack{
		Operands: []tape.Operand{{
			Name:  operator.Name,
			Value: value,
		}},
	}

	// encode stack
	stackValue, stackRef, err := stack.Encode(true)
	if err != nil {
		return err
	}

	// ensure release
	defer stackRef.Release()

	// prepare cell
	cell := tape.Cell{
		Type:  tape.StackCell,
		Value: stackValue,
	}

	// encode cell
	cellValue, cellRef, err := cell.Encode(true)
	if err != nil {
		return err
	}

	// ensure release
	defer cellRef.Release()

	// prefix key
	pk, pkr := prefixUserKey(key)
	defer pkr.Release()

	// merge value
	err = t.writer.Merge(pk, cellValue, nil)
	if err != nil {
		return err
	}

	// increment effect
	t.effect++

	return nil
}

// Effect will return the current effect of the transaction.
func (t *Transaction) Effect() int {
	return t.effect
}

// Iterator will construct and return a new iterator. The iterator must be
// closed as soon as it is not used anymore. There can be only one iterator
// created at a time.
func (t *Transaction) Iterator(prefix []byte) *Iterator {
	// increment iterators
	t.iterators++

	// prefix prefix
	pk, pkr := prefixUserKey(prefix)

	return &Iterator{
		txn:  t,
		pkr:  pkr,
		iter: t.reader.NewIter(prefixIterator(pk)),
	}
}

// Iterator is used to iterate over the key space of the database.
type Iterator struct {
	txn  *Transaction
	pkr  Ref
	iter *pebble.Iterator
}

// SeekGE will seek to the exact key or the next greater key.
func (i *Iterator) SeekGE(key []byte) bool {
	// prepare prefix
	pKey, pKeyRef := prefixUserKey(key)
	defer pKeyRef.Release()

	return i.iter.SeekGE(pKey)
}

// SeekLT will seek to the exact key or the next smaller key.
func (i *Iterator) SeekLT(key []byte) bool {
	// prepare prefix
	pKey, pKeyRef := prefixUserKey(key)
	defer pKeyRef.Release()

	return i.iter.SeekLT(pKey)
}

// First will seek to the first key in the range.
func (i *Iterator) First() bool {
	return i.iter.First()
}

// Last will seek to the last key in the range.
func (i *Iterator) Last() bool {
	return i.iter.Last()
}

// Valid will return whether a valid key/value pair is present.
func (i *Iterator) Valid() bool {
	return i.iter.Valid()
}

// Next will move on to the next key.
func (i *Iterator) Next() bool {
	return i.iter.Next()
}

// Prev will go back to the previous key.
func (i *Iterator) Prev() bool {
	return i.iter.Prev()
}

// Key will return a buffered key that can be used until released.
func (i *Iterator) Key() ([]byte, Ref) {
	return coding.Clone(i.TempKey())
}

// TempKey will return the temporary key which is only valid until the next
// iteration or the iterator is closed.
func (i *Iterator) TempKey() []byte {
	// get key
	key := trimUserKey(i.iter.Key())
	if len(key) == 0 {
		return nil
	}

	return key
}

// Value will return a buffered value that can be used until released.
func (i *Iterator) Value() ([]byte, Ref, error) {
	// get value
	bytes := i.iter.Value()
	if len(bytes) == 0 {
		return nil, NoopRef, nil
	}

	// decode cell (no need to clone as copying is explicit)
	var cell tape.Cell
	err := cell.Decode(bytes, false)
	if err != nil {
		return nil, nil, err
	}

	// copy and return raw cell
	if cell.Type == tape.RawCell {
		val, ref := coding.Clone(cell.Value)
		return val, ref, nil
	}

	// resolve cell
	computer := newComputer(i.txn.registry)
	result, ref, err := computer.resolve(cell)
	if err != nil {
		return nil, nil, err
	}

	return result.Value, ref, nil
}

// Error will return the error
func (i *Iterator) Error() error {
	return i.iter.Error()
}

// Close will close the iterator.
func (i *Iterator) Close() error {
	// decrement iterators
	i.txn.iterators--

	// release prefix
	defer i.pkr.Release()

	// close iterator
	err := i.iter.Close()
	if err != nil {
		return err
	}

	return nil
}

func prefixUserKey(key []byte) ([]byte, Ref) {
	return coding.Concat(userPrefix, key)
}

func trimUserKey(key []byte) []byte {
	// trim if not empty
	if len(key) > 0 {
		return key[1:]
	}

	return key
}

func prefixIterator(prefix []byte) *pebble.IterOptions {
	low, up := PrefixRange(prefix)

	return &pebble.IterOptions{
		LowerBound: low,
		UpperBound: up,
	}
}
