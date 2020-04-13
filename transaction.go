package turing

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/cockroachdb/pebble"

	"github.com/256dpi/turing/coding"
	"github.com/256dpi/turing/tape"
)

// TODO: Can a transaction be used concurrently?

var userPrefix = []byte("#")

// ErrReadOnly is returned by by a transaction on write operations if the
// instruction has been flagged as read only.
var ErrReadOnly = errors.New("turing: read only")

// ErrMaxEffect is returned by a transaction if the effect limit has been
// reached. The instruction should return with this error to have the current
// changes persistent and be executed again to persist the remaining changes.
var ErrMaxEffect = errors.New("turing: max effect")

type transaction struct {
	registry  *registry
	current   Instruction
	reader    pebble.Reader
	writer    pebble.Writer
	closers   int
	iterators int
	effect    int
}

var transactionPool = sync.Pool{
	New: func() interface{} {
		return &transaction{}
	},
}

func newTransaction() *transaction {
	return transactionPool.Get().(*transaction)
}

func recycleTransaction(txn *transaction) {
	txn.registry = nil
	txn.current = nil
	txn.reader = nil
	txn.writer = nil
	txn.closers = 0
	txn.iterators = 0
	txn.effect = 0
	transactionPool.Put(txn)
}

// Execute will execute the specified instruction as part of this transaction.
// It will return whether the instruction maxed the transaction effect. If so,
// the calling instruction should return ErrMaxEffect and call the instruction
// again in the following execution.
func (t *transaction) Execute(ins Instruction) (bool, error) {
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

func (t *transaction) Get(key []byte) ([]byte, bool, io.Closer, error) {
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

func (t *transaction) Use(key []byte, fn func(value []byte) error) error {
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

func (t *transaction) Set(key, value []byte) error {
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

func (t *transaction) Unset(key []byte) error {
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

func (t *transaction) Delete(start, end []byte) error {
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

func (t *transaction) Merge(key, value []byte, operator *Operator) error {
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

func (t *transaction) Effect() int {
	return t.effect
}

func (t *transaction) Iterate(prefix []byte) Iterator {
	// increment iterators
	t.iterators++

	// prefix prefix
	pk, pkr := prefixUserKey(prefix)

	return &iterator{
		txn:  t,
		pkr:  pkr,
		iter: t.reader.NewIter(prefixIterator(pk)),
	}
}

type iterator struct {
	txn  *transaction
	pkr  Ref
	iter *pebble.Iterator
}

func (i *iterator) SeekGE(key []byte) bool {
	// prepare prefix
	pKey, pKeyRef := prefixUserKey(key)
	defer pKeyRef.Release()

	return i.iter.SeekGE(pKey)
}

func (i *iterator) SeekLT(key []byte) bool {
	// prepare prefix
	pKey, pKeyRef := prefixUserKey(key)
	defer pKeyRef.Release()

	return i.iter.SeekLT(pKey)
}

func (i *iterator) First() bool {
	return i.iter.First()
}

func (i *iterator) Last() bool {
	return i.iter.Last()
}

func (i *iterator) Valid() bool {
	return i.iter.Valid()
}

func (i *iterator) Next() bool {
	return i.iter.Next()
}

func (i *iterator) Prev() bool {
	return i.iter.Prev()
}

func (i *iterator) Key() ([]byte, Ref) {
	return coding.Clone(i.TempKey())
}

func (i *iterator) TempKey() []byte {
	// get key
	key := trimUserKey(i.iter.Key())
	if len(key) == 0 {
		return nil
	}

	return key
}

func (i *iterator) Value() ([]byte, Ref, error) {
	// get value
	bytes := i.iter.Value()
	if len(bytes) == 0 {
		return nil, noopRef, nil
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

func (i *iterator) Error() error {
	return i.iter.Error()
}

func (i *iterator) Close() error {
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
