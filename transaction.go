package turing

import (
	"errors"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble"

	"github.com/256dpi/turing/pkg/coding"
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
	reader    pebble.Reader
	writer    pebble.Writer
	closers   int
	iterators int
	effect    int
}

func (t *Transaction) execute(ins Instruction) (bool, error) {
	// execute transaction
	var exhausted bool
	err := ins.Execute(t)
	if err == ErrMaxEffect {
		exhausted = true
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

	return exhausted, nil
}

// Get will lookup the specified key. The returned slice must not be modified by
// the caller. A closer is returned that must be closed once the value is not
// used anymore. Consider Use or Copy for safer alternatives.
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

	// decode value (no need to clone as available until closed)
	var value Value
	err = value.Decode(bytes, false)
	if err != nil {
		_ = closer.Close()
		return nil, false, nil, err
	}

	// TODO: Release.

	// compute value
	value, _, err = ComputeValue(value, t.registry)
	if err != nil {
		_ = closer.Close()
		return nil, false, nil, err
	}

	// increment closers
	t.closers++

	// wrap closer
	wrappedCloser := closerFunc(func() error {
		t.closers--
		return closer.Close()
	})

	return value.Value, true, wrappedCloser, nil
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
func (t *Transaction) Set(key, val []byte) error {
	// check writer
	if t.writer == nil {
		return ErrReadOnly
	}

	// check effect
	if t.effect >= MaxEffect {
		return ErrMaxEffect
	}

	// prepare value
	value := Value{
		Kind:  FullValue,
		Value: val,
	}

	// encode value
	ev, evr, err := value.Encode(true)
	if err != nil {
		return err
	}

	// ensure release
	defer evr.Release()

	// prefix key
	pk, pkr := prefixUserKey(key)
	defer pkr.Release()

	// set value
	err = t.writer.Set(pk, ev, nil)
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
func (t *Transaction) Merge(key, val []byte, operator *Operator) error {
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

	// TODO: Check if instruction registered operator?

	// prepare value
	value := Value{
		Kind: StackValue,
		Stack: []Operand{{
			Name:  operator.Name,
			Value: val,
		}},
	}

	// encode value
	ev, evr, err := value.Encode(true)
	if err != nil {
		return err
	}

	// ensure release
	defer evr.Release()

	// prefix key
	pk, pkr := prefixUserKey(key)
	defer pkr.Release()

	// merge value
	err = t.writer.Merge(pk, ev, nil)
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

// Key will return the current key. Unless copy is true, the key is only valid
// until the next call of Next().
func (i *Iterator) Key() []byte {
	// get key
	key := trimUserKey(i.iter.Key())
	if len(key) == 0 {
		return nil
	}

	return key
}

// Value will return the current value. Unless copy is true, the value is only
// valid until the next call of Next().
func (i *Iterator) Value() ([]byte, error) {
	// get value
	bytes := i.iter.Value()
	if len(bytes) == 0 {
		return nil, nil
	}

	// decode value (no need to clone as copying is explicit)
	var value Value
	err := value.Decode(bytes, false)
	if err != nil {
		return nil, err
	}

	// TODO: Release.

	// compute value
	value, _, err = ComputeValue(value, i.txn.registry)
	if err != nil {
		return nil, err
	}

	// set bytes
	bytes = value.Value

	return bytes, nil
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
