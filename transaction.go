package turing

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/cockroachdb/pebble"
)

// TODO: Can a transaction be used concurrently?

type closerFunc func() error

func (f closerFunc) Close() error {
	return f()
}

var txnPool = sync.Pool{
	New: func() interface{} {
		return &Transaction{}
	},
}

func obtainTxn() *Transaction {
	return txnPool.Get().(*Transaction)
}

func recycleTxn(txn *Transaction) {
	txn.registry = nil
	txn.reader = nil
	txn.writer = nil
	txn.closers = 0
	txn.effect = 0
	txnPool.Put(txn)
}

// ErrReadOnly is returned by by a transaction on write operations if the
// instruction has been flagged as read only.
var ErrReadOnly = errors.New("read only")

// ErrMaxEffect is returned by a transaction if the effect limit has been
// reached. The instruction should return with this error to have the current
// changes persistent and be executed again to persist the remaining changes.
var ErrMaxEffect = errors.New("max effect")

// Transaction is used by an instruction to perform changes to the database.
type Transaction struct {
	registry *registry
	reader   pebble.Reader
	writer   pebble.Writer
	closers  int
	effect   int
}

// Get will lookup the specified key. The returned slice must not be modified by
// the caller. A closer is returned that must be closed once the value is not
// used anymore. Consider Use or Copy for better safety.
func (t *Transaction) Get(key []byte) ([]byte, bool, io.Closer, error) {
	// get value
	value, found, closer, err := t.get(key)
	if err != nil {
		return nil, false, nil, err
	} else if !found {
		return nil, false, nil, nil
	}

	// increment
	t.closers++

	// wrap closer
	wrappedCloser := closerFunc(func() error {
		t.closers--
		return closer.Close()
	})

	return value, true, wrappedCloser, nil
}

// Use will lookup the specified key and yield it to the provided function if it
// exists.
func (t *Transaction) Use(key []byte, fn func(value []byte) error) error {
	// get value
	value, found, closer, err := t.get(key)
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

// Copy will lookup the specified key and return a copy if it exists.
func (t *Transaction) Copy(key []byte) ([]byte, bool, error) {
	// get value
	value, found, closer, err := t.get(key)
	if err != nil {
		return nil, false, err
	} else if !found {
		return nil, false, nil
	}

	// copy value
	value = Copy(nil, value)

	// close value
	err = closer.Close()
	if err != nil {
		return nil, false, err
	}

	return value, true, nil
}

// Set will set the specified key to the new value. This operation will count as
// one towards the effect of the transaction.
func (t *Transaction) Set(key, val []byte) error {
	// check writer
	if t.writer == nil {
		return ErrReadOnly
	}

	// encode value
	bytes, err := EncodeValue(Value{
		Kind:  FullValue,
		Value: val,
	})
	if err != nil {
		return err
	}

	// set key to value
	err = t.writer.Set(prefixUserKey(key), bytes, nil)
	if err != nil {
		return err
	}

	// check effect
	if t.effect >= MaxEffect {
		return ErrMaxEffect
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

	// delete key
	err := t.writer.Delete(prefixUserKey(key), nil)
	if err != nil {
		return err
	}

	// check effect
	if t.effect >= MaxEffect {
		return ErrMaxEffect
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

	// delete range
	err := t.writer.DeleteRange(prefixUserKey(start), prefixUserKey(end), nil)
	if err != nil {
		return err
	}

	// check effect
	if t.effect >= MaxEffect {
		return ErrMaxEffect
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

	// check registry
	if t.registry.operators[operator.Name] == nil {
		// TODO: Make sure current instruction registered operator.
		return fmt.Errorf("unknown operator: %s", operator.Name)
	}

	// encode value
	bytes, err := EncodeValue(Value{
		Kind: StackValue,
		Stack: []Operand{{
			Name:  operator.Name,
			Value: val,
		}},
	})
	if err != nil {
		return err
	}

	// set key to value
	err = t.writer.Merge(prefixUserKey(key), bytes, nil)
	if err != nil {
		return err
	}

	// check effect
	if t.effect >= MaxEffect {
		return ErrMaxEffect
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
	return &Iterator{
		txn:  t,
		iter: t.reader.NewIter(prefixIterator(prefixUserKey(prefix))),
	}
}

func (t *Transaction) get(key []byte) ([]byte, bool, io.Closer, error) {
	// get value
	bytes, closer, err := t.reader.Get(prefixUserKey(key))
	if err == pebble.ErrNotFound {
		return nil, false, nil, nil
	} else if err != nil {
		return nil, false, nil, err
	}

	// parse value
	value, err := DecodeValue(bytes)
	if err != nil {
		_ = closer.Close()
		return nil, false, nil, err
	}

	// resolve value
	value, err = ComputeValue(value, t.registry)
	if err != nil {
		_ = closer.Close()
		return nil, false, nil, err
	}

	return value.Value, true, closer, nil
}

// Iterator is used to iterate over the key space of the database.
type Iterator struct {
	txn  *Transaction
	iter *pebble.Iterator
}

// SeekGE will seek to the exact key or the next greater key.
func (i *Iterator) SeekGE(key []byte) bool {
	return i.iter.SeekGE(prefixUserKey(key))
}

// SeekLT will seek to the exact key or the next smaller key.
func (i *Iterator) SeekLT(key []byte) bool {
	return i.iter.SeekLT(prefixUserKey(key))
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

// Key will return the current key.
func (i *Iterator) Key(copy bool) []byte {
	// get key
	key := trimUserKey(i.iter.Key())
	if len(key) == 0 {
		return nil
	}

	// make copy
	if copy {
		key = Copy(nil, key)
	}

	return key
}

// Value will return the current value.
func (i *Iterator) Value(copy bool) ([]byte, error) {
	// get value
	bytes := i.iter.Value()
	if len(bytes) == 0 {
		return nil, nil
	}

	// parse value
	value, err := DecodeValue(bytes)
	if err != nil {
		return nil, err
	}

	// resolve value
	value, err = ComputeValue(value, i.txn.registry)
	if err != nil {
		return nil, err
	}

	// set bytes
	bytes = value.Value

	// make copy
	if copy {
		bytes = Copy(nil, bytes)
	}

	return bytes, nil
}

// Error will return the error
func (i *Iterator) Error() error {
	return i.iter.Error()
}

// Close will close the iterator.
func (i *Iterator) Close() error {
	return i.iter.Close()
}

func prefixUserKey(key []byte) []byte {
	return append([]byte{'#'}, key...)
}

func trimUserKey(key []byte) []byte {
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
