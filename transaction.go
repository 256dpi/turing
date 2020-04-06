package turing

import (
	"errors"
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
	reader  pebble.Reader
	writer  pebble.Writer
	closers int
	effect  int
}

// Get will lookup the specified key. The returned slice must not be modified by
// the caller. A closer is returned that must be closed once the value is not
// used anymore. Consider Use or Copy for better safety.
func (t *Transaction) Get(key []byte) ([]byte, bool, io.Closer, error) {
	// get value
	value, closer, err := t.reader.Get(prefixUserKey(key))
	if err == pebble.ErrNotFound {
		return nil, false, nil, nil
	} else if err != nil {
		return nil, false, nil, err
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
	value, closer, err := t.reader.Get(prefixUserKey(key))
	if err == pebble.ErrNotFound {
		return nil
	} else if err != nil {
		return err
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
	value, closer, err := t.reader.Get(prefixUserKey(key))
	if err == pebble.ErrNotFound {
		return nil, false, nil
	} else if err != nil {
		return nil, false, err
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
func (t *Transaction) Set(key, value []byte) error {
	// check writer
	if t.writer == nil {
		return ErrReadOnly
	}

	// set key to value
	err := t.writer.Set(prefixUserKey(key), value, nil)
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

// Effect will return the current effect of the transaction.
func (t *Transaction) Effect() int {
	return t.effect
}

// Iterator will construct and return a new iterator. The iterator must be
// closed as soon as it is not used anymore. There can be only one iterator
// created at a time.
func (t *Transaction) Iterator(prefix []byte) *Iterator {
	return &Iterator{
		iter: t.reader.NewIter(prefixIterator(prefixUserKey(prefix))),
	}
}

// Iterator is used to iterate over the key space of the database.
type Iterator struct {
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
	if key == nil {
		return nil
	}

	// make copy
	if copy {
		key = Copy(nil, key)
	}

	return key
}

// Value will return the current value.
func (i *Iterator) Value(copy bool) []byte {
	// get value
	value := i.iter.Value()
	if value == nil {
		return nil
	}

	// make copy
	if copy {
		value = Copy(nil, value)
	}

	return value
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
