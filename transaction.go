package turing

import (
	"errors"

	"github.com/dgraph-io/badger"
)

// ErrMaxEffect is returned by a transaction if the effect limit has been
// reached. The instruction should return with this error to have the current
// changes persistent and be executed again to persist the remaining changes.
var ErrMaxEffect = errors.New("max effect")

// Transaction is used by an instruction to perform changes to the data store.
type Transaction struct {
	txn    *badger.Txn
	effect int
}

// Get will lookup the specified key.
func (t *Transaction) Get(key []byte) (*Pair, error) {
	// get item
	item, err := t.txn.Get(userPrefix(key))
	if err == badger.ErrKeyNotFound {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	// create pair
	p := &Pair{
		item: item,
	}

	return p, nil
}

// Set will set the specified key to the new value. This operation will counter
// towards the effect of the transaction.
func (t *Transaction) Set(key, value []byte) error {
	// set key to value
	err := t.txn.Set(userPrefix(key), value)
	if err == badger.ErrTxnTooBig {
		return ErrMaxEffect
	} else if err != nil {
		return err
	}

	// increment effect
	t.effect++

	return nil
}

// Delete will delete the specified key. This operation will counter towards the
// effect of the transaction.
func (t *Transaction) Delete(key []byte) error {
	// delete key
	err := t.txn.Delete(userPrefix(key))
	if err == badger.ErrTxnTooBig {
		return ErrMaxEffect
	} else if err != nil {
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
func (t *Transaction) Iterator(prefix []byte, prefetch, reverse bool) *Iterator {
	return &Iterator{
		iter: t.txn.NewIterator(badger.IteratorOptions{
			Prefix:         userPrefix(prefix),
			PrefetchValues: prefetch,
			PrefetchSize:   100, // recommended value
			Reverse:        reverse,
		}),
	}
}

// Pair is a single key value pair.
type Pair struct {
	item *badger.Item
}

// Key will return the key of the pair.
func (p *Pair) Key() []byte {
	return userTrim(p.item.Key())
}

// CopyKey will create a copy of the key.
func (p *Pair) CopyKey(buf []byte) []byte {
	return userTrim(p.item.KeyCopy(buf))
}

// LoadValue will load the value and run the provided callback when it is
// available.
func (p *Pair) LoadValue(fn func([]byte) error) error {
	return p.item.Value(fn)
}

// CopyValue will create a copy of the value.
func (p *Pair) CopyValue(buf []byte) ([]byte, error) {
	return p.item.ValueCopy(buf)
}

// Iterator is used to iterate over the key space of the data store.
type Iterator struct {
	iter *badger.Iterator
}

// Seek to the exact key or the smallest greater key. The behaviour is reversed
// when iterating in reverse mode.
func (i *Iterator) Seek(key []byte) {
	i.iter.Seek(userPrefix(key))
}

// Valid will return whether a valid pair is present.
func (i *Iterator) Valid() bool {
	return i.iter.Valid()
}

// Pair will return the current key value pair.
func (i *Iterator) Pair() *Pair {
	// get item
	item := i.iter.Item()
	if item == nil {
		return nil
	}

	// create pair
	p := &Pair{
		item: item,
	}

	return p
}

// Next will move on to the next pair.
func (i *Iterator) Next() {
	i.iter.Next()
}

// Close will close the iterator.
func (i *Iterator) Close() {
	i.iter.Close()
}

func userPrefix(key []byte) []byte {
	return append([]byte{'#'}, key...)
}

func userTrim(key []byte) []byte {
	return key[1:]
}
