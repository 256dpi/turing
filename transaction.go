package turing

import (
	"github.com/dgraph-io/badger"
)

type Pair struct {
	item *badger.Item
}

func (p *Pair) Key() []byte {
	return userTrim(p.item.Key())
}

func (p *Pair) CopyKey(buf []byte) []byte {
	return userTrim(p.item.KeyCopy(buf))
}

func (p *Pair) LoadValue(fn func([]byte) error) error {
	return p.item.Value(fn)
}

func (p *Pair) CopyValue(buf []byte) ([]byte, error) {
	return p.item.ValueCopy(buf)
}

type Transaction struct {
	txn *badger.Txn
}

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

func (t *Transaction) Set(key, value []byte) error {
	// set key to value
	err := t.txn.Set(userPrefix(key), value)
	if err != nil {
		return err
	}

	return nil
}

func (t *Transaction) Delete(key []byte) error {
	// delete key
	err := t.txn.Delete(userPrefix(key))
	if err != nil {
		return err
	}

	return nil
}

func (t *Transaction) Iterator(config IteratorConfig) *Iterator {
	return &Iterator{
		iter: t.txn.NewIterator(badger.IteratorOptions{
			Prefix:         userPrefix(config.Prefix),
			PrefetchValues: config.Prefetch > 0,
			PrefetchSize:   config.Prefetch,
			Reverse:        config.Reverse,
		}),
	}
}

type IteratorConfig struct {
	Prefix   []byte
	Prefetch int
	Reverse  bool
}

type Iterator struct {
	iter *badger.Iterator
}

func (i *Iterator) Seek(key []byte) {
	i.iter.Seek(userPrefix(key))
}

func (i *Iterator) Valid() bool {
	return i.iter.Valid()
}

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

func (i *Iterator) Next() {
	i.iter.Next()
}

func (i *Iterator) Close() {
	i.iter.Close()
}

func userPrefix(key []byte) []byte {
	return append([]byte{'#'}, key...)
}

func userTrim(key []byte) []byte {
	return key[1:]
}
