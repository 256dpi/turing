package turing

import (
	"github.com/dgraph-io/badger"
)

type Value struct {
	item *badger.Item
}

func (v *Value) Key() []byte {
	return v.item.Key()
}

func (v *Value) CopyKey(buf []byte) []byte {
	return v.item.KeyCopy(buf)
}

func (v *Value) Load(fn func([]byte) error) error {
	return v.item.Value(fn)
}

func (v *Value) Copy(buf []byte) ([]byte, error) {
	return v.item.ValueCopy(buf)
}

func (v *Value) Size() int {
	return int(v.item.ValueSize())
}

type Transaction struct {
	txn *badger.Txn
}

func (t *Transaction) Get(key []byte) (*Value, error) {
	// get item
	item, err := t.txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	// wrap item
	v := &Value{
		item: item,
	}

	return v, nil
}

func (t *Transaction) Set(key, value []byte) error {
	// set value
	err := t.txn.Set(key, value)
	if err != nil {
		return err
	}

	return nil
}

func (t *Transaction) Delete(key []byte) error {
	// delete key
	err := t.txn.Delete(key)
	if err != nil {
		return err
	}

	return nil
}

func (t *Transaction) Iterator(config IteratorConfig) *Iterator {
	return &Iterator{
		iter: t.txn.NewIterator(badger.IteratorOptions{
			Prefix:         config.Prefix,
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
	i.iter.Seek(key)
}

func (i *Iterator) Valid() bool {
	return i.iter.Valid()
}

func (i *Iterator) Value() *Value {
	// get item
	item := i.iter.Item()
	if item == nil {
		return nil
	}

	// wrap item
	v := &Value{item: item}

	return v
}

func (i *Iterator) Next() {
	i.iter.Next()
}

func (i *Iterator) Close() {
	i.iter.Close()
}
