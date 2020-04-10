package basic

import (
	"github.com/vmihailenco/msgpack/v4"

	"github.com/256dpi/turing"
)

type List struct {
	Prefix  []byte   `msgpack:"p,omitempty"`
	Reverse bool     `msgpack:"r,omitempty"`
	Keys    [][]byte `msgpack:"k,omitempty"`
}

var listDesc = &turing.Description{
	Name: "std/basic/List",
}

func (l *List) Describe() *turing.Description {
	return listDesc
}

func (l *List) Effect() int {
	return 0
}

func (l *List) Execute(txn *turing.Transaction) error {
	// reset Map
	l.Keys = make([][]byte, 0)

	// create iterator
	iter := txn.Iterator(l.Prefix)
	defer iter.Close()

	// add all keys
	for iter.First(); iter.Valid(); iter.Next() {
		key := turing.Copy(nil, iter.Key())
		l.Keys = append(l.Keys, key)
	}

	return nil
}

func (l *List) Encode() ([]byte, turing.Ref, error) {
	buf, err := msgpack.Marshal(l)
	return buf, turing.NoopRef, err
}

func (l *List) Decode(bytes []byte) error {
	return msgpack.Unmarshal(bytes, l)
}
