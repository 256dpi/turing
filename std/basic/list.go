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

func (l *List) Describe() turing.Description {
	return turing.Description{
		Name: "std/basic/List",
	}
}

func (l *List) Execute(txn *turing.Transaction) error {
	// reset Map
	l.Keys = make([][]byte, 0)

	// create iterator
	iter := txn.Iterator(l.Prefix)
	defer iter.Close()

	// add all keys
	for iter.First(); iter.Valid(); iter.Next() {
		l.Keys = append(l.Keys, iter.Key(true))
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
