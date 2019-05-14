package basic

import (
	"github.com/256dpi/turing"
)

type List struct {
	Prefix  []byte   `json:"prefix,omitempty"`
	Reverse bool     `json:"reverse,omitempty"`
	Keys    [][]byte `json:"keys,omitempty"`
}

func (l *List) Describe() turing.Description {
	return turing.Description{
		Name:     "stdset/basic.List",
		ReadOnly: true,
	}
}

func (l *List) Execute(txn *turing.Transaction) error {
	// reset Map
	l.Keys = make([][]byte, 0)

	// create iterator
	iter := txn.Iterator(turing.IteratorConfig{
		Prefix:  l.Prefix,
		Reverse: l.Reverse,
	})

	// ensure closing
	defer iter.Close()

	// add all keys
	for iter.Seek(nil); iter.Valid(); iter.Next() {
		l.Keys = append(l.Keys, iter.Pair().CopyKey(nil))
	}

	return nil
}
