package basic

import (
	"encoding/json"

	"github.com/256dpi/turing"
)

type List struct {
	Prefix  []byte   `json:"prefix,omitempty"`
	Reverse bool     `json:"reverse,omitempty"`
	Keys    [][]byte `json:"keys,omitempty"`
}

func (l *List) Name() string {
	return "stdset/basic.List"
}

func (l *List) Build() turing.Instruction {
	return &List{}
}

func (l *List) Encode() ([]byte, error) {
	return json.Marshal(l)
}

func (l *List) Decode(data []byte) error {
	return json.Unmarshal(data, l)
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

func (l *List) Cardinality() int {
	return 0
}

func (l *List) ReadOnly() bool {
	return true
}
