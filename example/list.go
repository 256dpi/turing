package main

import (
	"encoding/json"

	"github.com/256dpi/turing"
)

type List struct {
	Pairs map[string]string `json:"pairs,omitempty"`
}

func (l *List) Name() string {
	return "list"
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
	// create iterator
	iter := txn.Iterator(turing.IteratorConfig{
		Prefetch: 10,
	})

	// ensure closing
	defer iter.Close()

	// iterate through all pairs
	for iter.Seek(nil); iter.Valid(); iter.Next() {
		err := iter.Pair().LoadValue(func(value []byte) error {
			l.Pairs[string(iter.Pair().Key())] = string(value)
			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}

