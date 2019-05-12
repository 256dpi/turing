package main

import (
	"encoding/json"
	"strconv"

	"github.com/256dpi/turing"
)

type List struct {
	Pairs map[string]int `json:"pairs,omitempty"`
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
	// create map
	l.Pairs = make(map[string]int)

	// create iterator
	iter := txn.Iterator(turing.IteratorConfig{
		Prefetch: 10,
	})

	// ensure closing
	defer iter.Close()

	// iterate through all pairs
	for iter.Seek(nil); iter.Valid(); iter.Next() {
		// load value
		err := iter.Pair().LoadValue(func(value []byte) error {
			// parse value
			count, err := strconv.Atoi(string(value))
			if err != nil {
				return err
			}

			// set count
			l.Pairs[string(iter.Pair().Key())] = count

			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}
