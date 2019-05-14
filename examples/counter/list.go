package main

import (
	"strconv"

	"github.com/256dpi/turing"
)

type list struct {
	Pairs map[string]int `json:"pairs,omitempty"`
}

func (l *list) Describe() turing.Description {
	return turing.Description{
		Name: "list",
	}
}

func (l *list) Execute(txn *turing.Transaction) error {
	// create map
	l.Pairs = make(map[string]int)

	// create iterator
	iter := txn.Iterator(nil, true, false)

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
