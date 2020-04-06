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
	iter := txn.Iterator(nil)
	defer iter.Close()

	// iterate through all pairs
	for iter.First(); iter.Valid(); iter.Next() {
		// get value
		value, err := iter.Value(false)
		if err != nil {
			return err
		}

		// parse value
		count, err := strconv.Atoi(string(value))
		if err != nil {
			return err
		}

		// set count
		l.Pairs[string(iter.Key(false))] = count
	}

	return nil
}
