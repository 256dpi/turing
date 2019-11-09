package main

import (
	"strconv"

	"github.com/256dpi/god"

	"github.com/256dpi/turing"
)

var retrieveCounter = god.NewCounter("retrieve")

type retrieve struct {
	Key   string
	Value int
}

func (r *retrieve) Describe() turing.Description {
	return turing.Description{
		Name: "retrieve",
	}
}

func (r *retrieve) Execute(txn *turing.Transaction) error {
	// get key
	value, err := txn.Get([]byte(r.Key))
	if err != nil {
		return err
	}

	// check pair
	if value != nil {
		// parse value
		count, err := strconv.Atoi(string(value))
		if err != nil {
			return err
		}

		// set count
		r.Value = count
	}

	// count
	retrieveCounter.Add(1)

	return nil
}
