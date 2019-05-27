package main

import (
	"strconv"

	"github.com/256dpi/god"

	"github.com/256dpi/turing"
)

var retrieveCounter = god.NewCounter("retrieve")
var retrieveTimer = god.NewTimer("retrieve")

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
	// measure execution
	retrieveTimer.Measure()()

	// get key
	pair, err := txn.Get([]byte(r.Key))
	if err != nil {
		return err
	}

	// check pair
	if pair != nil {
		err = pair.LoadValue(func(value []byte) error {
			// parse value
			count, err := strconv.Atoi(string(value))
			if err != nil {
				return err
			}

			// set count
			r.Value = count

			return nil
		})
		if err != nil {
			return err
		}
	}

	// count
	retrieveCounter.Add(1)

	return nil
}
