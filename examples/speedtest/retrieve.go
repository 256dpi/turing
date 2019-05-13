package main

import (
	"encoding/json"
	"strconv"

	"github.com/256dpi/god"

	"github.com/256dpi/turing"
)

type retrieve struct {
	Key   string
	Value int
}

func (l *retrieve) Name() string {
	return "retrieve"
}

func (l *retrieve) Build() turing.Instruction {
	return &retrieve{}
}

func (l *retrieve) Encode() ([]byte, error) {
	return json.Marshal(l)
}

func (l *retrieve) Decode(data []byte) error {
	return json.Unmarshal(data, l)
}

var retrieveCounter = god.NewCounter("retrieve")
var retrieveTimer = god.NewTimer("retrieve")

func (l *retrieve) Execute(txn *turing.Transaction) error {
	// measure execution
	retrieveTimer.Measure()()

	// get key
	pair, err := txn.Get([]byte(l.Key))
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
			l.Value = count

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

func (l *retrieve) Cardinality() int {
	return 0
}
