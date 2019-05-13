package main

import (
	"encoding/json"
	"strconv"
	"time"

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

func (l *retrieve) Execute(txn *turing.Transaction) error {
	// get start
	start := time.Now()

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

	// increment
	mutex.Lock()
	retrieveTimer.Add(time.Since(start))
	mutex.Unlock()

	return nil
}

func (l *retrieve) Cardinality() int {
	return 0
}
