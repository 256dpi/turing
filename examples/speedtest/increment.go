package main

import (
	"strconv"

	"github.com/256dpi/god"

	"github.com/256dpi/turing"
)

var incrementCounter = god.NewCounter("increment")
var incrementTimer = god.NewTimer("increment")

type increment struct {
	Key   string `json:"key,omitempty"`
	Value int    `json:"value,omitempty"`
}

func (i *increment) Describe() turing.Description {
	return turing.Description{
		Name:        "increment",
		Cardinality: 1,
	}
}

func (i *increment) Execute(txn *turing.Transaction) error {
	// measure execution
	defer incrementTimer.Measure()()

	// make key
	key := []byte(i.Key)

	// prepare count
	var count int

	// get existing value
	value, err := txn.Get(key)
	if err != nil {
		return err
	}

	// set current count if available
	if value != nil {
		err = value.LoadValue(func(value []byte) error {
			n, err := strconv.Atoi(string(value))
			count = n
			return err
		})
		if err != nil {
			return err
		}
	}

	// increment
	count++

	// set value
	err = txn.Set(key, []byte(strconv.Itoa(count)))
	if err != nil {
		return err
	}

	// set count
	i.Value = count

	// increment
	incrementCounter.Add(1)

	return nil
}
