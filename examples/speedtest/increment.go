package main

import (
	"strconv"

	"github.com/256dpi/god"

	"github.com/256dpi/turing"
)

var incrementCounter = god.NewCounter("increment")

type increment struct {
	Key   string `json:"k,omitempty"`
	Value int    `json:"v,omitempty"`
}

func (i *increment) Describe() turing.Description {
	return turing.Description{
		Name:   "increment",
		Effect: 1,
	}
}

func (i *increment) Execute(txn *turing.Transaction) error {
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
		count, err = strconv.Atoi(string(value))
		if err != nil {
			return err
		}
	}

	// increment
	count += i.Value

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
