package main

import (
	"strconv"

	"github.com/256dpi/turing"
)

type increment struct {
	Key   string `msgpack:"k,omitempty"`
	Value int    `msgpack:"v,omitempty"`
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

	// get count
	var count int
	var err error
	err = txn.Use(key, func(value []byte) error {
		count, err = strconv.Atoi(string(value))
		return err
	})
	if err != nil {
		return err
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

	return nil
}
