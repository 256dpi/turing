package main

import (
	"strconv"

	"github.com/256dpi/god"

	"github.com/256dpi/turing"
)

var retrieveCounter = god.NewCounter("retrieve", nil)

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
	// get count
	var err error
	err = txn.Use([]byte(r.Key), func(value []byte) error {
		r.Value, err = strconv.Atoi(string(value))
		return err
	})
	if err != nil {
		return err
	}

	// count
	retrieveCounter.Add(1)

	return nil
}
