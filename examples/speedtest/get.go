package main

import (
	"strconv"

	"github.com/256dpi/god"

	"github.com/256dpi/turing"
)

var getCounter = god.NewCounter("get", nil)

type get struct {
	Key   string `msgpack:"k,omitempty"`
	Value int64  `msgpack:"v,omitempty"`
}

func (r *get) Describe() turing.Description {
	return turing.Description{
		Name: "get",
	}
}

func (r *get) Execute(txn *turing.Transaction) error {
	// get count
	var err error
	err = txn.Use([]byte(r.Key), func(value []byte) error {
		r.Value, err = strconv.ParseInt(string(value), 10, 64)
		return err
	})
	if err != nil {
		return err
	}

	// count
	getCounter.Add(1)

	return nil
}
