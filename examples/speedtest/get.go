package main

import (
	"strconv"

	"github.com/256dpi/god"
	"github.com/vmihailenco/msgpack/v4"

	"github.com/256dpi/turing"
)

type get struct {
	Key   string `msgpack:"k,omitempty"`
	Value int64  `msgpack:"v,omitempty"`
}

func (g *get) Describe() turing.Description {
	return turing.Description{
		Name: "get",
	}
}

var getCounter = god.NewCounter("get", nil)

func (g *get) Execute(txn *turing.Transaction) error {
	getCounter.Add(1)

	// get count
	var err error
	err = txn.Use([]byte(g.Key), func(value []byte) error {
		g.Value, err = strconv.ParseInt(string(value), 10, 64)
		return err
	})
	if err != nil {
		return err
	}

	return nil
}

func (g *get) Encode() ([]byte, error) {
	return msgpack.Marshal(g)
}

func (g *get) Decode(bytes []byte) error {
	return msgpack.Unmarshal(bytes, g)
}
