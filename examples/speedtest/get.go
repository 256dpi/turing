package main

import (
	"fmt"
	"strconv"

	"github.com/256dpi/god"

	"github.com/256dpi/turing"
	"github.com/256dpi/turing/pkg/coding"
)

type get struct {
	Key   []byte
	Value int64
}

func (g *get) Describe() turing.Description {
	return turing.Description{
		Name: "get",
	}
}

func (g *get) Effect() int {
	return 1
}

var getCounter = god.NewCounter("get", nil)

func (g *get) Execute(txn *turing.Transaction) error {
	getCounter.Add(1)

	// get count
	var err error
	err = txn.Use(g.Key, func(value []byte) error {
		g.Value, err = strconv.ParseInt(string(value), 10, 64)
		return err
	})
	if err != nil {
		return err
	}

	return nil
}

func (g *get) Encode() ([]byte, turing.Ref, error) {
	return coding.Encode(true, func(enc *coding.Encoder) error {
		// encode version
		enc.Uint(1)

		// encode body
		enc.Bytes(g.Key)
		enc.Int(g.Value)

		return nil
	})
}

func (g *get) Decode(bytes []byte) error {
	return coding.Decode(bytes, func(dec *coding.Decoder) error {
		// decode version
		var version uint64
		dec.Uint(&version)
		if version != 1 {
			return fmt.Errorf("get: invalid version")
		}

		// decode body
		dec.Bytes(&g.Key, false)
		dec.Int(&g.Value)

		return nil
	})
}
