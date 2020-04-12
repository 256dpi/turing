package main

import (
	"github.com/256dpi/god"

	"github.com/256dpi/turing"
	"github.com/256dpi/turing/coding"
)

type get struct {
	Key   uint64
	Value uint64
}

var getDesc = &turing.Description{
	Name: "get",
}

func (g *get) Describe() *turing.Description {
	return getDesc
}

func (g *get) Effect() int {
	return 0
}

var getCounter = god.NewCounter("get", nil)

func (g *get) Execute(txn *turing.Transaction) error {
	getCounter.Add(1)

	// encode key
	key := encodeInt(g.Key)

	// get count
	err := txn.Use(key, func(value []byte) error {
		g.Value = decodeInt(value)
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (g *get) Encode() ([]byte, turing.Ref, error) {
	return coding.Encode(true, func(enc *coding.Encoder) error {
		enc.Uint64(g.Key)
		enc.Uint64(g.Value)
		return nil
	})
}

func (g *get) Decode(bytes []byte) error {
	return coding.Decode(bytes, func(dec *coding.Decoder) error {
		dec.Uint64(&g.Key)
		dec.Uint64(&g.Value)
		return nil
	})
}
