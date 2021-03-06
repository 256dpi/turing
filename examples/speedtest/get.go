package main

import (
	"github.com/256dpi/fpack"
	"github.com/256dpi/god"

	"github.com/256dpi/turing"
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

func (g *get) Execute(mem turing.Memory, _ turing.Cache) error {
	getCounter.Add(1)

	// encode key
	key := encodeNum(g.Key)

	// get count
	err := mem.Use(key, func(value []byte) error {
		g.Value = decodeNum(value)
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (g *get) Encode() ([]byte, turing.Ref, error) {
	return fpack.Encode(true, func(enc *fpack.Encoder) error {
		enc.Uint64(g.Key)
		enc.Uint64(g.Value)
		return nil
	})
}

func (g *get) Decode(bytes []byte) error {
	return fpack.Decode(bytes, func(dec *fpack.Decoder) error {
		g.Key = dec.Uint64()
		g.Value = dec.Uint64()
		return nil
	})
}
