package main

import (
	"sync"

	"github.com/256dpi/fpack"
	"github.com/256dpi/god"

	"github.com/256dpi/turing"
)

var addApplyCounter = god.NewCounter("add-a", nil)
var addCombineCounter = god.NewCounter("add-c", nil)

var incAdd = &turing.Operator{
	Name: "add",
	Zero: []byte("\x00\x00\x00\x00\x00\x00\x00\x00"),
	Apply: func(value []byte, ops [][]byte) ([]byte, turing.Ref, error) {
		addApplyCounter.Add(1)

		// parse value
		count := decodeNum(value)

		// apply operands
		for _, op := range ops {
			count += decodeNum(op)
		}

		// encode value
		value = encodeNum(count)

		return value, nil, nil
	},
	Combine: func(ops [][]byte) ([]byte, turing.Ref, error) {
		addCombineCounter.Add(1)

		// combine operands
		var count uint64
		for _, op := range ops {
			count += decodeNum(op)
		}

		// encode value
		value := encodeNum(count)

		return value, nil, nil
	},
}

type inc struct {
	Key   uint64
	Value uint64
	Merge bool
}

var incPool = sync.Pool{
	New: func() interface{} {
		return &inc{}
	},
}

var incDesc = &turing.Description{
	Name:      "inc",
	Operators: []*turing.Operator{incAdd},
	Builder: func() turing.Instruction {
		return incPool.Get().(*inc)
	},
	Recycler: func(ins turing.Instruction) {
		inc := ins.(*inc)
		inc.Key = 0
		inc.Value = 0
		inc.Merge = false
		incPool.Put(inc)
	},
	NoResult: true,
}

func (i *inc) Describe() *turing.Description {
	return incDesc
}

func (i *inc) Effect() int {
	return 1
}

var incCounter = god.NewCounter("inc", nil)

func (i *inc) Execute(mem turing.Memory, _ turing.Cache) error {
	incCounter.Add(1)

	// encode key
	key := encodeNum(i.Key)

	// use merge operator if requested
	if i.Merge {
		return mem.Merge(key, encodeNum(i.Value), incAdd)
	}

	// get count
	var count uint64
	err := mem.Use(key, func(value []byte) error {
		count = decodeNum(value)
		return nil
	})
	if err != nil {
		return err
	}

	// increment
	count += i.Value

	// set value
	err = mem.Set(key, encodeNum(count))
	if err != nil {
		return err
	}

	// set count
	i.Value = count

	return nil
}

func (i *inc) Encode() ([]byte, turing.Ref, error) {
	return fpack.Encode(true, func(enc *fpack.Encoder) error {
		enc.Uint64(i.Key)
		enc.Uint64(i.Value)
		enc.Bool(i.Merge)
		return nil
	})
}

func (i *inc) Decode(bytes []byte) error {
	return fpack.Decode(bytes, func(dec *fpack.Decoder) error {
		i.Key = dec.Uint64()
		i.Value = dec.Uint64()
		i.Merge = dec.Bool()
		return nil
	})
}
