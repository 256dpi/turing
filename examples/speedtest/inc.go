package main

import (
	"github.com/256dpi/god"

	"github.com/256dpi/turing"
	"github.com/256dpi/turing/pkg/coding"
)

var incAdd = &turing.Operator{
	Name: "add",
	Zero: []byte("\x00"),
	Apply: func(value []byte, ops [][]byte) ([]byte, error) {
		// parse value
		count := decodeInt(value)

		// apply operands
		for _, op := range ops {
			count += decodeInt(op)
		}

		return encodeInt(count), nil
	},
}

type inc struct {
	Key   int64
	Value int64
	Merge bool
}

func (i *inc) Describe() turing.Description {
	return turing.Description{
		Name: "inc",
		Operators: func() []*turing.Operator {
			return []*turing.Operator{incAdd}
		},
	}
}

func (i *inc) Effect() int {
	return 1
}

var incCounter = god.NewCounter("inc", nil)

func (i *inc) Execute(txn *turing.Transaction) error {
	incCounter.Add(1)

	// encode key
	key := encodeInt(i.Key)

	// use merge if requested
	if i.Merge {
		val := encodeInt(i.Value)
		return txn.Merge(key, val, incAdd)
	}

	// get count
	var count int64
	err := txn.Use(key, func(value []byte) error {
		count = decodeInt(value)
		return nil
	})
	if err != nil {
		return err
	}

	// inc
	count += i.Value

	// encode value
	val := encodeInt(count)

	// set value
	err = txn.Set(key, val)
	if err != nil {
		return err
	}

	// set count
	i.Value = count

	return nil
}

func (i *inc) Encode() ([]byte, turing.Ref, error) {
	return coding.Encode(true, func(enc *coding.Encoder) error {
		enc.Int(i.Key)
		enc.Int(i.Value)
		enc.Bool(i.Merge)
		return nil
	})
}

func (i *inc) Decode(bytes []byte) error {
	return coding.Decode(bytes, func(dec *coding.Decoder) error {
		dec.Int(&i.Key)
		dec.Int(&i.Value)
		dec.Bool(&i.Merge)
		return nil
	})
}
