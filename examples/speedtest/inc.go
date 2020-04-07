package main

import (
	"fmt"
	"strconv"

	"github.com/256dpi/god"

	"github.com/256dpi/turing"
	"github.com/256dpi/turing/pkg/coding"
	"github.com/256dpi/turing/std/basic"
)

var incAdd = basic.IncAdd

type inc struct {
	Key   string `msgpack:"k,omitempty"`
	Value int64  `msgpack:"v,omitempty"`
	Merge bool   `msgpack:"m,omitempty"`
}

func (i *inc) Describe() turing.Description {
	return turing.Description{
		Name:      "inc",
		Effect:    1,
		Operators: []*turing.Operator{incAdd},
	}
}

var incCounter = god.NewCounter("inc", nil)

func (i *inc) Execute(txn *turing.Transaction) error {
	incCounter.Add(1)

	// make key
	key := []byte(i.Key)

	// use merge if requested
	if i.Merge {
		return txn.Merge(key, strconv.AppendInt(nil, i.Value, 10), incAdd)
	}

	// get count
	var count int64
	var err error
	err = txn.Use(key, func(value []byte) error {
		count, err = strconv.ParseInt(string(value), 10, 64)
		return err
	})
	if err != nil {
		return err
	}

	// inc
	count += i.Value

	// set value
	err = txn.Set(key, strconv.AppendInt(nil, count, 10))
	if err != nil {
		return err
	}

	// set count
	i.Value = count

	return nil
}

func (i *inc) Encode() ([]byte, error) {
	return coding.Encode(func(enc *coding.Encoder) {
		// encode version
		enc.Uint(1)

		// encode body
		enc.String(i.Key)
		enc.Int(i.Value)
		enc.Bool(i.Merge)
	}), nil
}

func (i *inc) Decode(bytes []byte) error {
	return coding.Decode(bytes, func(dec *coding.Decoder) error {
		// decode version
		var version uint64
		dec.Uint(&version)
		if version != 1 {
			return fmt.Errorf("inc: invalid version")
		}

		// decode body
		dec.String(&i.Key, false)
		dec.Int(&i.Value)
		dec.Bool(&i.Merge)

		return nil
	})
}
