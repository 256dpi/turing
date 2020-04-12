package main

import (
	"github.com/256dpi/god"

	"github.com/256dpi/turing"
	"github.com/256dpi/turing/coding"
)

type sum struct {
	Total uint64
}

var sumDesc = &turing.Description{
	Name: "sum",
}

func (s *sum) Describe() *turing.Description {
	return sumDesc
}

func (s *sum) Effect() int {
	return 0
}

var sumCounter = god.NewCounter("sum", nil)

func (s *sum) Execute(txn *turing.Transaction) error {
	sumCounter.Add(1)

	// reset
	s.Total = 0

	// get iterator
	iter := txn.Iterator(nil)
	defer iter.Close()

	// iterate over key space
	for iter.First(); iter.Valid(); iter.Next() {
		// get value
		val, ref, err := iter.Value()
		if err != nil {
			return err
		}

		// increment
		s.Total += decodeInt(val)

		// release
		ref.Release()
	}

	return nil
}

func (s *sum) Encode() ([]byte, turing.Ref, error) {
	return coding.Encode(true, func(enc *coding.Encoder) error {
		enc.Uint64(s.Total)
		return nil
	})
}

func (s *sum) Decode(bytes []byte) error {
	return coding.Decode(bytes, func(dec *coding.Decoder) error {
		dec.Uint64(&s.Total)
		return nil
	})
}
