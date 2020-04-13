package main

import (
	"github.com/256dpi/god"

	"github.com/256dpi/turing"
	"github.com/256dpi/turing/coding"
)

type sum struct {
	Start uint64
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

func (s *sum) Execute(mem turing.Memory) error {
	sumCounter.Add(1)

	// reset
	s.Total = 0

	// get start
	start := encodeNum(s.Start)

	// get iterator
	iter := mem.Iterate(nil)
	defer iter.Close()

	// iterate over key space
	i := 0
	for iter.SeekGE(start); iter.Valid(); iter.Next() {
		// get value
		val, ref, err := iter.Value()
		if err != nil {
			return err
		}

		// increment
		s.Total += decodeNum(val)

		// release
		ref.Release()

		// increment
		i++
		if i >= int(*scanLength) {
			break
		}
	}

	return nil
}

func (s *sum) Encode() ([]byte, turing.Ref, error) {
	return coding.Encode(true, func(enc *coding.Encoder) error {
		enc.Uint64(s.Start)
		enc.Uint64(s.Total)
		return nil
	})
}

func (s *sum) Decode(bytes []byte) error {
	return coding.Decode(bytes, func(dec *coding.Decoder) error {
		dec.Uint64(&s.Start)
		dec.Uint64(&s.Total)
		return nil
	})
}
