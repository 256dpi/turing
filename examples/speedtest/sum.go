package main

import (
	"github.com/256dpi/fpack"
	"github.com/256dpi/god"

	"github.com/256dpi/turing"
)

type sum struct {
	Start uint64
	Count uint64
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

func (s *sum) Execute(mem turing.Memory, _ turing.Cache) error {
	sumCounter.Add(1)

	// reset
	s.Total = 0

	// get start
	start := encodeNum(s.Start)

	// get iterator
	iter := mem.Iterate(nil)
	defer iter.Close()

	// iterate over key space
	var i uint64
	for iter.SeekGE(start); iter.Valid(); iter.Next() {
		// increment
		err := iter.Use(func(key, value []byte) error {
			s.Total += decodeNum(value)
			return nil
		})
		if err != nil {
			return err
		}

		// increment
		if i++; i >= s.Count {
			break
		}
	}

	return nil
}

func (s *sum) Encode() ([]byte, turing.Ref, error) {
	return fpack.Encode(true, func(enc *fpack.Encoder) error {
		enc.Uint64(s.Start)
		enc.Uint64(s.Count)
		enc.Uint64(s.Total)
		return nil
	})
}

func (s *sum) Decode(bytes []byte) error {
	return fpack.Decode(bytes, func(dec *fpack.Decoder) error {
		s.Start = dec.Uint64()
		s.Count = dec.Uint64()
		s.Total = dec.Uint64()
		return nil
	})
}
