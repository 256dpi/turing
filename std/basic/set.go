package basic

import (
	"github.com/vmihailenco/msgpack/v4"

	"github.com/256dpi/turing"
)

type Set struct {
	Key   []byte `msgpack:"k,omitempty"`
	Value []byte `msgpack:"v,omitempty"`
}

func (s *Set) Describe() turing.Description {
	return turing.Description{
		Name:   "std/basic/Set",
		Effect: 1,
	}
}

func (s *Set) Execute(txn *turing.Transaction) error {
	// set pair
	err := txn.Set(s.Key, s.Value)
	if err != nil {
		return err
	}

	return nil
}

func (s *Set) Encode() ([]byte, turing.Ref, error) {
	buf, err := msgpack.Marshal(s)
	return buf, turing.NoopRef, err
}

func (s *Set) Decode(bytes []byte) error {
	return msgpack.Unmarshal(bytes, s)
}
