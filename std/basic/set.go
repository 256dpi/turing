package basic

import (
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
