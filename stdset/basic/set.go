package basic

import (
	"github.com/256dpi/turing"
)

type Set struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

func (s *Set) Describe() turing.Description {
	return turing.Description{
		Name:        "stdset/basic.Set",
		Cardinality: 1,
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
