package main

import (
	"encoding/json"

	"github.com/256dpi/turing"
)

type Set struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

func (s *Set) Name() string {
	return "stdset/basic.Set"
}

func (s *Set) Build() turing.Instruction {
	return &Set{}
}

func (s *Set) Encode() ([]byte, error) {
	return json.Marshal(s)
}

func (s *Set) Decode(data []byte) error {
	return json.Unmarshal(data, s)
}

func (s *Set) Execute(txn *turing.Transaction) error {
	// get pair
	err := txn.Set(s.Key, s.Value)
	if err != nil {
		return err
	}

	return nil
}

func (s *Set) Cardinality() int {
	return 1
}
