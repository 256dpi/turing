package main

import (
	"encoding/json"

	"github.com/dgraph-io/badger"

	"github.com/256dpi/turing"
)

type Set struct {
	Key   string `json:"key,omitempty"`
	Value string `json:"val,omitempty"`
}

func (s *Set) Name() string {
	return "set"
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

func (s *Set) Apply(txn *badger.Txn) error {
	return txn.Set([]byte(s.Key), []byte(s.Value))
}

type Del struct {
	Key string `json:"key,omitempty"`
}

func (d *Del) Name() string {
	return "del"
}

func (d *Del) Build() turing.Instruction {
	return &Del{}
}

func (d *Del) Encode() ([]byte, error) {
	return json.Marshal(d)
}

func (d *Del) Decode(data []byte) error {
	return json.Unmarshal(data, d)
}

func (d *Del) Apply(txn *badger.Txn) error {
	return txn.Delete([]byte(d.Key))
}
