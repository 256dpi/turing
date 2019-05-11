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

func (s *Set) Execute(txn *turing.Transaction) error {
	return txn.Set([]byte(s.Key), []byte(s.Value))
}

type Get struct {
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

func (g *Get) Name() string {
	return "get"
}

func (g *Get) Build() turing.Instruction {
	return &Get{}
}

func (g *Get) Encode() ([]byte, error) {
	return json.Marshal(g)
}

func (g *Get) Decode(data []byte) error {
	return json.Unmarshal(data, g)
}

func (g *Get) Execute(txn *turing.Transaction) error {
	// get value
	val, err := txn.Get([]byte(g.Key))
	if err == badger.ErrKeyNotFound {
		return nil
	} else if err != nil {
		return err
	}

	// copy value
	value, err := val.Copy(nil)
	if err != nil {
		return err
	}

	// save value
	g.Value = string(value)

	return nil
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

func (d *Del) Execute(txn *turing.Transaction) error {
	return txn.Delete([]byte(d.Key))
}
