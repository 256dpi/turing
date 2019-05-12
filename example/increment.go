package main

import (
	"encoding/json"
	"strconv"

	"github.com/256dpi/turing"
)

type Increment struct {
	Key   string `json:"key,omitempty"`
	Value int    `json:"value,omitempty"`
}

func (i *Increment) Name() string {
	return "increment"
}

func (i *Increment) Build() turing.Instruction {
	return &Increment{}
}

func (i *Increment) Encode() ([]byte, error) {
	return json.Marshal(i)
}

func (i *Increment) Decode(data []byte) error {
	return json.Unmarshal(data, i)
}

func (i *Increment) Execute(txn *turing.Transaction) error {
	// make key
	key := []byte(i.Key)

	// prepare count
	var count int

	// get existing value
	value, err := txn.Get(key)
	if err != nil {
		return err
	}

	// set current count if available
	if value != nil {
		err = value.LoadValue(func(value []byte) error {
			n, err := strconv.Atoi(string(value))
			count = n
			return err
		})
		if err != nil {
			return err
		}
	}

	// increment
	count++

	// set value
	err = txn.Set(key, []byte(strconv.Itoa(count)))
	if err != nil {
		return err
	}

	// set count
	i.Value = count

	return nil
}
