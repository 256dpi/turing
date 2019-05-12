package main

import (
	"encoding/json"
	"strconv"

	"github.com/256dpi/turing"
)

type Increment struct {
	Key string `json:"key,omitempty"`
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

	return nil
}

type List struct {
	Pairs map[string]string `json:"pairs,omitempty"`
}

func (l *List) Name() string {
	return "list"
}

func (l *List) Build() turing.Instruction {
	return &List{}
}

func (l *List) Encode() ([]byte, error) {
	return json.Marshal(l)
}

func (l *List) Decode(data []byte) error {
	return json.Unmarshal(data, l)
}

func (l *List) Execute(txn *turing.Transaction) error {
	// create iterator
	iter := txn.Iterator(turing.IteratorConfig{
		Prefetch: 10,
	})

	// ensure closing
	defer iter.Close()

	// iterate through all pairs
	for iter.Seek(nil); iter.Valid(); iter.Next() {
		err := iter.Pair().LoadValue(func(value []byte) error {
			l.Pairs[string(iter.Pair().Key())] = string(value)
			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}
