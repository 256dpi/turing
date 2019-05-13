package main

import (
	"encoding/json"
	"strconv"

	"github.com/256dpi/god"

	"github.com/256dpi/turing"
)

type increment struct {
	Key   string `json:"key,omitempty"`
	Value int    `json:"value,omitempty"`
}

func (i *increment) Name() string {
	return "increment"
}

func (i *increment) Build() turing.Instruction {
	return &increment{}
}

func (i *increment) Encode() ([]byte, error) {
	return json.Marshal(i)
}

func (i *increment) Decode(data []byte) error {
	return json.Unmarshal(data, i)
}

var incrementCounter = god.NewCounter("increment")
var incrementTimer = god.NewTimer("increment")

func (i *increment) Execute(txn *turing.Transaction) error {
	// measure execution
	defer incrementTimer.Measure()()

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

	// increment
	incrementCounter.Add(1)

	return nil
}

func (i *increment) Cardinality() int {
	return 1
}
