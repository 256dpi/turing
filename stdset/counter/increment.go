package counter

import (
	"strconv"

	"github.com/256dpi/turing"
)

var Add = &turing.Operator{
	Name: "turing/std/counter/add",
	Zero: []byte("0"),
	Apply: func(value []byte, ops [][]byte) ([]byte, error) {
		// parse value
		count, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return nil, err
		}

		// apply operands
		for _, op := range ops {
			// parse operand
			increment, err := strconv.ParseInt(string(op), 10, 64)
			if err != nil {
				return nil, err
			}

			// add increment
			count += increment
		}

		// encode count
		value = strconv.AppendInt(nil, count, 10)

		return value, nil
	},
}

type Increment struct {
	Key   []byte `json:"key,omitempty"`
	Value int64  `json:"value,omitempty"`
}

func (i *Increment) Describe() turing.Description {
	return turing.Description{
		Name:   "turing/std/counter/increment",
		Effect: 1,
	}
}

func (i *Increment) Execute(txn *turing.Transaction) error {
	// merge with value
	err := txn.Merge(i.Key, strconv.AppendInt(nil, i.Value, 10), Add)
	if err != nil {
		return err
	}

	return nil
}
