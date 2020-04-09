package basic

import (
	"strconv"

	"github.com/tidwall/cast"
	"github.com/vmihailenco/msgpack/v4"

	"github.com/256dpi/turing"
)

var IncAdd = &turing.Operator{
	Name: "std/counter/IncAdd",
	Zero: []byte("0"),
	Apply: func(value []byte, ops [][]byte) ([]byte, error) {
		// parse value
		count, err := strconv.ParseInt(cast.ToString(value), 10, 64)
		if err != nil {
			return nil, err
		}

		// apply operands
		for _, op := range ops {
			// parse operand
			increment, err := strconv.ParseInt(cast.ToString(op), 10, 64)
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

type Inc struct {
	Key   []byte `msgpack:"k,omitempty"`
	Value int64  `msgpack:"v,omitempty"`
}

func (i *Inc) Describe() turing.Description {
	return turing.Description{
		Name:      "std/basic/Inc",
		Effect:    1,
		Operators: []*turing.Operator{IncAdd},
	}
}

func (i *Inc) Execute(txn *turing.Transaction) error {
	// merge with value
	err := txn.Merge(i.Key, strconv.AppendInt(nil, i.Value, 10), IncAdd)
	if err != nil {
		return err
	}

	return nil
}

func (i *Inc) Encode() ([]byte, error) {
	return msgpack.Marshal(i)
}

func (i *Inc) Decode(bytes []byte) error {
	return msgpack.Unmarshal(bytes, i)
}
