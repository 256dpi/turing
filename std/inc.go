package std

import (
	"strconv"

	"github.com/tidwall/cast"
	"github.com/vmihailenco/msgpack/v4"

	"github.com/256dpi/turing"
	"github.com/256dpi/turing/pkg/coding"
)

var Add = &turing.Operator{
	Name: "turing/Add",
	Zero: []byte("0"),
	Apply: func(value []byte, ops [][]byte) ([]byte, turing.Ref, error) {
		// parse value
		count, err := strconv.ParseInt(cast.ToString(value), 10, 64)
		if err != nil {
			return nil, nil, err
		}

		// apply operands
		for _, op := range ops {
			// parse operand
			increment, err := strconv.ParseInt(cast.ToString(op), 10, 64)
			if err != nil {
				return nil, nil, err
			}

			// add increment
			count += increment
		}

		// borrow slice
		buf, ref := coding.Borrow(24)

		// encode count
		buf = buf[:0]
		buf = strconv.AppendInt(buf, count, 10)

		return buf, ref, nil
	},
}

type Inc struct {
	Key   []byte `msgpack:"k,omitempty"`
	Value int64  `msgpack:"v,omitempty"`
}

var incDesc = &turing.Description{
	Name:      "turing/Inc",
	Operators: []*turing.Operator{Add},
}

func (i *Inc) Describe() *turing.Description {
	return incDesc
}

func (i *Inc) Effect() int {
	return 1
}

func (i *Inc) Execute(txn *turing.Transaction) error {
	// merge with value
	err := txn.Merge(i.Key, strconv.AppendInt(nil, i.Value, 10), Add)
	if err != nil {
		return err
	}

	return nil
}

func (i *Inc) Encode() ([]byte, turing.Ref, error) {
	buf, err := msgpack.Marshal(i)
	return buf, turing.NoopRef, err
}

func (i *Inc) Decode(bytes []byte) error {
	return msgpack.Unmarshal(bytes, i)
}
