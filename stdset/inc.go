package stdset

import (
	"fmt"
	"strconv"

	"github.com/256dpi/fpack"
	"github.com/tidwall/cast"

	"github.com/256dpi/turing"
)

const int64Len = 24

// Add is an operator used by Inc to add together numerical values.
var Add = &turing.Operator{
	Name: "turing/Add",
	Zero: []byte("0"),
	Apply: func(value []byte, ops [][]byte) ([]byte, turing.Ref, error) {
		// parse value (fallback to zero)
		count, _ := strconv.ParseInt(cast.ToString(value), 10, 64)

		// apply operands
		for _, op := range ops {
			// parse operand (fallback to zero)
			increment, _ := strconv.ParseInt(cast.ToString(op), 10, 64)

			// add increment
			count += increment
		}

		// borrow slice
		buf, ref := fpack.Borrow(int64Len)

		// encode count
		buf = buf[:0]
		buf = strconv.AppendInt(buf, count, 10)

		return buf, ref, nil
	},
	Combine: func(ops [][]byte) ([]byte, turing.Ref, error) {
		// apply operands
		var count int64
		for _, op := range ops {
			// parse operand (fallback to zero)
			increment, _ := strconv.ParseInt(cast.ToString(op), 10, 64)

			// add increment
			count += increment
		}

		// borrow slice
		buf, ref := fpack.Borrow(int64Len)

		// encode count
		buf = buf[:0]
		buf = strconv.AppendInt(buf, count, 10)

		return buf, ref, nil
	},
}

// Inc will increment an numerical value.
type Inc struct {
	Key   []byte
	Value int64
}

var incDesc = &turing.Description{
	Name:      "turing/Inc",
	Operators: []*turing.Operator{Add},
}

// Describe implements the turing.Instruction interface.
func (i *Inc) Describe() *turing.Description {
	return incDesc
}

// Effect implements the turing.Instruction interface.
func (i *Inc) Effect() int {
	return 1
}

// Execute implements the turing.Instruction interface.
func (i *Inc) Execute(mem turing.Memory, _ turing.Cache) error {
	// borrow slice
	buf, ref := fpack.Borrow(int64Len)
	defer ref.Release()

	// encode count
	buf = buf[:0]
	buf = strconv.AppendInt(buf, i.Value, 10)

	// add value
	err := mem.Merge(i.Key, buf, Add)
	if err != nil {
		return err
	}

	return nil
}

// Encode implements the turing.Instruction interface.
func (i *Inc) Encode() ([]byte, turing.Ref, error) {
	return fpack.Encode(true, func(enc *fpack.Encoder) error {
		// encode version
		enc.Uint8(1)

		// encode body
		enc.Int64(i.Value)
		enc.Tail(i.Key)

		return nil
	})
}

// Decode implements the turing.Instruction interface.
func (i *Inc) Decode(bytes []byte) error {
	return fpack.Decode(bytes, func(dec *fpack.Decoder) error {
		// check version
		if dec.Uint8() != 1 {
			return fmt.Errorf("stdset: decode inc: invalid version")
		}

		// decode body
		i.Value = dec.Int64()
		i.Key = dec.Tail(true)

		return nil
	})
}
