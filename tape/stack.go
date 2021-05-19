package tape

import (
	"fmt"

	"github.com/256dpi/fpack"
)

// Operand represents a single operator operand.
type Operand struct {
	Name  string
	Value []byte
}

// Stack represents a list of operator operands.
type Stack struct {
	Operands []Operand
}

// Encode will encode the stack.
func (s *Stack) Encode(borrow bool) ([]byte, *fpack.Ref, error) {
	// check operands
	for _, op := range s.Operands {
		if op.Name == "" {
			return nil, nil, fmt.Errorf("turing: encode stack: missing operand name")
		}
	}

	return fpack.Encode(borrow, func(enc *fpack.Encoder) error {
		// write version
		enc.Uint8(1)

		// write length (~65K operands)
		enc.Uint16(uint16(len(s.Operands)))

		// write operands
		for _, op := range s.Operands {
			enc.String(op.Name, 2) // ~65KK
			enc.Bytes(op.Value, 4) // ~4.3GBM
		}

		return nil
	})
}

// Decode will decode the stack.
func (s *Stack) Decode(bytes []byte, clone bool) error {
	return fpack.Decode(bytes, func(dec *fpack.Decoder) error {
		// check version
		if dec.Uint8() != 1 {
			return fmt.Errorf("turing: decode stack: invalid version")
		}

		// decode length
		length := dec.Uint16()

		// decode operands
		s.Operands = make([]Operand, int(length))
		for i := range s.Operands {
			s.Operands[i].Name = dec.String(2, clone)
			s.Operands[i].Value = dec.Bytes(4, clone)
		}

		return nil
	})
}

// WalkStack will walk the encoded stack and yield the operands. ErrBreak may
// be returned to stop execution.
func WalkStack(bytes []byte, fn func(i int, op Operand) (bool, error)) error {
	return fpack.Decode(bytes, func(dec *fpack.Decoder) error {
		// check version
		if dec.Uint8() != 1 {
			return fmt.Errorf("turing: walk stack: invalid version")
		}

		// decode length
		length := dec.Uint16()

		// decode operands
		var op Operand
		for i := 0; i < int(length); i++ {
			op.Name = dec.String(2, false)
			op.Value = dec.Bytes(4, false)
			ok, err := fn(i, op)
			if err != nil || !ok {
				return err
			}
		}

		return nil
	})
}
