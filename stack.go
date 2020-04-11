package turing

import (
	"fmt"

	"github.com/256dpi/turing/pkg/coding"
)

// Operand represents a single merge operand.
type Operand struct {
	Name  string
	Value []byte
}

// Stack represents a stack of merge operands.
type Stack struct {
	Operands []Operand
}

// Encode will encode the stack.
func (s *Stack) Encode(borrow bool) ([]byte, Ref, error) {
	// check operands
	for _, op := range s.Operands {
		if op.Name == "" {
			return nil, NoopRef, fmt.Errorf("turing: encode stack: missing operand name")
		}
	}

	return coding.Encode(borrow, func(enc *coding.Encoder) error {
		// write version
		enc.VarUint(1)

		// write length
		enc.VarUint(uint64(len(s.Operands)))

		// write operands
		for _, op := range s.Operands {
			enc.VarString(op.Name)
			enc.VarBytes(op.Value)
		}

		return nil
	})
}

// Decode will decode the stack.
func (s *Stack) Decode(bytes []byte, clone bool) error {
	return coding.Decode(bytes, func(dec *coding.Decoder) error {
		// decode version
		var version uint64
		dec.VarUint(&version)
		if version != 1 {
			return fmt.Errorf("turing: decode stack: invalid version")
		}

		// decode length
		var length uint64
		dec.VarUint(&length)

		// decode operands
		s.Operands = make([]Operand, int(length))
		for i := range s.Operands {
			dec.VarString(&s.Operands[i].Name, clone)
			dec.VarBytes(&s.Operands[i].Value, clone)
		}

		return nil
	})
}

// WalkStack will walk the encoded stack and yield the operands.
func WalkStack(bytes []byte, fn func(op Operand) error) error {
	err := coding.Decode(bytes, func(dec *coding.Decoder) error {
		// decode version
		var version uint64
		dec.VarUint(&version)
		if version != 1 {
			return fmt.Errorf("turing: walk stack: invalid version")
		}

		// decode length
		var length uint64
		dec.VarUint(&length)

		// decode operands
		var op Operand
		var err error
		for i := 0; i < int(length); i++ {
			dec.VarString(&op.Name, false)
			dec.VarBytes(&op.Value, false)
			if err = fn(op); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil && err != ErrBreak {
		return err
	}

	return nil
}
