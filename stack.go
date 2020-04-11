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
		enc.Uint(1)

		// write length
		enc.Uint(uint64(len(s.Operands)))

		// write operands
		for _, op := range s.Operands {
			enc.String(op.Name)
			enc.Bytes(op.Value)
		}

		return nil
	})
}

// Decode will decode the stack.
func (s *Stack) Decode(bytes []byte, clone bool) error {
	return coding.Decode(bytes, func(dec *coding.Decoder) error {
		// decode version
		var version uint64
		dec.Uint(&version)
		if version != 1 {
			return fmt.Errorf("turing: decode stack: invalid version")
		}

		// decode length
		var length uint64
		dec.Uint(&length)

		// prepare stack
		s.Operands = make([]Operand, int(length))

		// decode operands
		for i := range s.Operands {
			dec.String(&s.Operands[i].Name, clone)
			dec.Bytes(&s.Operands[i].Value, clone)
		}

		return nil
	})
}
