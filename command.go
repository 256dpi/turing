package turing

import (
	"fmt"

	"github.com/256dpi/turing/pkg/coding"
)

// Operation is a single instruction executed as part of a command.
type Operation struct {
	// The instruction name.
	Name string

	// The instruction data.
	Data []byte
}

// Command represents a set of operations to be executed together.
type Command struct {
	// The operations.
	Operations []Operation
}

// Encode will encode the command into a byte slice.
func (c *Command) Encode(borrow bool) ([]byte, Ref, error) {
	// check operations
	for _, op := range c.Operations {
		if op.Name == "" {
			return nil, NoopRef, fmt.Errorf("turing: encode command: missing operation name")
		}
	}

	return coding.Encode(borrow, func(enc *coding.Encoder) error {
		// encode version
		enc.Uint8(1)

		// encode number of operations
		enc.Uint16(uint16(len(c.Operations))) // ~65K

		// encode operations
		for _, op := range c.Operations {
			enc.String(op.Name, 2) // ~65KB
			enc.Bytes(op.Data, 4)  // ~4.3GB
		}

		return nil
	})
}

// Decode will decode a command from the provided byte slice. If clone is
// not set, the command may change if the decoded byte slice changes.
func (c *Command) Decode(bytes []byte, clone bool) error {
	return coding.Decode(bytes, func(dec *coding.Decoder) error {
		// decode version
		var version uint8
		dec.Uint8(&version)
		if version != 1 {
			return fmt.Errorf("turing: decode command: invalid version")
		}

		// decode number of operations
		var length uint16
		dec.Uint16(&length)

		// decode operations
		c.Operations = make([]Operation, length)
		for i := 0; i < int(length); i++ {
			dec.String(&c.Operations[i].Name, 2, clone)
			dec.Bytes(&c.Operations[i].Data, 4, clone)
		}

		return nil
	})
}

// WalkCommand will walk the encoded command and yield the operations.
func WalkCommand(bytes []byte, fn func(i int, op Operation) error) error {
	err := coding.Decode(bytes, func(dec *coding.Decoder) error {
		// decode version
		var version uint8
		dec.Uint8(&version)
		if version != 1 {
			return fmt.Errorf("turing: walk command: invalid version")
		}

		// decode number of operations
		var length uint16
		dec.Uint16(&length)

		// decode operations
		var op Operation
		var err error
		for i := 0; i < int(length); i++ {
			dec.String(&op.Name, 2, false)
			dec.Bytes(&op.Data, 4, false)
			if err = fn(i, op); err != nil {
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
