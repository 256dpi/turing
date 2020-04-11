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
		enc.VarUint(1)

		// encode number of operations
		enc.VarUint(uint64(len(c.Operations)))

		// encode operations
		for _, op := range c.Operations {
			enc.VarString(op.Name)
			enc.VarBytes(op.Data)
		}

		return nil
	})
}

// Decode will decode a command from the provided byte slice. If clone is
// not set, the command may change if the decoded byte slice changes.
func (c *Command) Decode(bytes []byte, clone bool) error {
	return coding.Decode(bytes, func(dec *coding.Decoder) error {
		// decode version
		var version uint64
		dec.VarUint(&version)
		if version != 1 {
			return fmt.Errorf("turing: decode command: invalid version")
		}

		// decode number of operations
		var length uint64
		dec.VarUint(&length)

		// decode operations
		c.Operations = make([]Operation, length)
		for i := 0; i < int(length); i++ {
			dec.VarString(&c.Operations[i].Name, clone)
			dec.VarBytes(&c.Operations[i].Data, clone)
		}

		return nil
	})
}

// WalkCommand will walk the encoded command and yield the operations.
func WalkCommand(bytes []byte, fn func(i int, op Operation) error) error {
	err := coding.Decode(bytes, func(dec *coding.Decoder) error {
		// decode version
		var version uint64
		dec.VarUint(&version)
		if version != 1 {
			return fmt.Errorf("turing: walk command: invalid version")
		}

		// decode number of operations
		var length uint64
		dec.VarUint(&length)

		// decode operations
		var op Operation
		var err error
		for i := 0; i < int(length); i++ {
			dec.VarString(&op.Name, false)
			dec.VarBytes(&op.Data, false)
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
