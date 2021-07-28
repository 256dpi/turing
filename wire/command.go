package wire

import (
	"fmt"

	"github.com/256dpi/fpack"
)

// Operation represents an encoded instruction.
type Operation struct {
	Name string
	Code []byte
}

// Command represents a list of operations.
type Command struct {
	Operations []Operation
}

// Encode will encode the command.
func (c *Command) Encode(borrow bool) ([]byte, fpack.Ref, error) {
	// check operations
	for _, op := range c.Operations {
		if op.Name == "" {
			return nil, fpack.Ref{}, fmt.Errorf("turing: encode command: missing operation name")
		}
	}

	return fpack.Encode(borrow, func(enc *fpack.Encoder) error {
		// encode version
		enc.Uint8(1)

		// encode number of operations
		enc.Uint16(uint16(len(c.Operations))) // ~65K

		// encode operations
		for _, op := range c.Operations {
			enc.String(op.Name, 2) // ~65KB
			enc.Bytes(op.Code, 4)  // ~4.3GB
		}

		return nil
	})
}

// Decode will decode the command.
func (c *Command) Decode(bytes []byte, clone bool) error {
	return fpack.Decode(bytes, func(dec *fpack.Decoder) error {
		// check version
		if dec.Uint8() != 1 {
			return fmt.Errorf("turing: decode command: invalid version")
		}

		// decode number of operations
		length := dec.Uint16()

		// decode operations
		c.Operations = make([]Operation, length)
		for i := 0; i < int(length); i++ {
			c.Operations[i].Name = dec.String(2, clone)
			c.Operations[i].Code = dec.Bytes(4, clone)
		}

		return nil
	})
}

// WalkCommand will walk the encoded command and yield the operations. ErrBreak
// may be returned to stop execution.
func WalkCommand(bytes []byte, fn func(i int, op Operation) (bool, error)) error {
	return fpack.Decode(bytes, func(dec *fpack.Decoder) error {
		// check version
		if dec.Uint8() != 1 {
			return fmt.Errorf("turing: walk command: invalid version")
		}

		// decode number of operations
		length := dec.Uint16()

		// decode operations
		var op Operation
		for i := 0; i < int(length); i++ {
			op.Name = dec.String(2, false)
			op.Code = dec.Bytes(4, false)
			ok, err := fn(i, op)
			if err != nil || !ok {
				return err
			}
		}

		return nil
	})
}
