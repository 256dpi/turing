package wire

import (
	"fmt"

	"github.com/256dpi/turing/coding"
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
func (c *Command) Encode(borrow bool) ([]byte, *coding.Ref, error) {
	// check operations
	for _, op := range c.Operations {
		if op.Name == "" {
			return nil, nil, fmt.Errorf("turing: encode command: missing operation name")
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
			enc.Bytes(op.Code, 4)  // ~4.3GB
		}

		return nil
	})
}

// Decode will decode the command.
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
			dec.Bytes(&c.Operations[i].Code, 4, clone)
		}

		return nil
	})
}

// WalkCommand will walk the encoded command and yield the operations. ErrBreak
// may be returned to stop execution.
func WalkCommand(bytes []byte, fn func(i int, op Operation) (bool, error)) error {
	return coding.Decode(bytes, func(dec *coding.Decoder) error {
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
		for i := 0; i < int(length); i++ {
			dec.String(&op.Name, 2, false)
			dec.Bytes(&op.Code, 4, false)
			ok, err := fn(i, op)
			if err != nil || !ok {
				return err
			}
		}

		return nil
	})
}
