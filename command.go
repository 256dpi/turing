package turing

import (
	"fmt"

	"github.com/256dpi/turing/pkg/coding"
)

// Command represents the commands replicated between machines.
type Command struct {
	// The name of the instruction to be executed.
	Name string

	// The encoded instruction to be executed.
	Data []byte
}

// EncodeCommand will encode the provided command into a byte slice.
func EncodeCommand(cmd Command) ([]byte, error) {
	// check name
	if cmd.Name == "" {
		return nil, fmt.Errorf("encode command: missing name")
	}

	// encode command
	return coding.Encode(func(enc *coding.Encoder) error {
		enc.Uint(1)
		enc.String(cmd.Name)
		enc.Tail(cmd.Data)
		return nil
	})
}

// DecodeCommand will decode a command from the provided byte slice. If clone is
// not set, the command may changed if the decoded byte slice changes.
func DecodeCommand(bytes []byte, clone bool) (Command, error) {
	// decode command
	var cmd Command
	err := coding.Decode(bytes, func(dec *coding.Decoder) error {
		// decode version
		var version uint64
		dec.Uint(&version)
		if version != 1 {
			return fmt.Errorf("decode command: invalid version")
		}

		// decode name and data
		dec.String(&cmd.Name, clone)
		dec.Tail(&cmd.Data, clone)

		return nil
	})
	if err != err {
		return Command{}, fmt.Errorf("decode command: %w", err)
	}

	return cmd, nil
}
