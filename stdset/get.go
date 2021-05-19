package stdset

import (
	"fmt"

	"github.com/256dpi/fpack"
	"github.com/256dpi/turing"
)

// Get will get a value.
type Get struct {
	Key    []byte
	Value  []byte
	Exists bool
}

var getDesc = &turing.Description{
	Name: "turing/Get",
}

// Describe implements the turing.Instruction interface.
func (g *Get) Describe() *turing.Description {
	return getDesc
}

// Effect implements the turing.Instruction interface.
func (g *Get) Effect() int {
	return 0
}

// Execute implements the turing.Instruction interface.
func (g *Get) Execute(mem turing.Memory, _ turing.Cache) error {
	// get value
	err := mem.Use(g.Key, func(value []byte) error {
		g.Value = turing.Clone(value)
		g.Exists = true
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

// Encode implements the turing.Instruction interface.
func (g *Get) Encode() ([]byte, turing.Ref, error) {
	return fpack.Encode(true, func(enc *fpack.Encoder) error {
		// encode version
		enc.Uint8(1)

		// encode body
		enc.VarBytes(g.Key)
		enc.Bool(g.Exists)
		enc.Tail(g.Value)

		return nil
	})
}

// Decode implements the turing.Instruction interface.
func (g *Get) Decode(bytes []byte) error {
	return fpack.Decode(bytes, func(dec *fpack.Decoder) error {
		// check version
		if dec.Uint8() != 1 {
			return fmt.Errorf("stdset: decode get: invalid version")
		}

		// decode body
		g.Key = dec.VarBytes(true)
		g.Exists = dec.Bool()
		g.Value = dec.Tail(true)

		return nil
	})
}
