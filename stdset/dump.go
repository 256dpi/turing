package stdset

import (
	"fmt"

	"github.com/256dpi/fpack"
	"github.com/256dpi/turing"
)

// Dump will dump all key value pairs.
type Dump struct {
	Prefix []byte
	Map    map[string]string
}

var dumpDesc = &turing.Description{
	Name: "turing/Dump",
}

// Describe implements the turing.Instruction interface.
func (d *Dump) Describe() *turing.Description {
	return dumpDesc
}

// Effect implements the turing.Instruction interface.
func (d *Dump) Effect() int {
	return 0
}

// Execute implements the turing.Instruction interface.
func (d *Dump) Execute(mem turing.Memory, _ turing.Cache) error {
	// prepare map
	d.Map = map[string]string{}

	// create iterator
	iter := mem.Iterate(d.Prefix)
	defer iter.Close()

	// iterate through all pairs
	for iter.First(); iter.Valid(); iter.Next() {
		err := iter.Use(func(key, value []byte) error {
			d.Map[string(key)] = string(value)
			return nil
		})
		if err != nil {
			return err
		}
	}

	// close iterator
	err := iter.Close()
	if err != nil {
		return err
	}

	return nil
}

// Encode implements the turing.Instruction interface.
func (d *Dump) Encode() ([]byte, turing.Ref, error) {
	return fpack.Encode(true, func(enc *fpack.Encoder) error {
		// encode version
		enc.Uint8(1)

		// encode prefix
		enc.VarBytes(d.Prefix)

		// encode length
		enc.VarUint(uint64(len(d.Map)))

		// encode pairs
		for key, value := range d.Map {
			enc.VarString(key)
			enc.VarString(value)
		}

		return nil
	})
}

// Decode implements the turing.Instruction interface.
func (d *Dump) Decode(bytes []byte) error {
	return fpack.Decode(bytes, func(dec *fpack.Decoder) error {
		// check version
		if dec.Uint8() != 1 {
			return fmt.Errorf("stdset: decode dump: invalid version")
		}

		// decode prefix
		d.Prefix = dec.VarBytes(true)

		// decode length
		length := dec.VarUint()

		// prepare map
		d.Map = make(map[string]string, length)

		// decode pairs
		for i := 0; i < int(length); i++ {
			key := dec.VarString(true)
			d.Map[key] = dec.VarString(true)
		}

		return nil
	})
}
