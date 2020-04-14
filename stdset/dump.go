package stdset

import (
	"fmt"

	"github.com/256dpi/turing"
	"github.com/256dpi/turing/coding"
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
func (d *Dump) Execute(mem turing.Memory) error {
	// prepare map
	d.Map = map[string]string{}

	// create iterator
	iter := mem.Iterate(d.Prefix)
	defer iter.Close()

	// iterate through all pairs
	for iter.First(); iter.Valid(); iter.Next() {
		err := iter.Use(func(value []byte) error {
			d.Map[string(iter.TempKey())] = string(value)
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
	return coding.Encode(true, func(enc *coding.Encoder) error {
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
	return coding.Decode(bytes, func(dec *coding.Decoder) error {
		// decode version
		var version uint8
		dec.Uint8(&version)
		if version != 1 {
			return fmt.Errorf("stdset: decode dump: invalid version")
		}

		// decode prefix
		dec.VarBytes(&d.Prefix, true)

		// decode length
		var length uint64
		dec.VarUint(&length)

		// prepare map
		d.Map = make(map[string]string, length)

		// decode pairs
		for i := 0; i < int(length); i++ {
			// decode key
			var key string
			dec.VarString(&key, true)

			// decode value
			var value string
			dec.VarString(&value, true)

			// set pair
			d.Map[key] = value
		}

		return nil
	})
}
