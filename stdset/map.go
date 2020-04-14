package stdset

import (
	"fmt"

	"github.com/256dpi/turing"
	"github.com/256dpi/turing/coding"
)

// Map will map all key value pairs.
type Map struct {
	Prefix []byte
	Pairs  map[string][]byte
}

var mapDesc = &turing.Description{
	Name: "turing/Map",
}

// Describe implements the turing.Instruction interface.
func (m *Map) Describe() *turing.Description {
	return mapDesc
}

// Effect implements the turing.Instruction interface.
func (m *Map) Effect() int {
	return 0
}

// Execute implements the turing.Instruction interface.
func (m *Map) Execute(mem turing.Memory) error {
	// create map
	m.Pairs = make(map[string][]byte)

	// create iterator
	iter := mem.Iterate(m.Prefix)
	defer iter.Close()

	// iterate through all pairs
	for iter.First(); iter.Valid(); iter.Next() {
		err := iter.Use(func(value []byte) error {
			m.Pairs[string(iter.TempKey())] = turing.Clone(value)
			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// Encode implements the turing.Instruction interface.
func (m *Map) Encode() ([]byte, turing.Ref, error) {
	return coding.Encode(true, func(enc *coding.Encoder) error {
		// encode version
		enc.Uint8(1)

		// encode prefix
		enc.VarBytes(m.Prefix)

		// encode length
		enc.VarUint(uint64(len(m.Pairs)))

		// encode pairs
		for key, value := range m.Pairs {
			enc.VarString(key)
			enc.VarBytes(value)
		}

		return nil
	})
}

// Decode implements the turing.Instruction interface.
func (m *Map) Decode(bytes []byte) error {
	return coding.Decode(bytes, func(dec *coding.Decoder) error {
		// decode version
		var version uint8
		dec.Uint8(&version)
		if version != 1 {
			return fmt.Errorf("stdset: decode map: invalid version")
		}

		// decode prefix
		dec.VarBytes(&m.Prefix, true)

		// decode length
		var length uint64
		dec.VarUint(&length)

		// decode pairs
		m.Pairs = map[string][]byte{}
		for i := 0; i < int(length); i++ {
			// decode key
			var key string
			dec.VarString(&key, true)

			// decode value
			var value []byte
			dec.VarBytes(&value, true)

			// set pair
			m.Pairs[key] = value
		}

		return nil
	})
}
