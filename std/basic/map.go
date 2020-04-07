package basic

import (
	"github.com/vmihailenco/msgpack/v4"

	"github.com/256dpi/turing"
)

type Map struct {
	Prefix []byte            `msgpack:"p,omitempty"`
	Pairs  map[string][]byte `msgpack:"m,omitempty"`
}

func (m *Map) Describe() turing.Description {
	return turing.Description{
		Name: "std/basic/Map",
	}
}

func (m *Map) Execute(txn *turing.Transaction) error {
	// create map
	m.Pairs = make(map[string][]byte)

	// create iterator
	iter := txn.Iterator(m.Prefix)
	defer iter.Close()

	// iterate through all pairs
	for iter.First(); iter.Valid(); iter.Next() {
		// get value
		value, err := iter.Value(true)
		if err != nil {
			return err
		}

		// set key value
		m.Pairs[string(iter.Key(false))] = value
	}

	return nil
}

func (m *Map) Encode() ([]byte, error) {
	return msgpack.Marshal(m)
}

func (m *Map) Decode(bytes []byte) error {
	return msgpack.Unmarshal(bytes, m)
}
