package basic

import (
	"github.com/256dpi/turing"
)

type Map struct {
	Prefix []byte            `json:"prefix,omitempty"`
	Pairs  map[string][]byte `json:"pairs,omitempty"`
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
