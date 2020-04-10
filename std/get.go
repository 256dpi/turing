package std

import (
	"github.com/vmihailenco/msgpack/v4"

	"github.com/256dpi/turing"
)

type Get struct {
	Key    []byte `msgpack:"k,omitempty"`
	Value  []byte `msgpack:"v,omitempty"`
	Exists bool   `msgpack:"e,omitempty"`
}

var getDesc = &turing.Description{
	Name: "turing/Get",
}

func (g *Get) Describe() *turing.Description {
	return getDesc
}

func (g *Get) Effect() int {
	return 0
}

func (g *Get) Execute(txn *turing.Transaction) error {
	// get value
	err := txn.Use(g.Key, func(value []byte) error {
		g.Value = turing.Copy(value)
		g.Exists = true
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (g *Get) Encode() ([]byte, turing.Ref, error) {
	buf, err := msgpack.Marshal(g)
	return buf, turing.NoopRef, err
}

func (g *Get) Decode(bytes []byte) error {
	return msgpack.Unmarshal(bytes, g)
}
