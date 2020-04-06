package basic

import (
	"github.com/256dpi/turing"
)

type Get struct {
	Key    []byte `msgpack:"k,omitempty"`
	Value  []byte `msgpack:"v,omitempty"`
	Exists bool   `msgpack:"e,omitempty"`
}

func (g *Get) Describe() turing.Description {
	return turing.Description{
		Name: "std/basic/Get",
	}
}

func (g *Get) Execute(txn *turing.Transaction) error {
	// get value
	var err error
	g.Value, g.Exists, err = txn.Copy(g.Key)
	if err != nil {
		return err
	}

	return nil
}
