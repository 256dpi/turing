package basic

import (
	"github.com/vmihailenco/msgpack/v4"

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

func (g *Get) Effect() int {
	return 0
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

func (g *Get) Encode() ([]byte, turing.Ref, error) {
	buf, err := msgpack.Marshal(g)
	return buf, turing.NoopRef, err
}

func (g *Get) Decode(bytes []byte) error {
	return msgpack.Unmarshal(bytes, g)
}
