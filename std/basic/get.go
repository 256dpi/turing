package basic

import (
	"github.com/256dpi/turing"
)

type Get struct {
	Key    []byte `json:"k,omitempty"`
	Value  []byte `json:"v,omitempty"`
	Exists bool   `json:"e,omitempty"`
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
