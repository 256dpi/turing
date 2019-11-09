package basic

import (
	"github.com/256dpi/turing"
)

type Get struct {
	Key    []byte `json:"key"`
	Value  []byte `json:"value"`
	Exists bool   `json:"exists"`
}

func (g *Get) Describe() turing.Description {
	return turing.Description{
		Name: "stdset/basic.Get",
	}
}

func (g *Get) Execute(txn *turing.Transaction) error {
	// reset
	g.Value = nil
	g.Exists = false

	// get value
	value, err := txn.Get(g.Key)
	if err != nil {
		return err
	}

	// return if missing
	if value == nil {
		return nil
	}

	// copy value
	g.Value = turing.Copy(nil, value)

	// set flag
	g.Exists = true

	return nil
}
