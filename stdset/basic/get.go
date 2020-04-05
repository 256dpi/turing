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
	// get value
	var err error
	g.Value, g.Exists, err = txn.Copy(g.Key)
	if err != nil {
		return err
	}

	return nil
}
