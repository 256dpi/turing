package basic

import (
	"encoding/json"

	"github.com/256dpi/turing"
)

type Get struct {
	Key    []byte `json:"key"`
	Value  []byte `json:"value"`
	Exists bool   `json:"exists"`
}

func (g *Get) Name() string {
	return "stdset/basic.Get"
}

func (g *Get) Build() turing.Instruction {
	return &Get{}
}

func (g *Get) Encode() ([]byte, error) {
	return json.Marshal(g)
}

func (g *Get) Decode(data []byte) error {
	return json.Unmarshal(data, g)
}

func (g *Get) Execute(txn *turing.Transaction) error {
	// reset
	g.Value = nil
	g.Exists = false

	// get pair
	pair, err := txn.Get(g.Key)
	if err != nil {
		return err
	}

	// return if missing
	if pair == nil {
		return nil
	}

	// copy value
	g.Value, err = pair.CopyValue(nil)
	if err != nil {
		return err
	}

	// set flag
	g.Exists = true

	return nil
}

func (g *Get) Cardinality() int {
	return 0
}

func (g *Get) ReadOnly() bool {
	return true
}
