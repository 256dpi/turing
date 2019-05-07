package turing

import (
	"encoding/json"
	"io"

	"github.com/hashicorp/raft"
)

type Command struct {
	Name string `json:"name"`
	Data []byte `json:"data"`
}

type FSM struct {
	instructions map[string]Instruction
}

func (m *FSM) Apply(l *raft.Log) interface{} {
	// TODO: Handle errors.

	// parse command
	var c Command
	err := json.Unmarshal(l.Data, &c)
	if err != nil {
		panic("failed to unmarshal raft log")
	}

	// get factory instruction
	factory, ok := m.instructions[c.Name]
	if !ok {
		panic("missing instruction: " + c.Name)
	}

	// create new instruction
	instruction := factory.Build()

	// decode instruction
	err = instruction.Decode(c.Data)
	if err != nil {
		panic("failed to decode instruction: " + c.Name)
	}

	// TODO: Pass in transaction.

	// apply instruction
	err = instruction.Apply(nil)
	if err != nil {
		panic("failed to apply instruction: " + c.Name)
	}

	return nil
}

func (*FSM) Snapshot() (raft.FSMSnapshot, error) {
	panic("implement me")
}

func (*FSM) Restore(io.ReadCloser) error {
	panic("implement me")
}
