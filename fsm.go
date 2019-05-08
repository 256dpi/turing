package turing

import (
	"encoding/json"
	"io"

	"github.com/dgraph-io/badger"
	"github.com/hashicorp/raft"
)

type command struct {
	Name string `json:"name"`
	Data []byte `json:"data"`
}

type fsm struct {
	db *badger.DB

	instructions map[string]Instruction
}

func (m *fsm) Apply(l *raft.Log) interface{} {
	// TODO: Handle errors.

	// parse command
	var c command
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

	// apply instruction
	err = m.db.Update(instruction.Execute)
	if err != nil {
		panic("failed to apply instruction: " + c.Name)
	}

	return nil
}

func (*fsm) Snapshot() (raft.FSMSnapshot, error) {
	panic("implement me")
}

func (*fsm) Restore(io.ReadCloser) error {
	panic("implement me")
}
