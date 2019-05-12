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

type stateMachine struct {
	database *badger.DB

	instructions map[string]Instruction
}

func newStateMachine(db *badger.DB, instructions []Instruction) *stateMachine {
	// create instruction map
	im := make(map[string]Instruction)
	for _, i := range instructions {
		im[i.Name()] = i
	}

	// create state machine
	stateMachine := &stateMachine{
		database:     db,
		instructions: im,
	}

	return stateMachine
}

func (m *stateMachine) Apply(l *raft.Log) interface{} {
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
	err = m.database.Update(func(txn *badger.Txn) error {
		return instruction.Execute(&Transaction{txn: txn})
	})
	if err != nil {
		panic("failed to apply instruction: " + c.Name)
	}

	return nil
}

func (m *stateMachine) Snapshot() (raft.FSMSnapshot, error) {
	return m, nil
}

func (m *stateMachine) Persist(sink raft.SnapshotSink) error {
	// backup database
	_, err := m.database.Backup(sink, 0)
	if err != nil {
		return err
	}

	return nil
}

func (m *stateMachine) Release() {
	// do nothing
}

func (m *stateMachine) Restore(rc io.ReadCloser) error {
	// TODO: Clear database beforehand?

	// load backup
	err := m.database.Load(rc)
	if err != nil {
		return err
	}

	return nil
}
