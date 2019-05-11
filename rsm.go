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

type rsm struct {
	db *badger.DB

	instructions map[string]Instruction
}

func (m *rsm) Apply(l *raft.Log) interface{} {
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
	err = m.db.Update(func(txn *badger.Txn) error {
		return instruction.Execute(&Transaction{txn: txn})
	})
	if err != nil {
		panic("failed to apply instruction: " + c.Name)
	}

	return nil
}

func (m *rsm) Snapshot() (raft.FSMSnapshot, error) {
	return m, nil
}

func (m *rsm) Persist(sink raft.SnapshotSink) error {
	// backup database
	_, err := m.db.Backup(sink, 0)
	if err != nil {
		return err
	}

	return nil
}

func (m *rsm) Release() {
	// do nothing
}

func (m *rsm) Restore(rc io.ReadCloser) error {
	// TODO: Clear database beforehand?

	// load backup
	err := m.db.Load(rc)
	if err != nil {
		return err
	}

	return nil
}
