package turing

import (
	"encoding/json"
	"io"
	"strconv"

	"github.com/dgraph-io/badger"
	"github.com/lni/dragonboat/statemachine"
)

// TODO: Ignore index from all user operations.

var indexKey = []byte("!?idx")

type command struct {
	Name string `json:"name"`
	Data []byte `json:"data"`
}

type replicator struct {
	config       MachineConfig
	database     *database
	instructions map[string]Instruction
}

func newReplicator(config MachineConfig) *replicator {
	// create instruction map
	instructions := make(map[string]Instruction)
	for _, i := range config.Instructions {
		instructions[i.Name()] = i
	}

	// create replicator
	replicator := &replicator{
		config:       config,
		instructions: instructions,
	}

	return replicator
}

func (m *replicator) Open(stop <-chan struct{}) (uint64, error) {
	// open database
	database, err := openDatabase(m.config.dbDir(), m.config.Logger)
	if err != nil {
		return 0, err
	}

	// set database
	m.database = database

	// prepare index
	var index uint64

	// get last committed index
	err = database.View(func(txn *badger.Txn) error {
		// get key
		item, err := txn.Get(indexKey)
		if err == badger.ErrKeyNotFound {
			return nil
		} else if err != nil {
			return err
		}

		// parse value
		err = item.Value(func(val []byte) error {
			index, err = strconv.ParseUint(string(val), 10, 64)
			return err
		})
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return 0, err
	}

	return index, nil
}

func (m *replicator) Update(entries []statemachine.Entry) []statemachine.Entry {
	// TODO: Handle errors.

	// handle all entries
	for _, entry := range entries {
		// parse command
		var c command
		err := json.Unmarshal(entry.Cmd, &c)
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
			// execute transaction
			err = instruction.Execute(&Transaction{txn: txn})
			if err != nil {
				return err
			}

			// set seq
			err = txn.Set(indexKey, []byte(strconv.FormatUint(entry.Index, 10)))
			if err != nil {
				return err
			}

			return nil
		})
		if err != nil {
			panic("failed to apply instruction: " + c.Name)
		}

		// encode instruction
		bytes, err := json.Marshal(instruction)
		if err != nil {
			panic("failed to encode instruction: " + c.Name)
		}

		// set result
		entry.Result = statemachine.Result{
			Data: bytes,
		}
	}

	return entries
}

func (m *replicator) Lookup(data []byte) ([]byte, error) {
	// TODO: Handle errors.

	// parse command
	var c command
	err := json.Unmarshal(data, &c)
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
	err = m.database.View(func(txn *badger.Txn) error {
		return instruction.Execute(&Transaction{txn: txn})
	})
	if err != nil {
		panic("failed to apply instruction: " + c.Name)
	}

	// encode instruction
	bytes, err := json.Marshal(instruction)
	if err != nil {
		panic("failed to encode instruction: " + c.Name)
	}

	return bytes, nil
}

func (m *replicator) PrepareSnapshot() (interface{}, error) {
	return nil, nil
}

func (m *replicator) SaveSnapshot(checkpoint interface{}, sink io.Writer, abort <-chan struct{}) error {
	// backup database
	_, err := m.database.Backup(sink, 0)
	if err != nil {
		return err
	}

	return nil
}

func (m *replicator) RecoverFromSnapshot(source io.Reader, abort <-chan struct{}) error {
	// TODO: Clear database beforehand?

	// load backup
	err := m.database.Load(source)
	if err != nil {
		return err
	}

	return nil
}

func (m *replicator) Close() {
	// close database
	_ = m.database.Close()
}

func (m *replicator) GetHash() uint64 {
	return 42
}
