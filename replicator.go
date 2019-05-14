package turing

import (
	"encoding/json"
	"io"
	"strconv"

	"github.com/dgraph-io/badger"
	"github.com/lni/dragonboat/statemachine"
)

// TODO: Handle conflicts.

// TODO: Handle too big transactions.

var indexKey = []byte("$index")

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
		instructions[i.Describe().Name] = i
	}

	// create replicator
	replicator := &replicator{
		config:       config,
		instructions: instructions,
	}

	return replicator
}

func (r *replicator) Open(stop <-chan struct{}) (uint64, error) {
	// open database
	database, err := openDatabase(r.config.dbDir(), logSink)
	if err != nil {
		return 0, err
	}

	// set database
	r.database = database

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

func (r *replicator) Update(entries []statemachine.Entry) []statemachine.Entry {
	// TODO: Handle errors.

	// TODO: Improve batching.

	// prepare cardinality
	cardinality := 0

	// create transaction
	txn := r.database.NewTransaction(true)

	// handle all entries
	for _, entry := range entries {
		// parse command
		var cmd command
		err := json.Unmarshal(entry.Cmd, &cmd)
		if err != nil {
			panic(err.Error())
		}

		// get factory instruction
		factory, ok := r.instructions[cmd.Name]
		if !ok {
			panic("missing instruction: " + cmd.Name)
		}

		// create new instruction
		instruction := buildInstruction(factory)

		// decode instruction
		err = decodeInstruction(cmd.Data, instruction)
		if err != nil {
			panic(err.Error())
		}

		// increment cardinality
		cardinality += instruction.Describe().Cardinality

		// check if new transaction is needed
		if cardinality >= int(r.database.MaxBatchCount()) {
			// commit current transaction
			err = txn.Commit()
			if err != nil {
				panic(err.Error())
			}

			// create new transaction
			txn = r.database.NewTransaction(true)

			// reset cardinality
			cardinality = instruction.Describe().Cardinality
		}

		// execute transaction
		err = instruction.Execute(&Transaction{txn: txn})
		if err != nil {
			panic(err.Error())
		}

		// set seq
		err = txn.Set(indexKey, []byte(strconv.FormatUint(entry.Index, 10)))
		if err != nil {
			panic(err.Error())
		}

		// encode instruction
		bytes, err := encodeInstruction(instruction)
		if err != nil {
			panic(err.Error())
		}

		// set result
		entry.Result = statemachine.Result{
			Data: bytes,
		}
	}

	// commit transaction
	err := txn.Commit()
	if err != nil {
		panic(err.Error())
	}

	return entries
}

func (r *replicator) Lookup(data []byte) ([]byte, error) {
	// TODO: Handle errors.

	// parse command
	var cmd command
	err := json.Unmarshal(data, &cmd)
	if err != nil {
		panic(err.Error())
	}

	// get factory instruction
	factory, ok := r.instructions[cmd.Name]
	if !ok {
		panic("missing instruction: " + cmd.Name)
	}

	// create new instruction
	instruction := buildInstruction(factory)

	// decode instruction
	err = decodeInstruction(cmd.Data, instruction)
	if err != nil {
		panic(err.Error())
	}

	// apply instruction
	err = r.database.View(func(txn *badger.Txn) error {
		return instruction.Execute(&Transaction{txn: txn})
	})
	if err != nil {
		panic(err.Error())
	}

	// encode instruction
	bytes, err := encodeInstruction(instruction)
	if err != nil {
		panic(err.Error())
	}

	return bytes, nil
}

func (r *replicator) PrepareSnapshot() (interface{}, error) {
	return nil, nil
}

func (r *replicator) SaveSnapshot(_ interface{}, sink io.Writer, abort <-chan struct{}) error {
	// backup database
	_, err := r.database.Backup(sink, 0)
	if err != nil {
		return err
	}

	return nil
}

func (r *replicator) RecoverFromSnapshot(source io.Reader, abort <-chan struct{}) error {
	// TODO: Clear database beforehand?

	// load backup
	err := r.database.Load(source)
	if err != nil {
		return err
	}

	return nil
}

func (r *replicator) Close() {
	// close database
	_ = r.database.Close()
}

func (r *replicator) GetHash() uint64 {
	return 42
}
