package turing

import (
	"encoding/json"
	"errors"
	"io"

	"github.com/lni/dragonboat/statemachine"
)

// TODO: Handle conflicts.

// TODO: Handle too big transactions.

type command struct {
	Name string `json:"name"`
	Data []byte `json:"data"`
}

type replicator struct {
	config       Config
	database     *database
	instructions map[string]Instruction
}

func newReplicator(config Config) *replicator {
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
	database, index, err := openDatabase(r.config.dbDir())
	if err != nil {
		return 0, err
	}

	// set database
	r.database = database

	return index, nil
}

func (r *replicator) Update(entries []statemachine.Entry) []statemachine.Entry {
	// observe
	defer observe(operationMetrics.WithLabelValues("replicator.update"))()

	// prepare instruction list
	list := make([]Instruction, len(entries))

	// prepare index
	var index uint64

	// decode instructions
	for i := range entries {
		// parse command
		var cmd command
		err := json.Unmarshal(entries[i].Cmd, &cmd)
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

		// add instruction
		list[i] = instruction

		// set last index
		index = entries[i].Index
	}

	// execute instructions
	err := r.database.update(list, index)
	if err != nil {
		panic(err.Error())
	}

	// encode instructions
	for i := range entries {
		// encode instruction
		bytes, err := encodeInstruction(list[i])
		if err != nil {
			panic(err.Error())
		}

		// set result
		entries[i].Result = statemachine.Result{
			Data: bytes,
		}
	}

	return entries
}

func (r *replicator) Lookup(data []byte) ([]byte, error) {
	// observe
	defer observe(operationMetrics.WithLabelValues("replicator.lookup"))()

	// parse command
	var cmd command
	err := json.Unmarshal(data, &cmd)
	if err != nil {
		return nil, err
	}

	// get factory instruction
	factory, ok := r.instructions[cmd.Name]
	if !ok {
		return nil, errors.New("missing instruction: " + cmd.Name)
	}

	// create new instruction
	instruction := buildInstruction(factory)

	// decode instruction
	err = decodeInstruction(cmd.Data, instruction)
	if err != nil {
		return nil, err
	}

	// perform lookup
	err = r.database.lookup(instruction)
	if err != nil {
		return nil, err
	}

	// encode instruction
	bytes, err := encodeInstruction(instruction)
	if err != nil {
		return nil, err
	}

	return bytes, nil
}

func (r *replicator) PrepareSnapshot() (interface{}, error) {
	return nil, nil
}

func (r *replicator) SaveSnapshot(_ interface{}, sink io.Writer, abort <-chan struct{}) error {
	return r.database.backup(sink)
}

func (r *replicator) RecoverFromSnapshot(source io.Reader, abort <-chan struct{}) error {
	return r.database.restore(source)
}

func (r *replicator) Close() {
	_ = r.database.close()
}

func (r *replicator) GetHash() uint64 {
	return 42
}
