package turing

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/lni/dragonboat/statemachine"
)

// TODO: Handle conflicts.

type command struct {
	Name string `json:"name"`
	Data []byte `json:"data"`
}

type replicator struct {
	config       Config
	manager      *manager
	database     *database
	instructions map[string]Instruction
}

func newReplicator(config Config, manager *manager) *replicator {
	// create instruction map
	instructions := make(map[string]Instruction)
	for _, i := range config.Instructions {
		instructions[i.Describe().Name] = i
	}

	// create replicator
	replicator := &replicator{
		config:       config,
		manager:      manager,
		instructions: instructions,
	}

	return replicator
}

func (r *replicator) Open(stop <-chan struct{}) (uint64, error) {
	// open database
	database, index, err := openDatabase(r.config.dbDir(), r.manager)
	if err != nil {
		return 0, err
	}

	// set database
	r.database = database

	return index, nil
}

func (r *replicator) Update(entries []statemachine.Entry) ([]statemachine.Entry, error) {
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
			return nil, err
		}

		// get factory instruction
		factory, ok := r.instructions[cmd.Name]
		if !ok {
			return nil, fmt.Errorf("missing instruction: " + cmd.Name)
		}

		// create new instruction
		instruction := buildInstruction(factory)

		// decode instruction
		err = decodeInstruction(cmd.Data, instruction)
		if err != nil {
			return nil, err
		}

		// add instruction
		list[i] = instruction

		// set last index
		index = entries[i].Index
	}

	// execute instructions
	err := r.database.update(list, index)
	if err != nil {
		return nil, err
	}

	// encode instructions
	for i := range entries {
		// encode instruction
		bytes, err := encodeInstruction(list[i])
		if err != nil {
			return nil, err
		}

		// set result
		entries[i].Result = statemachine.Result{
			Data: bytes,
		}
	}

	return entries, nil
}

func (r *replicator) Sync() error {
	return nil
}

func (r *replicator) Lookup(data interface{}) (interface{}, error) {
	// observe
	defer observe(operationMetrics.WithLabelValues("replicator.lookup"))()

	// get instruction
	instruction := data.(Instruction)

	// perform lookup
	err := r.database.lookup(instruction)
	if err != nil {
		return nil, err
	}

	return instruction, nil
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

func (r *replicator) Close() error {
	return r.database.close()
}
