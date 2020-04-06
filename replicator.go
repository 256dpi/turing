package turing

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/lni/dragonboat/v3/statemachine"
)

type command struct {
	Name string `json:"name"`
	Data []byte `json:"data"`
}

type replicator struct {
	config   Config
	manager  *manager
	database *database
	registry map[string]Instruction
}

func newReplicator(config Config, manager *manager) *replicator {
	// create instruction registry
	registry := make(map[string]Instruction)
	for _, i := range config.Instructions {
		registry[i.Describe().Name] = i
	}

	// create replicator
	replicator := &replicator{
		config:   config,
		manager:  manager,
		registry: registry,
	}

	return replicator
}

func (r *replicator) Open(stop <-chan struct{}) (uint64, error) {
	// open database
	database, index, err := openDatabase(r.config, r.manager)
	if err != nil {
		return 0, err
	}

	// set database
	r.database = database

	return index, nil
}

func (r *replicator) Update(entries []statemachine.Entry) ([]statemachine.Entry, error) {
	// observe
	timer := observe(operationMetrics, "replicator.update")
	defer timer.ObserveDuration()

	// prepare instruction and index list
	instructions := make([]Instruction, len(entries))
	indexes := make([]uint64, len(entries))

	// decode instructions
	for i, entry := range entries {
		// parse command
		var cmd command
		err := json.Unmarshal(entry.Cmd, &cmd)
		if err != nil {
			return nil, err
		}

		// get factory instruction
		factory, ok := r.registry[cmd.Name]
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

		// set instruction and index
		instructions[i] = instruction
		indexes[i] = entry.Index
	}

	// execute instructions
	err := r.database.update(instructions, indexes)
	if err != nil {
		return nil, err
	}

	// encode instructions
	for i := range entries {
		// encode instruction
		bytes, err := encodeInstruction(instructions[i])
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
	return r.database.sync()
}

func (r *replicator) Lookup(data interface{}) (interface{}, error) {
	// observe
	timer := observe(operationMetrics, "replicator.lookup")
	defer timer.ObserveDuration()

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
