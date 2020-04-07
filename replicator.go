package turing

import (
	"fmt"
	"io"

	"github.com/lni/dragonboat/v3/statemachine"
)

type replicator struct {
	config   Config
	registry *registry
	manager  *manager
	database *database
}

func newReplicator(config Config, registry *registry, manager *manager) *replicator {
	return &replicator{
		config:   config,
		registry: registry,
		manager:  manager,
	}
}

func (r *replicator) Open(stop <-chan struct{}) (uint64, error) {
	// open database
	database, index, err := openDatabase(r.config, r.registry, r.manager)
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
		cmd, err := DecodeCommand(entry.Cmd, false)
		if err != nil {
			return nil, err
		}

		// get factory instruction
		factory, ok := r.registry.instructions[cmd.Name]
		if !ok {
			return nil, fmt.Errorf("missing instruction: " + cmd.Name)
		}

		// create new instruction
		instruction := buildInstruction(factory)

		// decode instruction
		err = instruction.Decode(cmd.Data)
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
		bytes, err := instructions[i].Encode()
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
	err := r.database.lookup([]Instruction{instruction})
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
