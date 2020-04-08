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

	// decode instructions
	for i, entry := range entries {
		// decode command
		var cmd Command
		err := cmd.Decode(entry.Cmd, false)
		if err != nil {
			return nil, err
		}

		// prepare instruction and index list
		instructions := make([]Instruction, 0, len(cmd.Operations))
		indexes := make([]uint64, 0, len(cmd.Operations))

		// decode operations
		for _, op := range cmd.Operations {
			// get factory instruction
			factory, ok := r.registry.instructions[op.Name]
			if !ok {
				return nil, fmt.Errorf("turing: missing instruction: " + op.Name)
			}

			// create new instruction
			instruction := buildInstruction(factory)

			// decode instruction
			err = instruction.Decode(op.Data)
			if err != nil {
				return nil, err
			}

			// set instruction and index
			instructions = append(instructions, instruction)
			indexes = append(indexes, entry.Index)
		}

		// execute instructions
		err = r.database.update(instructions, indexes)
		if err != nil {
			return nil, err
		}

		// encode operations
		for j, ins := range instructions {
			// encode instruction
			bytes, err := ins.Encode()
			if err != nil {
				return nil, err
			}

			// set bytes
			cmd.Operations[j].Data = bytes
		}

		// encode command
		bytes, err := cmd.Encode()
		if err != nil {
			return nil, err
		}

		// set result
		entries[i].Result.Data = bytes
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

	// get instructions
	list := data.([]Instruction)

	// perform lookup
	err := r.database.lookup(list)
	if err != nil {
		return nil, err
	}

	return nil, nil
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
