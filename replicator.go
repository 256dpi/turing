package turing

import (
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
	timer := observe(operationMetrics, "replicator.Update")
	defer timer.finish()

	// handle entries
	for i, entry := range entries {
		// decode command (no need to clone as only used temporary)
		var cmd Command
		err := cmd.Decode(entry.Cmd, false)
		if err != nil {
			return nil, err
		}

		// prepare instruction list
		list := make([]Instruction, 0, len(cmd.Operations))

		// decode operations
		for _, op := range cmd.Operations {
			// build instruction
			ins, err := r.registry.build(op.Name)
			if err != nil {
				return nil, err
			}

			// decode instruction
			err = ins.Decode(op.Data)
			if err != nil {
				return nil, err
			}

			// add instruction
			list = append(list, ins)
		}

		// execute instructions
		err = r.database.update(list, entry.Index)
		if err != nil {
			return nil, err
		}

		// encode operations
		for j, ins := range list {
			// encode instruction
			bytes, ref, err := ins.Encode()
			if err != nil {
				return nil, err
			}

			// ensure release
			defer ref.Release()

			// set bytes
			cmd.Operations[j].Data = bytes
		}

		// TODO: Borrow?

		// encode command
		bytes, _, err := cmd.Encode(false)
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
	timer := observe(operationMetrics, "replicator.Lookup")
	defer timer.finish()

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
