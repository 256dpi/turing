package turing

import (
	"io"

	"github.com/cockroachdb/pebble"
	"github.com/lni/dragonboat/v3/statemachine"

	"github.com/256dpi/turing/wire"
)

type replicator struct {
	config       Config
	registry     *registry
	manager      *manager
	database     *database
	instructions []Instruction
	operations   []wire.Operation
	references   []Ref
}

func newReplicator(config Config, registry *registry, manager *manager) *replicator {
	return &replicator{
		config:       config,
		registry:     registry,
		manager:      manager,
		instructions: make([]Instruction, config.ProposalBatchSize),
		operations:   make([]wire.Operation, config.ProposalBatchSize),
		references:   make([]Ref, config.ProposalBatchSize),
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

var replicatorUpdate = systemMetrics.WithLabelValues("replicator.Update")

func (r *replicator) Update(entries []statemachine.Entry) ([]statemachine.Entry, error) {
	// observe
	timer := observe(replicatorUpdate)
	defer timer.finish()

	// handle entries
	for i, entry := range entries {
		// reset lists
		instructions := r.instructions[:0]
		operations := r.operations[:0]
		references := r.references[:0]

		// decode command
		err := wire.WalkCommand(entry.Cmd, func(i int, op wire.Operation) (bool, error) {
			// build instruction
			ins, err := r.registry.build(op.Name)
			if err != nil {
				return false, err
			}

			// decode instruction
			err = ins.Decode(op.Code)
			if err != nil {
				return false, err
			}

			// add instruction
			instructions = append(instructions, ins)

			return true, nil
		})
		if err != nil {
			return nil, err
		}

		// execute instructions
		err = r.database.update(instructions, entry.Index)
		if err != nil {
			return nil, err
		}

		// encode operations
		for _, ins := range instructions {
			// append empty operation when no result
			if ins.Describe().NoResult {
				operations = append(operations, wire.Operation{
					Name: ins.Describe().Name,
				})

				continue
			}

			// encode instruction
			bytes, ref, err := ins.Encode()
			if err != nil {
				return nil, err
			}

			// set append operation
			operations = append(operations, wire.Operation{
				Name: ins.Describe().Name,
				Code: bytes,
			})

			// append reference
			if ref != nil {
				references = append(references, ref)
			}

			// recycle instruction if possible
			recycler := ins.Describe().Recycler
			if recycler != nil {
				recycler(ins)
			}
		}

		// prepare command
		cmd := wire.Command{
			Operations: operations,
		}

		// TODO: Borrow slice.
		//  Improve dragonboat to provide a release mechanism

		// encode command
		bytes, _, err := cmd.Encode(false)
		if err != nil {
			return nil, err
		}

		// release references
		for _, ref := range references {
			ref.Release()
		}

		// set result
		entries[i].Result.Data = bytes
	}

	return entries, nil
}

func (r *replicator) Sync() error {
	return r.database.sync()
}

var replicatorLookup = systemMetrics.WithLabelValues("replicator.Lookup")

func (r *replicator) Lookup(data interface{}) (interface{}, error) {
	// observe
	timer := observe(replicatorLookup)
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
	return r.database.snapshot()
}

func (r *replicator) SaveSnapshot(snapshot interface{}, sink io.Writer, abort <-chan struct{}) error {
	return r.database.backup(snapshot.(*pebble.Snapshot), sink, abort)
}

func (r *replicator) RecoverFromSnapshot(source io.Reader, abort <-chan struct{}) error {
	return r.database.restore(source)
}

func (r *replicator) Close() error {
	return r.database.close()
}
