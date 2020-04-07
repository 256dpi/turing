package turing

import (
	"context"
	"fmt"
	"time"
)

// TODO: How should parallel instructions executions be handled?
//  Should we try to batch requests before handing them over to raft?

// Machine maintains a raft cluster with members and maintains consensus about the
// execute instructions on the distributed database.
type Machine struct {
	config      Config
	registry    *registry
	manager     *manager
	coordinator *coordinator
	database    *database
	balancer    *balancer
}

// Start will create a new machine using the specified configuration.
func Start(config Config) (*Machine, error) {
	// check config
	err := config.check()
	if err != nil {
		return nil, err
	}

	// build registry
	registry, err := buildRegistry(config)
	if err != nil {
		return nil, err
	}

	// prepare manager
	manager := newManager()

	// TODO: Balancer should be configurable.

	// prepare balancer
	balancer := newBalancer(10000, 0)

	// prepare coordinator
	var coordinator *coordinator
	if !config.Standalone {
		coordinator, err = createCoordinator(config, registry, manager)
		if err != nil {
			return nil, err
		}
	}

	// prepare database
	var database *database
	if config.Standalone {
		database, _, err = openDatabase(config, registry, manager)
		if err != nil {
			return nil, err
		}
	}

	// create machine
	m := &Machine{
		config:      config,
		registry:    registry,
		manager:     manager,
		coordinator: coordinator,
		database:    database,
		balancer:    balancer,
	}

	return m, nil
}

// Execute will execute the specified instruction. NonLinear may be set to true
// to allow read only instructions to query data without linearizability
// guarantees. This may be substantially faster but return stale data.
func (m *Machine) Execute(ctx context.Context, instruction Instruction, nonLinear bool) error {
	// observe
	timer := observe(operationMetrics, "Machine.Execute")
	defer timer.ObserveDuration()

	// ensure context
	if ctx == nil {
		ctx = context.Background()
	}

	// TODO: Make timeout configurable.

	// ensure deadline
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
	}

	// get description
	description := instruction.Describe()

	// check registry
	if m.registry.instructions[description.Name] == nil {
		return fmt.Errorf("missing instruction: %s", description.Name)
	}

	// validate instruction
	err := description.Validate()
	if err != nil {
		return err
	}

	// execute directly if standalone
	if m.config.Standalone {
		// perform lookup
		if description.Effect == 0 {
			return m.database.lookup(instruction)
		}

		// perform update
		return m.database.update([]Instruction{instruction}, []uint64{0})
	}

	// balance
	m.balancer.get(description.Effect != 0)
	defer m.balancer.put(description.Effect != 0)

	// immediately perform read
	if description.Effect == 0 {
		err = m.coordinator.lookup(ctx, instruction, nonLinear)
		if err != nil {
			return err
		}

		return nil
	}

	// encode instruction
	id, err := instruction.Encode()
	if err != nil {
		return err
	}

	// prepare command
	cmd := Command{
		Name: description.Name,
		Data: id,
	}

	// encode command
	bytes, err := EncodeCommand(cmd)
	if err != nil {
		return err
	}

	// perform update
	result, err := m.coordinator.update(ctx, bytes)
	if err != nil {
		return err
	}

	// decode result
	if result != nil {
		err = instruction.Decode(result)
		if err != nil {
			return err
		}
	}

	return nil
}

// Subscribe will subscribe the provided observer.
func (m *Machine) Subscribe(observer Observer) {
	m.manager.subscribe(observer)
}

// Unsubscribe will unsubscribe the provided observer.
func (m *Machine) Unsubscribe(observer Observer) {
	m.manager.unsubscribe(observer)
}

// Status will return the current status.
func (m *Machine) Status() Status {
	// get status from coordinator
	if m.coordinator != nil {
		return m.coordinator.status()
	}

	return Status{}
}

// Stop will stop the machine.
func (m *Machine) Stop() {
	// close development db
	if m.database != nil {
		_ = m.database.close()
	}

	// close coordinator
	if m.coordinator != nil {
		m.coordinator.close()
	}
}
