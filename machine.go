package turing

import (
	"fmt"
)

// TODO: How should parallel instructions executions be handled?
//  Should we try to batch requests before handing them over to raft?

// Machine maintains a raft cluster with members and maintains consensus about the
// execute instructions on the distributed database.
type Machine struct {
	config      Config
	registry    *registry
	manager     *manager
	balancer    *balancer
	coordinator *coordinator
	controller  *controller
	database    *database
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

	// prepare database and controller
	var database *database
	var controller *controller
	if config.Standalone {
		// open database
		database, _, err = openDatabase(config, registry, manager)
		if err != nil {
			return nil, err
		}

		// create controller
		controller = newController(config, database)
	}

	// create machine
	m := &Machine{
		config:      config,
		registry:    registry,
		manager:     manager,
		balancer:    balancer,
		coordinator: coordinator,
		controller:  controller,
		database:    database,
	}

	return m, nil
}

// Execute will execute the specified instruction. NonLinear may be set to true
// to allow read only instructions to query data without linearizability
// guarantees. This may be substantially faster but return stale data.
func (m *Machine) Execute(instruction Instruction, nonLinear bool) error {
	// observe
	timer := observe(operationMetrics, "Machine.Execute")
	defer timer.ObserveDuration()

	// get description
	description := instruction.Describe()

	// validate description
	err := description.Validate()
	if err != nil {
		return err
	}

	// check registry
	if m.registry.instructions[description.Name] == nil {
		return fmt.Errorf("missing instruction: %s", description.Name)
	}

	// execute directly if standalone
	if m.config.Standalone {
		// perform lookup
		if description.Effect == 0 {
			return m.controller.lookup(instruction)
		}

		// perform update
		return m.controller.update(instruction)
	}

	// balance
	m.balancer.get(description.Effect != 0)
	defer m.balancer.put(description.Effect != 0)

	// immediately perform read
	if description.Effect == 0 {
		err = m.coordinator.lookup(instruction, nonLinear)
		if err != nil {
			return err
		}

		return nil
	}

	// perform update
	err = m.coordinator.update(instruction)
	if err != nil {
		return err
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
	// close coordinator
	if m.coordinator != nil {
		m.coordinator.close()
	}

	// close controller
	if m.controller != nil {
		_ = m.controller.close
	}

	// close database
	if m.database != nil {
		_ = m.database.close()
	}
}
