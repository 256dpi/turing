package turing

import (
	"encoding/json"
)

// Machine maintains a raft cluster with members and maintains consensus about the
// execute instructions on the distributed database.
type Machine struct {
	manager     *manager
	coordinator *coordinator
	development *database
}

// Start will create a new machine using the specified configuration.
func Start(config Config) (*Machine, error) {
	// check config
	err := config.check()
	if err != nil {
		return nil, err
	}

	// create machine
	m := &Machine{
		manager: newManager(),
	}

	// create coordinator in normal mode
	if !config.Development {
		m.coordinator, err = createCoordinator(config, m.manager)
		if err != nil {
			return nil, err
		}
	}

	// create database in development mode
	if config.Development {
		m.development, _, err = openDatabase(config.dbDir(), m.manager)
		if err != nil {
			return nil, err
		}
	}

	return m, nil
}

// Execute will execute the specified instruction. NonLinear may be set to true
// to allow read only instructions to query data without linearizability
// guarantees. This may be substantially faster but return stale data.
func (m *Machine) Execute(instruction Instruction, nonLinear bool) error {
	// observe
	defer observe(operationMetrics.WithLabelValues("Machine.Execute"))()

	// get description
	description := instruction.Describe()

	// validate instruction
	err := description.Validate()
	if err != nil {
		return err
	}

	// execute directly in development mode
	if m.development != nil {
		// perform lookup
		if description.Effect == 0 {
			return m.development.lookup(instruction)
		}

		// perform update
		return m.development.update([]Instruction{instruction}, 0)
	}

	// immediately execute lookups
	if description.Effect == 0 {
		err = m.coordinator.lookup(instruction, nonLinear)
		if err != nil {
			return err
		}

		return nil
	}

	// encode instruction
	id, err := encodeInstruction(instruction)
	if err != nil {
		return err
	}

	// prepare command
	cmd := &command{
		Name: description.Name,
		Data: id,
	}

	// encode command
	bytes, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	// apply command
	result, err := m.coordinator.update(bytes)
	if err != nil {
		return err
	}

	// decode result
	if result != nil {
		err = decodeInstruction(result, instruction)
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
	if m.development != nil {
		_ = m.development.close()
	}

	// close coordinator
	if m.coordinator != nil {
		m.coordinator.close()
	}
}
