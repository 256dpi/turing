package turing

import (
	"encoding/json"
)

// Machine maintains a raft cluster with members and maintains consensus about the
// execute instructions on the distributed data store.
type Machine struct {
	coordinator *coordinator
}

// Create will create a new machine using the specified configuration.
func Create(config Config) (*Machine, error) {
	// check config
	err := config.check()
	if err != nil {
		return nil, err
	}

	// create coordinator
	coordinator, err := createCoordinator(config)
	if err != nil {
		return nil, err
	}

	// create machine
	n := &Machine{
		coordinator: coordinator,
	}

	return n, nil
}

// Execute will execute the specified instruction.
func (m *Machine) Execute(instruction Instruction) error {
	// observe
	defer observe(operationMetrics.WithLabelValues("Machine.Execute"))()

	// validate instruction
	err := instruction.Describe().Validate()
	if err != nil {
		return err
	}

	// encode instruction
	id, err := encodeInstruction(instruction)
	if err != nil {
		return err
	}

	// prepare command
	cmd := &command{
		Name: instruction.Describe().Name,
		Data: id,
	}

	// encode command
	bytes, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	// prepare result
	var result []byte

	// apply command
	if instruction.Describe().Effect == 0 {
		result, err = m.coordinator.lookup(bytes)
		if err != nil {
			return err
		}
	} else {
		result, err = m.coordinator.update(bytes)
		if err != nil {
			return err
		}
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

// Status will return the current status.
func (m *Machine) Status() Status {
	return m.coordinator.status()
}

// Close will close the machine.
func (m *Machine) Close() {
	m.coordinator.close()
}
