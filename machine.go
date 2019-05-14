package turing

import (
	"encoding/json"
)

type Machine struct {
	coordinator *coordinator
}

func CreateMachine(config MachineConfig) (*Machine, error) {
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

func (m *Machine) IsLeader() bool {
	return m.coordinator.isLeader()
}

func (m *Machine) Leader() *Route {
	return m.coordinator.leader()
}

func (m *Machine) State() string {
	return m.coordinator.state()
}

func (m *Machine) Execute(i Instruction) error {
	// encode instruction
	id, err := encodeInstruction(i)
	if err != nil {
		return err
	}

	// prepare command
	cmd := &command{
		Name: i.Describe().Name,
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
	if i.Describe().Effect == 0 {
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
		err = decodeInstruction(result, i)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Machine) Close() {
	m.coordinator.close()
}
