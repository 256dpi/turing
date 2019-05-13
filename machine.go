package turing

import (
	"encoding/json"
	"errors"
)

var ErrNoLeader = errors.New("no leader")

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

func (m *Machine) Update(i Instruction) error {
	// encode instruction
	id, err := i.Encode()
	if err != nil {
		return err
	}

	// prepare command
	cmd := &command{
		Name: i.Name(),
		Data: id,
	}

	// encode command
	cd, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	// apply command
	result, err := m.coordinator.update(cd)
	if err != nil {
		return err
	}

	// decode result
	if result != nil {
		err = json.Unmarshal(result, i)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Machine) View(i Instruction) error {
	// encode instruction
	id, err := i.Encode()
	if err != nil {
		return err
	}

	// prepare command
	cmd := &command{
		Name: i.Name(),
		Data: id,
	}

	// encode command
	cd, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	// apply command
	result, err := m.coordinator.lookup(cd)
	if err != nil {
		return err
	}

	// decode result
	if result != nil {
		err = json.Unmarshal(result, i)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Machine) Close() {
	// TODO: Implement.
}
