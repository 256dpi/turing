package turing

import (
	"fmt"
)

// Options define options used during instruction execution.
type Options struct {
	// StaleRead can be set to execute a stale read. While this is much faster
	// the instruction might read stale data in relationship to the current
	// state of the cluster. In other words, settings this value will reduce
	// the default linearizable guarantee to a serializable guarantee.
	StaleRead bool
}

// Machine maintains a raft cluster with members and maintains consensus about
// the executed instructions on the replicated database.
type Machine struct {
	config      Config
	registry    *registry
	manager     *manager
	coordinator *coordinator
	controller  *controller
}

// Start will create a new machine using the specified configuration.
func Start(config Config) (*Machine, error) {
	// validate config
	err := config.Validate()
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

	// prepare coordinator
	var coordinator *coordinator
	if !config.Standalone {
		coordinator, err = createCoordinator(config, registry, manager)
		if err != nil {
			return nil, err
		}
	}

	// prepare controller
	var controller *controller
	if config.Standalone {
		controller, err = createController(config, registry, manager)
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
		controller:  controller,
	}

	return m, nil
}

var machineExecute = systemMetrics.WithLabelValues("Machine.Execute")

// Execute will execute the specified instruction.
func (m *Machine) Execute(ins Instruction, opts ...Options) error {
	return m.execute(ins, nil, opts...)
}

// ExecuteAsync will execute the specified instruction asynchronously. The
// specified function is called once the instruction has been executed.
func (m *Machine) ExecuteAsync(ins Instruction, fn func(error), opts ...Options) error {
	return m.execute(ins, fn, opts...)
}

func (m *Machine) execute(ins Instruction, fn func(error), opts ...Options) error {
	// observe
	timer := observe(machineExecute)
	defer timer.finish()

	// get options
	var options Options
	if len(opts) == 1 {
		options = opts[0]
	}

	// get description
	desc := ins.Describe()

	// validate description
	err := desc.Validate()
	if err != nil {
		return err
	}

	// get effect
	effect := ins.Effect()
	if effect > m.config.MaxEffect {
		return fmt.Errorf("turing: instruction effect too high")
	} else if effect < 0 && effect != UnboundedEffect {
		return fmt.Errorf("turing: invalid instruction effect")
	}

	// check registry
	if m.registry.ins[desc.Name] == nil {
		return fmt.Errorf("turing: missing instruction: %s", desc.Name)
	}

	// execute directly if standalone
	if m.config.Standalone {
		// perform lookup
		if effect == 0 {
			return m.controller.lookup(ins, fn)
		}

		// perform update
		return m.controller.update(ins, fn)
	}

	// immediately perform read
	if effect == 0 {
		err = m.coordinator.lookup(ins, fn, options)
		if err != nil {
			return err
		}

		return nil
	}

	// perform update
	err = m.coordinator.update(ins, fn)
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
		_ = m.controller.close()
	}
}
