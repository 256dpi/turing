package turing

import (
	"fmt"
	"reflect"
)

type registry struct {
	ins map[string]Instruction
	ops map[string]*Operator
}

func buildRegistry(config Config) (*registry, error) {
	// prepare registry
	reg := &registry{
		ins: map[string]Instruction{},
		ops: map[string]*Operator{},
	}

	// add instructions
	for _, ins := range config.Instructions {
		// get description
		desc := ins.Describe()

		// check existence
		if reg.ins[desc.Name] != nil {
			return nil, fmt.Errorf("turing: build registry: duplicate instruction: %s", desc.Name)
		}

		// set observer
		desc.observer = instructionMetrics.WithLabelValues(desc.Name)

		// store instruction
		reg.ins[desc.Name] = ins

		// add operators
		for _, op := range desc.Operators {
			// get name
			name := op.Name

			// check existing operator
			eop, ok := reg.ops[name]
			if ok {
				// check equality
				if eop != op {
					return nil, fmt.Errorf("turing: build registry: different operator for same name: %s", name)
				}

				continue
			}

			// set counter
			op.counter = operatorMetrics.WithLabelValues(op.Name)

			// store operator
			reg.ops[name] = op
		}
	}

	return reg, nil
}

func (r *registry) build(name string) (Instruction, error) {
	// get factory instruction
	factory, ok := r.ins[name]
	if !ok {
		return nil, fmt.Errorf("turing: registry build: missing instruction: " + name)
	}

	// use builder if available
	builder := factory.Describe().Builder
	if builder != nil {
		return builder(), nil
	}

	// otherwise use reflect
	return reflect.New(reflect.TypeOf(factory).Elem()).Interface().(Instruction), nil
}
