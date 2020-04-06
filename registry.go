package turing

import "fmt"

type registry struct {
	instructions map[string]Instruction
	operators    map[string]*Operator
}

func buildRegistry(config Config) (*registry, error) {
	// prepare registry
	registry := &registry{
		instructions: map[string]Instruction{},
		operators:    map[string]*Operator{},
	}

	// add instructions
	for _, instruction := range config.Instructions {
		// get name
		name := instruction.Describe().Name

		// check existence
		if registry.instructions[name] != nil {
			return nil, fmt.Errorf("duplicate instruction: %s", name)
		}

		// store instruction
		registry.instructions[name] = instruction
	}

	// add operators
	for _, operator := range config.Operators {
		// get name
		name := operator.Name

		// check existence
		if registry.operators[name] != nil {
			return nil, fmt.Errorf("duplicate operator: %s", name)
		}

		// store operator
		registry.operators[name] = operator
	}

	return registry, nil
}
