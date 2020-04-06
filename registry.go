package turing

import "fmt"

type registry struct {
	instructions map[string]Instruction
}

func buildRegistry(config Config) (*registry, error) {
	// prepare registry
	registry := &registry{
		instructions: map[string]Instruction{},
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

	return registry, nil
}
