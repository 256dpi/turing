package turing

import "fmt"

type registry struct {
	instructions map[string]Instruction
	operators    map[string]*Operator
}

func buildRegistry(config Config) (*registry, error) {
	// prepare registry
	reg := &registry{
		instructions: map[string]Instruction{},
		operators:    map[string]*Operator{},
	}

	// add instructions
	for _, ins := range config.Instructions {
		// get description
		desc := ins.Describe()

		// check existence
		if reg.instructions[desc.Name] != nil {
			return nil, fmt.Errorf("turing: duplicate instruction: %s", desc.Name)
		}

		// store instruction
		reg.instructions[desc.Name] = ins

		// add operators
		for _, op := range desc.Operators {
			// get name
			name := op.Name

			// check existing operator
			eop := reg.operators[name]
			if eop != nil && eop != op {
				return nil, fmt.Errorf("turing: different operator for same name: %s", name)
			}

			// store operator
			reg.operators[name] = op
		}
	}

	return reg, nil
}
