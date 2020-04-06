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
		dsc := ins.Describe()

		// check existence
		if reg.instructions[dsc.Name] != nil {
			return nil, fmt.Errorf("duplicate instruction: %s", dsc.Name)
		}

		// store instruction
		reg.instructions[dsc.Name] = ins

		// add operators
		for _, op := range dsc.Operators {
			// get name
			name := op.Name

			// check existing operator
			eop := reg.operators[name]
			if eop != nil && eop != op {
				return nil, fmt.Errorf("different operator for same name: %s", name)
			}

			// store operator
			reg.operators[name] = op
		}
	}

	return reg, nil
}
