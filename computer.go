package turing

import (
	"fmt"
	"sync"
)

type computer struct {
	registry *registry
	opNames  [1000]string
	opValues [1000][]byte
	operands [1000]Operand
}

var computerPool = sync.Pool{
	New: func() interface{} {
		return &computer{}
	},
}

func newComputer(registry *registry) *computer {
	// borrow computer
	computer := computerPool.Get().(*computer)
	computer.registry = registry

	return computer
}

func (c *computer) stack(values []Value) (Value, Ref, error) {
	// ensure recycle
	defer c.recycle()

	// validate and collect operands
	opNames := c.opNames[:0]
	opValues := c.opValues[:0]
	for _, value := range values {
		// check value
		if value.Kind != StackValue {
			return Value{}, nil, fmt.Errorf("turing: computer stack: expected stack value as operand, got: %d", value.Kind)
		}

		// decode stack
		err := WalkStack(value.Value, func(op Operand) error {
			opNames = append(opNames, op.Name)
			opValues = append(opValues, op.Value)
			return nil
		})
		if err != nil {
			return Value{}, nil, err
		}
	}

	// prepare new stack
	stack := Stack{
		Operands: c.operands[:0],
	}

	// collect stack values
	for i := range opNames {
		stack.Operands = append(stack.Operands, Operand{
			Name:  opNames[i],
			Value: opValues[i],
		})
	}

	// encode stack
	sv, svr, err := stack.Encode(true)
	if err != nil {
		return Value{}, nil, err
	}

	// create value
	value := Value{
		Kind:  StackValue,
		Value: sv,
	}

	return value, svr, nil
}

func (c *computer) eval(values []Value) (Value, Ref, error) {
	// ensure recycle
	defer c.recycle()

	// check values
	if len(values) < 2 {
		return Value{}, nil, fmt.Errorf("turing: computer eval: need at least two values")
	}

	// check first value
	if values[0].Kind != FullValue {
		return Value{}, nil, fmt.Errorf("turing: computer eval: expected full value as base, got: %d", values[0].Kind)
	}

	// get base value
	base := values[0].Value

	// validate and collect operands
	opNames := c.opNames[:0]
	opValues := c.opValues[:0]
	for _, value := range values[1:] {
		// check value
		if value.Kind != StackValue {
			return Value{}, nil, fmt.Errorf("turing: computer eval: expected stack value as operand, got: %d", value.Kind)
		}

		// decode stack
		err := WalkStack(value.Value, func(op Operand) error {
			opNames = append(opNames, op.Name)
			opValues = append(opValues, op.Value)
			return nil
		})
		if err != nil {
			return Value{}, nil, err
		}
	}

	// merge all operands
	var start int
	var name = opNames[0]
	var err error
	var ref Ref
	for i := 1; ; i++ {
		// continue if not finished and same name
		if i < len(opNames) && name == opNames[i] {
			continue
		}

		// operator changed or finished, merge values

		// lookup operator
		operator, ok := c.registry.ops[name]
		if !ok {
			return Value{}, nil, fmt.Errorf("turing: merge values: unknown operator: %q", name)
		}

		// count execution if possible
		if operator.counter != nil {
			operator.counter.Inc()
		}

		// merge base with operands
		var newRef Ref
		base, newRef, err = operator.Apply(base, opValues[start:i])
		if err != nil {
			return Value{}, nil, err
		}

		// release old ref
		if ref != nil {
			ref.Release()
		}

		// set new ref
		ref = newRef

		// break if last
		if i == len(opNames) {
			break
		}

		// otherwise set next
		name = opNames[i]
		start = i
	}

	// prepare result
	result := Value{
		Kind:  FullValue,
		Value: base,
	}

	return result, ref, nil
}

func (c *computer) recycle() {
	// unset registry
	c.registry = nil

	// return
	computerPool.Put(c)
}
