package turing

import (
	"fmt"
	"sync"
)

type computer struct {
	registry *registry
	strings  [1000]string
	bytes    [1000][]byte
	operands [1000]Operand
	refs     [1000]Ref
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

func (c *computer) combine(values []Value) (Value, Ref, error) {
	// ensure recycle
	defer c.recycle()

	// validate and collect operands
	opNames := c.strings[:0]
	opValues := c.bytes[:0]
	for _, value := range values {
		// check value
		if value.Kind != StackValue {
			return Value{}, nil, fmt.Errorf("turing: computer combine: expected stack value as operand, got: %d", value.Kind)
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

	// combine or append operands
	refs := c.refs[:0]
	err := c.pipeline(opNames, opValues, func(operator *Operator, ops [][]byte) error {
		// append if combine is missing
		if operator.Combine == nil {
			for _, op := range ops {
				stack.Operands = append(stack.Operands, Operand{
					Name:  operator.Name,
					Value: op,
				})
			}

			return nil
		}

		// combine operands

		// count execution if possible
		if operator.counter != nil {
			operator.counter.Inc()
		}

		// merge base with operands
		value, ref, err := operator.Combine(ops)
		if err != nil {
			return err
		}

		// append operand
		stack.Operands = append(stack.Operands, Operand{
			Name:  operator.Name,
			Value: value,
		})

		// append ref
		refs = append(refs, ref)

		return nil
	})
	if err != nil {
		return Value{}, nil, err
	}

	// encode stack
	sv, svr, err := stack.Encode(true)
	if err != nil {
		return Value{}, nil, err
	}

	// release refs
	for _, ref := range refs {
		ref.Release()
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
	opNames := c.strings[:0]
	opValues := c.bytes[:0]
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

	// merge all operands with base
	var ref Ref
	err := c.pipeline(opNames, opValues, func(op *Operator, ops [][]byte) error {
		// count execution if possible
		if op.counter != nil {
			op.counter.Inc()
		}

		// merge base with operands
		var newRef Ref
		var err error
		base, newRef, err = op.Apply(base, ops)
		if err != nil {
			return err
		}

		// release old ref
		if ref != nil {
			ref.Release()
		}

		// set new ref
		ref = newRef

		return nil
	})
	if err != nil {
		return Value{}, nil, err
	}

	// prepare result
	result := Value{
		Kind:  FullValue,
		Value: base,
	}

	return result, ref, nil
}

func (c *computer) resolve(value Value) (Value, Ref, error) {
	// check kind
	if value.Kind != StackValue {
		return Value{}, nil, fmt.Errorf("turing: computer resolve: expected stack value, got: %d", value.Kind)
	}

	// decode stack
	var stack Stack
	err := stack.Decode(value.Value, false)
	if err != nil {
		return Value{}, nil, err
	}

	// get first operator
	operator, ok := c.registry.ops[stack.Operands[0].Name]
	if !ok {
		return Value{}, nil, fmt.Errorf("turing: computer resolve: missing operator: %s", stack.Operands[0].Name)
	}

	// prepare zero value
	zero := Value{
		Kind:  FullValue,
		Value: operator.Zero,
	}

	// prepare list
	list := [2]Value{zero, value}

	// merge values
	value, ref, err := c.eval(list[:])
	if err != nil {
		return Value{}, NoopRef, err
	}

	return value, ref, nil
}

func (c *computer) pipeline(names []string, values [][]byte, fn func(op *Operator, ops [][]byte) error) error {
	// process all operands
	var start int
	var name = names[0]
	for i := 1; ; i++ {
		// continue if not finished and same name
		if i < len(names) && name == names[i] {
			continue
		}

		// operator changed or finished, yield operands

		// lookup operator
		operator, ok := c.registry.ops[name]
		if !ok {
			return fmt.Errorf("turing: computer pipeline: unknown operator: %q", name)
		}

		// yield operator and values
		err := fn(operator, values[start:i])
		if err != nil {
			return err
		}

		// break if last
		if i == len(names) {
			break
		}

		// otherwise set next
		name = names[i]
		start = i
	}

	return nil
}

func (c *computer) recycle() {
	// unset registry
	c.registry = nil

	// return
	computerPool.Put(c)
}
