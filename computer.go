package turing

import (
	"fmt"
	"sync"

	"github.com/256dpi/turing/tape"
)

type computer struct {
	registry *registry
	names    [1000]string
	values   [1000][]byte
	operands [1000]tape.Operand
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

func (c *computer) combine(cells []tape.Cell) (tape.Cell, Ref, error) {
	// validate and collect operands
	names := c.names[:0]
	values := c.values[:0]
	for _, cell := range cells {
		// check cell
		if cell.Type != tape.StackCell {
			return tape.Cell{}, nil, fmt.Errorf("turing: computer combine: expected stack cell, got: %d", cell.Type)
		}

		// decode stack
		err := tape.WalkStack(cell.Value, func(i int, op tape.Operand) (bool, error) {
			names = append(names, op.Name)
			values = append(values, op.Value)
			return true, nil
		})
		if err != nil {
			return tape.Cell{}, nil, err
		}
	}

	// prepare new stack
	stack := tape.Stack{
		Operands: c.operands[:0],
	}

	// combine or append operands
	refs := c.refs[:0]
	err := c.pipeline(names, values, func(operator *Operator, ops [][]byte) error {
		// append if combine is missing
		if operator.Combine == nil {
			for _, op := range ops {
				stack.Operands = append(stack.Operands, tape.Operand{
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

		// combine operands
		result, ref, err := operator.Combine(ops)
		if err != nil {
			return err
		}

		// append operand
		stack.Operands = append(stack.Operands, tape.Operand{
			Name:  operator.Name,
			Value: result,
		})

		// append ref if existing
		if ref != nil {
			refs = append(refs, ref)
		}

		return nil
	})
	if err != nil {
		return tape.Cell{}, nil, err
	}

	// encode stack
	stackBytes, stackRef, err := stack.Encode(true)
	if err != nil {
		return tape.Cell{}, nil, err
	}

	// release refs
	for _, ref := range refs {
		ref.Release()
	}

	// prepare result
	result := tape.Cell{
		Type:  tape.StackCell,
		Value: stackBytes,
	}

	return result, stackRef, nil
}

func (c *computer) apply(cells []tape.Cell) (tape.Cell, Ref, error) {
	// check cells
	if len(cells) < 2 {
		return tape.Cell{}, nil, fmt.Errorf("turing: computer apply: need at least two cells")
	}

	// check first cell
	if cells[0].Type != tape.RawCell {
		return tape.Cell{}, nil, fmt.Errorf("turing: computer apply: expected raw cell, got: %d", cells[0].Type)
	}

	// get base
	base := cells[0].Value

	// validate and collect operands
	names := c.names[:0]
	values := c.values[:0]
	for _, cell := range cells[1:] {
		// check cell
		if cell.Type != tape.StackCell {
			return tape.Cell{}, nil, fmt.Errorf("turing: computer apply: expected stack cell, got: %d", cell.Type)
		}

		// decode stack
		err := tape.WalkStack(cell.Value, func(i int, op tape.Operand) (bool, error) {
			names = append(names, op.Name)
			values = append(values, op.Value)
			return true, nil
		})
		if err != nil {
			return tape.Cell{}, nil, err
		}
	}

	// merge all operands with base
	var ref Ref = noopRef
	err := c.pipeline(names, values, func(op *Operator, ops [][]byte) error {
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
		if newRef != nil {
			ref = newRef
		}

		return nil
	})
	if err != nil {
		return tape.Cell{}, nil, err
	}

	// prepare result
	result := tape.Cell{
		Type:  tape.RawCell,
		Value: base,
	}

	return result, ref, nil
}

func (c *computer) resolve(cell tape.Cell) (tape.Cell, Ref, error) {
	// check type
	if cell.Type != tape.StackCell {
		return tape.Cell{}, nil, fmt.Errorf("turing: computer resolve: expected stack cell, got: %d", cell.Type)
	}

	// get first operator
	var operator *Operator
	err := tape.WalkStack(cell.Value, func(i int, op tape.Operand) (bool, error) {
		// get first operator
		operator = c.registry.ops[op.Name]
		if operator == nil {
			return false, fmt.Errorf("turing: computer resolve: missing operator: %s", op.Name)
		}

		return false, nil
	})
	if err != nil {
		return tape.Cell{}, nil, err
	}

	// prepare cells
	cells := [2]tape.Cell{
		{
			Type:  tape.RawCell,
			Value: operator.Zero,
		},
		cell,
	}

	// apply cells
	cell, ref, err := c.apply(cells[:])
	if err != nil {
		return tape.Cell{}, nil, err
	}

	return cell, ref, nil
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
