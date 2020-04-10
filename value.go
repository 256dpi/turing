package turing

import (
	"fmt"

	"github.com/256dpi/turing/pkg/coding"
)

// Kind represents the kind of value stored at a key.
type Kind byte

const (
	// FullValue is a full value.
	FullValue Kind = 1

	// StackValue is a stack of merge values.
	StackValue Kind = 2
)

// Valid returns whether the kind is valid.
func (k Kind) Valid() bool {
	switch k {
	case FullValue, StackValue:
		return true
	default:
		return false
	}
}

// Operand represents a single merge operand.
type Operand struct {
	Name  string
	Value []byte
}

// Value represents a decoded value
type Value struct {
	// The kind of the value.
	Kind Kind

	// The full value.
	Value []byte

	// The stacked operands.
	Stack []Operand
}

// Encode will encode the value.
func (v *Value) Encode(borrow bool) ([]byte, Ref, error) {
	// check kind
	if !v.Kind.Valid() {
		return nil, NoopRef, fmt.Errorf("turing: encode value: invalid kind: %c", v.Kind)
	}

	// check stack
	if v.Kind == StackValue {
		for _, op := range v.Stack {
			if op.Name == "" {
				return nil, NoopRef, fmt.Errorf("turing: encode value: missing operand name")
			}
		}
	}

	return coding.Encode(borrow, func(enc *coding.Encoder) error {
		// write version
		enc.Uint(1)

		// write kind
		enc.Uint(uint64(v.Kind))

		// write full value
		if v.Kind == FullValue {
			enc.Tail(v.Value)
			return nil
		}

		// otherwise write stack value

		// write length
		enc.Uint(uint64(len(v.Stack)))

		// write operands
		for _, op := range v.Stack {
			enc.String(op.Name)
			enc.Bytes(op.Value)
		}

		return nil
	})
}

// Decode will decode the value.
func (v *Value) Decode(bytes []byte, clone bool) error {
	return coding.Decode(bytes, func(dec *coding.Decoder) error {
		// decode version
		var version uint64
		dec.Uint(&version)
		if version != 1 {
			return fmt.Errorf("turing: decode value: invalid version")
		}

		// read kind
		var kind uint64
		dec.Uint(&kind)
		v.Kind = Kind(kind)
		if !v.Kind.Valid() {
			return fmt.Errorf("turing: decode value: invalid kind: %d", v.Kind)
		}

		// decode full value
		if v.Kind == FullValue {
			dec.Tail(&v.Value, clone)
			return nil
		}

		// otherwise decode stack

		// get operands
		var num uint64
		dec.Uint(&num)

		// prepare stack
		v.Stack = make([]Operand, int(num))

		// read operands
		for i := range v.Stack {
			dec.String(&v.Stack[i].Name, clone)
			dec.Bytes(&v.Stack[i].Value, clone)
		}

		return nil
	})
}

// StackValues will stack the provided values.
func StackValues(values []Value) (Value, error) {
	// validated and count values
	var total int
	for _, value := range values {
		if value.Kind != StackValue {
			return Value{}, fmt.Errorf("turing: stack values: unexpected value: %d", value.Kind)
		}

		// increment
		total += len(value.Stack)
	}

	// collect stack values
	stack := make([]Operand, 0, total)
	for _, value := range values {
		stack = append(stack, value.Stack...)
	}

	// create value
	value := Value{
		Kind:  StackValue,
		Stack: stack,
	}

	return value, nil
}

// MergeValues will merge the provided values.
func MergeValues(values []Value, registry *registry) (Value, Ref, error) {
	// get first value
	value := values[0].Value
	if values[0].Kind != FullValue {
		return Value{}, nil, fmt.Errorf("turing: merge values: expected full value, got: %d", values[0].Kind)
	}

	// slice
	values = values[1:]

	// validate and count values
	var total int
	for _, value := range values {
		if value.Kind != StackValue {
			return Value{}, nil, fmt.Errorf("turing: merge values: expected stack value, got: %d", value.Kind)
		}

		// increment
		total += len(value.Stack)
	}

	// TODO: Allocating those slices is expensive.

	// validate and collect operands
	names := make([]string, 0, total)
	operands := make([][]byte, 0, total)
	for _, value := range values {
		for _, operand := range value.Stack {
			names = append(names, operand.Name)
			operands = append(operands, operand.Value)
		}
	}

	// merge all operands
	var start int
	var name string
	var err error
	var ref Ref
	for i := range names {
		// continue if first or same name
		if i == 0 {
			name = names[i]
			continue
		} else if name == names[i] {
			continue
		}

		// operator changed, merge values

		// lookup operator
		operator, ok := registry.ops[name]
		if !ok {
			return Value{}, nil, fmt.Errorf("turing: merge values: unknown operator: %q", name)
		}

		// count execution
		operator.counter.Inc()

		// merge value with operands
		var newRef Ref
		value, newRef, err = operator.Apply(value, operands[start:i])
		if err != nil {
			return Value{}, nil, err
		}

		// release old ref
		if ref != nil {
			ref.Release()
		}

		// set new ref
		ref = newRef

		// set new name
		name = names[i]
		start = i
	}

	// lookup operator
	operator, ok := registry.ops[name]
	if !ok {
		return Value{}, nil, fmt.Errorf("turing: merge values: unknown operator: %q", name)
	}

	// count execution
	operator.counter.Inc()

	// merge value with operands
	var newRef Ref
	value, newRef, err = operator.Apply(value, operands[start:])
	if err != nil {
		return Value{}, nil, err
	}

	// release old ref
	if ref != nil {
		ref.Release()
	}

	// prepare result
	result := Value{
		Kind:  FullValue,
		Value: value,
	}

	return result, newRef, nil
}

// ComputeValue will compute the final value from a stack value using the zero
// value from the first operand operator.
func ComputeValue(value Value, registry *registry) (Value, Ref, error) {
	// check kind
	if value.Kind != StackValue {
		return Value{}, nil, fmt.Errorf("turing: compute value: expected stack value, got: %d", value.Kind)
	}

	// get first operator
	operator, ok := registry.ops[value.Stack[0].Name]
	if !ok {
		return Value{}, nil, fmt.Errorf("turing: compute value: missing operator: %s", value.Stack[0].Name)
	}

	// prepare zero value
	zero := Value{
		Kind:  FullValue,
		Value: operator.Zero,
	}

	// merge values
	value, ref, err := MergeValues([]Value{zero, value}, registry)
	if err != nil {
		return Value{}, NoopRef, err
	}

	return value, ref, nil
}
