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

// EncodeValue will encode a value.
func EncodeValue(value Value) ([]byte, error) {
	// check Kind
	if !value.Kind.Valid() {
		return nil, fmt.Errorf("turing: encode value: invalid kind: %c", value.Kind)
	}

	// check stack
	if value.Kind == StackValue {
		for _, op := range value.Stack {
			if op.Name == "" {
				return nil, fmt.Errorf("turing: encode value: missing operand name")
			}
		}
	}

	// encode value
	return coding.Encode(func(enc *coding.Encoder) error {
		// write version
		enc.Uint(1)

		// write kind
		enc.Uint(uint64(value.Kind))

		// write full value
		if value.Kind == FullValue {
			enc.Tail(value.Value)
			return nil
		}

		// otherwise write stack value

		// write length
		enc.Uint(uint64(len(value.Stack)))

		// write operands
		for _, op := range value.Stack {
			enc.String(op.Name)
			enc.Bytes(op.Value)
		}

		return nil
	})
}

// DecodeValue will decode a value.
func DecodeValue(bytes []byte) (Value, error) {
	// decode value
	var value Value
	err := coding.Decode(bytes, func(dec *coding.Decoder) error {
		// decode version
		var version uint64
		dec.Uint(&version)
		if version != 1 {
			return fmt.Errorf("turing: decode value: invalid version")
		}

		// read kind
		var kind uint64
		dec.Uint(&kind)
		value.Kind = Kind(kind)
		if !value.Kind.Valid() {
			return fmt.Errorf("turing: decode value: invalid kind: %d", value.Kind)
		}

		// decode full value
		if value.Kind == FullValue {
			dec.Tail(&value.Value, false)
			return nil
		}

		// otherwise decode stack

		// get operands
		var num uint64
		dec.Uint(&num)

		// prepare stack
		value.Stack = make([]Operand, int(num))

		// read operands
		for i := range value.Stack {
			dec.String(&value.Stack[i].Name, false)
			dec.Bytes(&value.Stack[i].Value, false)
		}

		return nil
	})
	if err != nil {
		return Value{}, fmt.Errorf("turing: decode value: %w", err)
	}

	return value, nil
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
func MergeValues(values []Value, registry *registry) (Value, error) {
	// get first value
	value := values[0].Value
	if values[0].Kind != FullValue {
		return Value{}, fmt.Errorf("turing: merge values: unexpected value: %d", values[0].Kind)
	}

	// slice
	values = values[1:]

	// validated and count values
	var total int
	for _, value := range values {
		if value.Kind != StackValue {
			return Value{}, fmt.Errorf("turing: merge values: unexpected value: %d", value.Kind)
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
		operator, ok := registry.operators[name]
		if !ok {
			return Value{}, fmt.Errorf("turing: merge values: unknown operator: %q", name)
		}

		// merge value with operands
		value, err = operator.Apply(value, operands[start:i])
		if err != nil {
			return Value{}, err
		}

		// set new name
		name = names[i]
		start = i
	}

	// lookup operator
	operator, ok := registry.operators[name]
	if !ok {
		return Value{}, fmt.Errorf("turing: merge values: unknown operator: %q", name)
	}

	// merge value with operands
	value, err = operator.Apply(value, operands[start:])
	if err != nil {
		return Value{}, err
	}

	// prepare result
	result := Value{
		Kind:  FullValue,
		Value: value,
	}

	return result, nil
}

// ComputeValue will compute the final value. A full value is immediately
// returned while a stacked value is merged with the first operators zero value.
func ComputeValue(value Value, registry *registry) (Value, error) {
	// directly return full value
	if value.Kind == FullValue {
		return value, nil
	}

	// value is a stack value

	// get first operator
	operator, ok := registry.operators[value.Stack[0].Name]
	if !ok {
		return Value{}, fmt.Errorf("turing: compute value: missing operator: %s", value.Stack[0].Name)
	}

	// prepare zero value
	zero := Value{
		Kind:  FullValue,
		Value: operator.Zero,
	}

	// merge values
	value, err := MergeValues([]Value{zero, value}, registry)
	if err != nil {
		return Value{}, err
	}

	return value, nil
}
