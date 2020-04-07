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

// DecodeValue will decode a value.
func DecodeValue(bytes []byte) (Value, error) {
	// decode value
	var value Value
	ok := coding.Decode(bytes, func(dec *coding.Decoder) {
		// read kind
		var kind uint64
		dec.Uint(&kind)
		value.Kind = Kind(kind)

		// decode full value
		if value.Kind == FullValue {
			dec.Tail(&value.Value)
			return
		}

		// otherwise decode stack

		// get operands
		var num uint64
		dec.Uint(&num)

		// prepare stack
		value.Stack = make([]Operand, int(num))

		// read operands
		for i := range value.Stack {
			dec.String(&value.Stack[i].Name)
			dec.Bytes(&value.Stack[i].Value)
		}
	})
	if !ok {
		return Value{}, fmt.Errorf("decode value: invalid buffer")
	}

	// get kind
	if !value.Kind.Valid() {
		return Value{}, fmt.Errorf("decode value: invalid kind: %d", value.Kind)
	}

	return value, nil
}

// EncodeValue will encode a value.
func EncodeValue(value Value) ([]byte, error) {
	// check Kind
	if !value.Kind.Valid() {
		return nil, fmt.Errorf("encode value: invalid kind: %c", value.Kind)
	}

	// check stack
	if value.Kind == StackValue {
		for _, op := range value.Stack {
			if op.Name == "" {
				return nil, fmt.Errorf("missing operand name")
			}
		}
	}

	// encode value
	buf := coding.Encode(func(enc *coding.Encoder) {
		// write kind
		enc.Uint(uint64(value.Kind))

		// write full value
		if value.Kind == FullValue {
			enc.Tail(value.Value)
			return
		}

		// otherwise write stack

		// write length
		enc.Uint(uint64(len(value.Stack)))

		// write operands
		for _, op := range value.Stack {
			enc.String(op.Name)
			enc.Bytes(op.Value)
		}
	})

	return buf, nil
}

// StackValues will stack the provided values.
func StackValues(values []Value) (Value, error) {
	// stack values
	var stack []Operand
	for _, value := range values {
		switch value.Kind {
		case StackValue:
			stack = append(stack, value.Stack...)
		default:
			return Value{}, fmt.Errorf("unexpected value")
		}
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

	// collect operands
	var names []string
	var operands [][]byte
	for _, value := range values[1:] {
		switch value.Kind {
		case StackValue:
			for _, operand := range value.Stack {
				names = append(names, operand.Name)
				operands = append(operands, operand.Value)
			}
		default:
			return Value{}, fmt.Errorf("unexpected value")
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
			return Value{}, fmt.Errorf("merge values: unknown operator: %q", name)
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
		return Value{}, fmt.Errorf("unknown operator: %q", name)
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
	// skip if full value
	if value.Kind == FullValue {
		return value, nil
	}

	// get first operator
	operator, ok := registry.operators[value.Stack[0].Name]
	if !ok {
		return Value{}, fmt.Errorf("compute value: missing operator: %s", value.Stack[0].Name)
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
