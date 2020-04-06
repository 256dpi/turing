package turing

import (
	"bytes"
	"fmt"

	"github.com/vmihailenco/msgpack/v4"
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
	Name  string `msgpack:"n"`
	Value []byte `msgpack:"v"`
}

// Value represents a decoded value
type Value struct {
	// The kind of the value.
	Kind Kind `msgpack:"-"`

	// The full value.
	Value []byte `msgpack:"-"`

	// The stacked operands.
	Stack []Operand `msgpack:"s"`
}

// DecodeValue will decode a value.
func DecodeValue(bytes []byte) (Value, error) {
	// check length
	if len(bytes) == 0 {
		return Value{}, fmt.Errorf("decode value: zero length")
	}

	// get kind
	kind := Kind(bytes[0])
	if !kind.Valid() {
		return Value{}, fmt.Errorf("decode value: invalid kind: %c", kind)
	}

	// decode full value
	if kind == FullValue {
		return Value{
			Kind:  kind,
			Value: bytes[1:],
		}, nil
	}

	// decode value
	var value Value
	err := msgpack.Unmarshal(bytes[1:], &value)
	if err != nil {
		return Value{}, err
	}

	// set kind
	value.Kind = kind

	return value, nil
}

// EncodeValue will encode a value.
func EncodeValue(value Value) ([]byte, error) {
	// check Kind
	if !value.Kind.Valid() {
		return nil, fmt.Errorf("encode value: invalid kind: %c", value.Kind)
	}

	// encode full value
	if value.Kind == FullValue {
		return append([]byte{uint8(FullValue)}, value.Value...), nil
	}

	// check stack
	if value.Kind == StackValue {
		for _, op := range value.Stack {
			if op.Name == "" {
				return nil, fmt.Errorf("missing operand name")
			}
		}
	}

	// prepare buffer
	var buf bytes.Buffer

	// write kind
	err := buf.WriteByte(byte(value.Kind))
	if err != nil {
		return nil, err
	}

	// encode value
	err = msgpack.NewEncoder(&buf).Encode(value)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
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
