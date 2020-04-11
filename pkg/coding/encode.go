package coding

import (
	"encoding/binary"
	"sync"
)

// Encoder manages data encoding.
type Encoder struct {
	b10 [10]byte
	len int
	buf []byte
}

// NewEncoder will return an encoder.
func NewEncoder() *Encoder {
	return encoderPool.Get().(*Encoder)
}

// Length will return the accumulated length.
func (e *Encoder) Length() int {
	return e.len
}

// Reset will reset the encoder and set the provided byte slice.
func (e *Encoder) Reset(buf []byte) {
	e.len = 0
	e.buf = buf
}

// Bool writes a boolean.
func (e *Encoder) Bool(yes bool) {
	if yes {
		e.Uint8(1)
	} else {
		e.Uint8(0)
	}
}

// Int8 writes a one byte integer.
func (e *Encoder) Int8(num int8) {
	e.Int(int64(num), 1)
}

// Int16 writes a two byte integer.
func (e *Encoder) Int16(num int16) {
	e.Int(int64(num), 2)
}

// Int32 writes a four byte integer.
func (e *Encoder) Int32(num int32) {
	e.Int(int64(num), 4)
}

// Int64 writes a eight byte integer.
func (e *Encoder) Int64(num int64) {
	e.Int(num, 8)
}

// Int writes a one, two, four or eight byte integer.
func (e *Encoder) Int(n int64, size int) {
	// convert
	un := uint64(n) << 1
	if n < 0 {
		un = ^un
	}

	// handle length
	if e.buf == nil {
		e.len += size
		return
	}

	// write number
	switch size {
	case 1:
		e.buf[0] = uint8(un)
	case 2:
		binary.BigEndian.PutUint16(e.buf, uint16(un))
	case 4:
		binary.BigEndian.PutUint32(e.buf, uint32(un))
	case 8:
		binary.BigEndian.PutUint64(e.buf, un)
	}

	// slice
	e.buf = e.buf[size:]
}

// Uint8 writes a one byte unsigned integer.
func (e *Encoder) Uint8(num uint8) {
	e.Uint(uint64(num), 1)
}

// Uint16 writes a two byte unsigned integer.
func (e *Encoder) Uint16(num uint16) {
	e.Uint(uint64(num), 2)
}

// Uint32 writes a four byte unsigned integer.
func (e *Encoder) Uint32(num uint32) {
	e.Uint(uint64(num), 4)
}

// Uint64 writes a eight byte unsigned integer.
func (e *Encoder) Uint64(num uint64) {
	e.Uint(num, 8)
}

// Uint writes a one, two, four or eight byte unsigned integer.
func (e *Encoder) Uint(num uint64, size int) {
	// handle length
	if e.buf == nil {
		e.len += size
		return
	}

	// write number
	switch size {
	case 1:
		e.buf[0] = uint8(num)
	case 2:
		binary.BigEndian.PutUint16(e.buf, uint16(num))
	case 4:
		binary.BigEndian.PutUint32(e.buf, uint32(num))
	case 8:
		binary.BigEndian.PutUint64(e.buf, num)
	}

	// slice
	e.buf = e.buf[size:]
}

// VarInt writes a variable signed integer.
func (e *Encoder) VarInt(num int64) {
	// handle length
	if e.buf == nil {
		e.len += binary.PutVarint(e.b10[:], num)
		return
	}

	// write number
	n := binary.PutVarint(e.buf, num)
	e.buf = e.buf[n:]
}

// VarUint writes a variable unsigned integer.
func (e *Encoder) VarUint(num uint64) {
	// handle length
	if e.buf == nil {
		e.len += binary.PutUvarint(e.b10[:], num)
		return
	}

	// write number
	n := binary.PutUvarint(e.buf, num)
	e.buf = e.buf[n:]
}

// String writes a length prefixed string.
func (e *Encoder) String(str string) {
	// handle length
	if e.buf == nil {
		e.len += binary.PutUvarint(e.b10[:], uint64(len(str)))
		e.len += len(str)
		return
	}

	// write length
	n := binary.PutUvarint(e.buf, uint64(len(str)))
	e.buf = e.buf[n:]

	// write string
	copy(e.buf, str)
	e.buf = e.buf[len(str):]
}

// Bytes writes a length prefixed byte slice.
func (e *Encoder) Bytes(buf []byte) {
	// handle length
	if e.buf == nil {
		e.len += binary.PutUvarint(e.b10[:], uint64(len(buf)))
		e.len += len(buf)
		return
	}

	// write length
	n := binary.PutUvarint(e.buf, uint64(len(buf)))
	e.buf = e.buf[n:]

	// write bytes
	copy(e.buf, buf)
	e.buf = e.buf[len(buf):]
}

// Tail writes a tail byte slice.
func (e *Encoder) Tail(buf []byte) {
	// handle length
	if e.buf == nil {
		e.len += len(buf)
		return
	}

	// write bytes
	copy(e.buf, buf)
	e.buf = e.buf[len(buf):]
}

// Release will release the encoder.
func (e *Encoder) Release() {
	// reset encoder
	e.len = 0
	e.buf = nil

	// return encoder
	encoderPool.Put(e)
}

var encoderPool = sync.Pool{
	New: func() interface{} {
		return &Encoder{}
	},
}

// Encode will encode data using the provided encoding function. The function
// is run once to assess the length of the buffer and once to encode the data.
func Encode(borrow bool, fn func(enc *Encoder) error) ([]byte, *Ref, error) {
	// borrow encoder
	enc := NewEncoder()
	defer enc.Release()

	// count
	err := fn(enc)
	if err != nil {
		return nil, nil, err
	}

	// get length
	length := enc.Length()

	// get buffer
	var buf []byte
	var ref *Ref
	if borrow {
		buf, ref = Borrow(length)
		buf = buf[:enc.len]
	} else {
		buf = make([]byte, length)
	}

	// reset encoder
	enc.Reset(buf)

	// encode
	err = fn(enc)
	if err != nil {
		if ref != nil {
			ref.Release()
		}

		return nil, nil, err
	}

	return buf, ref, nil
}
