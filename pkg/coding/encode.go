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

// Bool writes a boolean.
func (e *Encoder) Bool(truthy bool) {
	if truthy {
		e.Uint(1)
	} else {
		e.Uint(0)
	}
}

// Uint writes an unsigned integer.
func (e *Encoder) Uint(num uint64) {
	// handle length
	if e.buf == nil {
		e.len += binary.PutUvarint(e.b10[:], num)
		return
	}

	// write number
	n := binary.PutUvarint(e.buf, num)
	e.buf = e.buf[n:]
}

// Int writes a signed integer.
func (e *Encoder) Int(num int64) {
	// handle length
	if e.buf == nil {
		e.len += binary.PutVarint(e.b10[:], num)
		return
	}

	// write number
	n := binary.PutVarint(e.buf, num)
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

var encoderPool = sync.Pool{
	New: func() interface{} {
		return &Encoder{}
	},
}

// Encode will encode data using the provided encoding function. The function
// is run once to assess the length of the buffer and once to encode the data.
func Encode(borrow bool, fn func(enc *Encoder) error) ([]byte, *Ref, error) {
	// borrow encoder
	enc := encoderPool.Get().(*Encoder)
	defer func() {
		enc.len = 0
		enc.buf = nil
		encoderPool.Put(enc)
	}()

	// count
	err := fn(enc)
	if err != nil {
		return nil, nil, err
	}

	// get buffer
	var buf []byte
	var ref *Ref
	if borrow {
		buf, ref = Borrow(enc.len)
		buf = buf[:enc.len]
	} else {
		buf = make([]byte, enc.len)
	}

	// set buffer
	enc.buf = buf

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
