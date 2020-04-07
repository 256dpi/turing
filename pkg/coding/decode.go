package coding

import (
	"encoding/binary"
	"sync"
)

// Decoder manages data decoding.
type Decoder struct {
	buf []byte
	err bool
}

// Int reads a signed integer.
func (e *Decoder) Int(num *int64) {
	// skip if errored
	if e.err {
		return
	}

	// read length
	var n int
	*num, n = binary.Varint(e.buf)
	if n == 0 {
		e.err = true
		return
	}

	// slice
	e.buf = e.buf[n:]
}

// Uint reads an unsigned integer.
func (e *Decoder) Uint(num *uint64) {
	// skip if errored
	if e.err {
		return
	}

	// read length
	var n int
	*num, n = binary.Uvarint(e.buf)
	if n == 0 {
		e.err = true
		return
	}

	// slice
	e.buf = e.buf[n:]
}

// String reads a length prefixed string.
func (e *Decoder) String(str *string) {
	// skip if errored
	if e.err {
		return
	}

	// read length
	length, n := binary.Uvarint(e.buf)
	if n == 0 {
		e.err = true
		return
	}

	// slice
	e.buf = e.buf[n:]

	// check length
	if len(e.buf) < int(length) {
		e.err = true
		return
	}

	// set bytes
	*str = string(e.buf[:length])
	e.buf = e.buf[length:]
}

// Bytes reads a length prefixed byte slice.
func (e *Decoder) Bytes(bytes *[]byte) {
	// skip if errored
	if e.err {
		return
	}

	// read length
	length, n := binary.Uvarint(e.buf)
	if n == 0 {
		e.err = true
		return
	}

	// slice
	e.buf = e.buf[n:]

	// check length
	if len(e.buf) < int(length) {
		e.err = true
		return
	}

	// set bytes
	*bytes = e.buf[:length]
	e.buf = e.buf[length:]
}

// Tail reads a tail byte slice.
func (e *Decoder) Tail(bytes *[]byte) {
	// skip if errored
	if e.err {
		return
	}

	// set bytes
	*bytes = e.buf[:len(e.buf)]
	e.buf = e.buf[len(e.buf):]
}

var decoderPool = sync.Pool{
	New: func() interface{} {
		return &Decoder{}
	},
}

// Decode will decode data using the provided decoding function. The function is
// run once to decode the data. It will return whether the buffer was long enough
// to read all data.
func Decode(buf []byte, fn func(dec *Decoder)) bool {
	// borrow decoder
	dec := decoderPool.Get().(*Decoder)
	dec.buf = buf

	// decode
	fn(dec)
	ok := !dec.err

	// return decoder
	dec.buf = nil
	dec.err = false
	decoderPool.Put(dec)

	return ok
}
