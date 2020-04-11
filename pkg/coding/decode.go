package coding

import (
	"encoding/binary"
	"errors"
	"sync"

	"github.com/tidwall/cast"
)

// ErrBufferTooShort if the provided buffer is too short.
var ErrBufferTooShort = errors.New("turing: buffer too short")

// Decoder manages data decoding.
type Decoder struct {
	buf []byte
	err error
}

// NewDecoder will return a decoder for the provided byte slice.
func NewDecoder(bytes []byte) *Decoder {
	// borrow decoder
	dec := decoderPool.Get().(*Decoder)
	dec.buf = bytes

	return dec
}

// Bool reads a boolean.
func (d *Decoder) Bool(bol *bool) {
	var num uint64
	d.Uint(&num)
	*bol = num == 1
}

// Uint reads an unsigned integer.
func (d *Decoder) Uint(num *uint64) {
	// skip if errored
	if d.err != nil {
		return
	}

	// read length
	var n int
	*num, n = binary.Uvarint(d.buf)
	if n == 0 {
		d.err = ErrBufferTooShort
		return
	}

	// slice
	d.buf = d.buf[n:]
}

// Int reads a signed integer.
func (d *Decoder) Int(num *int64) {
	// skip if errored
	if d.err != nil {
		return
	}

	// read length
	var n int
	*num, n = binary.Varint(d.buf)
	if n == 0 {
		d.err = ErrBufferTooShort
		return
	}

	// slice
	d.buf = d.buf[n:]
}

// String reads a length prefixed string. If the string is not cloned it may
// change if the decoded byte slice changes.
func (d *Decoder) String(str *string, clone bool) {
	// skip if errored
	if d.err != nil {
		return
	}

	// read length
	length, n := binary.Uvarint(d.buf)
	if n == 0 {
		d.err = ErrBufferTooShort
		return
	}

	// slice
	d.buf = d.buf[n:]

	// check length
	if len(d.buf) < int(length) {
		d.err = ErrBufferTooShort
		return
	}

	// cast or set string
	if clone {
		*str = string(d.buf[:length])
		d.buf = d.buf[length:]
	} else {
		*str = cast.ToString(d.buf[:length])
		d.buf = d.buf[length:]
	}
}

// Bytes reads a length prefixed byte slice. If the byte slice is not cloned it
// may change if the decoded byte slice changes.
func (d *Decoder) Bytes(bytes *[]byte, clone bool) {
	// skip if errored
	if d.err != nil {
		return
	}

	// read length
	length, n := binary.Uvarint(d.buf)
	if n == 0 {
		d.err = ErrBufferTooShort
		return
	}

	// slice
	d.buf = d.buf[n:]

	// check length
	if len(d.buf) < int(length) {
		d.err = ErrBufferTooShort
		return
	}

	// clone or set bytes
	if clone {
		*bytes = make([]byte, length)
		copy(*bytes, d.buf[:length])
		d.buf = d.buf[length:]
	} else {
		*bytes = d.buf[:length]
		d.buf = d.buf[length:]
	}
}

// Tail reads a tail byte slice.
func (d *Decoder) Tail(bytes *[]byte, clone bool) {
	// skip if errored
	if d.err != nil {
		return
	}

	// clone or set bytes
	if clone {
		*bytes = make([]byte, len(d.buf))
		copy(*bytes, d.buf[:len(d.buf)])
		d.buf = d.buf[len(d.buf):]
	} else {
		*bytes = d.buf[:len(d.buf)]
		d.buf = d.buf[len(d.buf):]
	}
}

// Error will return the error.
func (d *Decoder) Error() error {
	return d.err
}

// Release will release the decoder.
func (d *Decoder) Release() {
	// reset decoder
	d.buf = nil
	d.err = nil

	// return decoder
	decoderPool.Put(d)
}

var decoderPool = sync.Pool{
	New: func() interface{} {
		return &Decoder{}
	},
}

// Decode will decode data using the provided decoding function. The function is
// run once to decode the data. It will return whether the buffer was long enough
// to read all data.
func Decode(bytes []byte, fn func(dec *Decoder) error) error {
	// get decoder
	dec := NewDecoder(bytes)
	defer dec.Release()

	// decode
	err := fn(dec)
	if err == nil {
		err = dec.Error()
	}

	return err
}
