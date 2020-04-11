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
	var num uint8
	d.Uint8(&num)
	*bol = num == 1
}

// Int8 reads a one byte integer.
func (d *Decoder) Int8(num *int8) {
	*num = int8(d.int(1))
}

// Int16 reads a two byte integer.
func (d *Decoder) Int16(num *int16) {
	*num = int16(d.int(2))
}

// Int32 reads a four byte integer.
func (d *Decoder) Int32(num *int32) {
	*num = int32(d.int(4))
}

// Int64 reads a eight byte integer.
func (d *Decoder) Int64(num *int64) {
	*num = d.int(8)
}

func (d *Decoder) int(size int) int64 {
	// skip if errored
	if d.err != nil {
		return 0
	}

	// check length
	if len(d.buf) < size {
		d.err = ErrBufferTooShort
		return 0
	}

	// read
	var num uint64
	switch size {
	case 1:
		num = uint64(d.buf[0])
	case 2:
		num = uint64(binary.BigEndian.Uint16(d.buf))
	case 4:
		num = uint64(binary.BigEndian.Uint32(d.buf))
	case 8:
		num = binary.BigEndian.Uint64(d.buf)
	}

	// slice
	d.buf = d.buf[size:]

	// convert
	n := int64(num >> 1)
	if num&1 != 0 {
		n = ^n
	}

	return n
}

// Uint8 reads a one byte unsigned integer.
func (d *Decoder) Uint8(num *uint8) {
	*num = uint8(d.uint(1))
}

// Uint16 reads a two byte unsigned integer.
func (d *Decoder) Uint16(num *uint16) {
	*num = uint16(d.uint(2))
}

// Uint32 reads a four byte unsigned integer.
func (d *Decoder) Uint32(num *uint32) {
	*num = uint32(d.uint(4))
}

// Uint64 reads a eight byte unsigned integer.
func (d *Decoder) Uint64(num *uint64) {
	*num = d.uint(8)
}

func (d *Decoder) uint(size int) uint64 {
	// skip if errored
	if d.err != nil {
		return 0
	}

	// check length
	if len(d.buf) < size {
		d.err = ErrBufferTooShort
		return 0
	}

	// read
	var num uint64
	switch size {
	case 1:
		num = uint64(d.buf[0])
	case 2:
		num = uint64(binary.BigEndian.Uint16(d.buf))
	case 4:
		num = uint64(binary.BigEndian.Uint32(d.buf))
	case 8:
		num = binary.BigEndian.Uint64(d.buf)
	}

	// slice
	d.buf = d.buf[size:]

	return num
}

// VarUint reads a variable unsigned integer.
func (d *Decoder) VarUint(num *uint64) {
	// skip if errored
	if d.err != nil {
		return
	}

	// read
	var n int
	*num, n = binary.Uvarint(d.buf)
	if n == 0 {
		d.err = ErrBufferTooShort
		return
	}

	// slice
	d.buf = d.buf[n:]
}

// VarInt reads a variable signed integer.
func (d *Decoder) VarInt(num *int64) {
	// skip if errored
	if d.err != nil {
		return
	}

	// read
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
