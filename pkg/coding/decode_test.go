package coding

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecode(t *testing.T) {
	data := []byte("\x01\x0e\x03foo\x03bar\x80\x04baz")

	var bol bool
	var num int64
	var str string
	var buf []byte
	var mum uint64
	var tail []byte
	err := Decode(data, func(dec *Decoder) error {
		dec.Bool(&bol)
		dec.VarInt(&num)
		dec.String(&str, false)
		dec.Bytes(&buf, false)
		dec.VarUint(&mum)
		dec.Tail(&tail, false)
		return nil
	})
	assert.NoError(t, err)
	assert.True(t, bol)
	assert.Equal(t, int64(7), num)
	assert.Equal(t, "foo", str)
	assert.Equal(t, []byte("bar"), buf)
	assert.Equal(t, uint64(512), mum)
	assert.Equal(t, []byte("baz"), tail)

	assert.Equal(t, 0.0, testing.AllocsPerRun(10, func() {
		var bol bool
		var num int64
		var str string
		var buf []byte
		var mum uint64
		var tail []byte
		_ = Decode(data, func(dec *Decoder) error {
			dec.Bool(&bol)
			dec.VarInt(&num)
			dec.String(&str, false)
			dec.Bytes(&buf, false)
			dec.VarUint(&mum)
			dec.Tail(&tail, false)
			return nil
		})
	}))
}

func BenchmarkDecode(b *testing.B) {
	data := []byte("\x01\x0e\x03foo\x03bar\x80\x04baz")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var bol bool
		var num int64
		var str string
		var buf []byte
		var mum uint64
		var tail []byte
		err := Decode(data, func(dec *Decoder) error {
			dec.Bool(&bol)
			dec.VarInt(&num)
			dec.String(&str, false)
			dec.Bytes(&buf, false)
			dec.VarUint(&mum)
			dec.Tail(&tail, false)
			return nil
		})
		if err != nil {
			panic(err)
		}
	}
}
