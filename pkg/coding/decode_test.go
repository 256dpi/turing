package coding

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecode(t *testing.T) {
	data := []byte("\x0e\x03foo\x03bar\x80\x04baz")

	var num int64
	var str string
	var buf []byte
	var mum uint64
	var tail []byte
	err := Decode(data, func(dec *Decoder) error {
		dec.Int(&num)
		dec.String(&str, false)
		dec.Bytes(&buf, false)
		dec.Uint(&mum)
		dec.Tail(&tail, false)
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, int64(7), num)
	assert.Equal(t, "foo", str)
	assert.Equal(t, []byte("bar"), buf)
	assert.Equal(t, uint64(512), mum)
	assert.Equal(t, []byte("baz"), tail)
}

func BenchmarkDecode(b *testing.B) {
	data := []byte("\x0e\x03foo\x03bar\x80\x04baz")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var num int64
		var str string
		var buf []byte
		var mum uint64
		var tail []byte
		err := Decode(data, func(dec *Decoder) error {
			dec.Int(&num)
			dec.String(&str, false)
			dec.Bytes(&buf, false)
			dec.Uint(&mum)
			dec.Tail(&tail, false)
			return nil
		})
		if err != nil {
			panic(err)
		}
	}
}
