package coding

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncode(t *testing.T) {
	res, _, err := Encode(true, func(enc *Encoder) error {
		enc.Bool(true)
		enc.VarInt(7)
		enc.String("foo")
		enc.Bytes([]byte("bar"))
		enc.VarUint(512)
		enc.Tail([]byte("baz"))
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, []byte("\x01\x0e\x03foo\x03bar\x80\x04baz"), res)

	assert.Equal(t, 0.0, testing.AllocsPerRun(10, func() {
		_, ref, _ := Encode(true, func(enc *Encoder) error {
			enc.Bool(true)
			enc.VarInt(7)
			enc.String("foo")
			enc.Bytes([]byte("bar"))
			enc.VarUint(512)
			enc.Tail([]byte("baz"))
			return nil
		})
		ref.Release()
	}))
}

func BenchmarkEncode(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, ref, err := Encode(true, func(enc *Encoder) error {
			enc.Bool(true)
			enc.VarInt(7)
			enc.String("foo")
			enc.Bytes([]byte("bar"))
			enc.VarUint(512)
			enc.Tail([]byte("baz"))
			return nil
		})
		if err != nil {
			panic(err)
		}

		ref.Release()
	}
}
