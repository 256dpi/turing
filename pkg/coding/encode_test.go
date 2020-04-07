package coding

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncode(t *testing.T) {
	res, err := Encode(func(enc *Encoder) error {
		enc.Bool(true)
		enc.Int(7)
		enc.String("foo")
		enc.Bytes([]byte("bar"))
		enc.Uint(512)
		enc.Tail([]byte("baz"))
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, []byte("\x01\x0e\x03foo\x03bar\x80\x04baz"), res)
}

func BenchmarkEncode(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := Encode(func(enc *Encoder) error {
			enc.Bool(true)
			enc.Int(7)
			enc.String("foo")
			enc.Bytes([]byte("bar"))
			enc.Uint(512)
			enc.Tail([]byte("baz"))
			return nil
		})
		if err != nil {
			panic(err)
		}
	}
}
