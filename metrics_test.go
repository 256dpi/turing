package turing

import "testing"

func BenchmarkObserve(b *testing.B) {
	obs := operationMetrics.WithLabelValues("foo")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		timer := observe(obs)
		timer.finish()
	}
}
