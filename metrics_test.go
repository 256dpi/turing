package turing

import "testing"

func BenchmarkObserve(b *testing.B) {
	obs := systemMetrics.WithLabelValues("foo")

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			timer := observe(obs)
			timer.finish()
		}
	})
}
