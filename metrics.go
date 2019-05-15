package turing

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var operationMetrics = prometheus.NewSummaryVec(prometheus.SummaryOpts{
	Namespace: "turing",
	Subsystem: "",
	Name:      "operations",
	Help:      "Internal operation timings in milliseconds.",
}, []string{"name"})

var instructionMetrics = prometheus.NewSummaryVec(prometheus.SummaryOpts{
	Namespace: "turing",
	Subsystem: "",
	Name:      "instructions",
	Help:      "Instruction execution timings in milliseconds.",
}, []string{"name"})

var databaseMetrics = prometheus.NewSummaryVec(prometheus.SummaryOpts{
	Namespace: "turing",
	Subsystem: "",
	Name:      "database",
	Help:      "Various database metrics.",
}, []string{"metric"})

// RegisterMetrics will register prometheus metrics.
func RegisterMetrics() {
	prometheus.MustRegister(operationMetrics)
	prometheus.MustRegister(instructionMetrics)
	prometheus.MustRegister(databaseMetrics)
}

func observe(summary prometheus.Observer) func() {
	start := time.Now()
	return func() {
		summary.Observe(float64(time.Since(start)) / float64(time.Millisecond))
	}
}
