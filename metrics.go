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

func init() {
	// register metrics
	prometheus.MustRegister(operationMetrics)
	prometheus.MustRegister(instructionMetrics)
}

type timer struct {
	begin    time.Time
	observer prometheus.Observer
}

func observe(observer prometheus.Observer) timer {
	return timer{
		begin:    time.Now(),
		observer: observer,
	}
}

func (t *timer) finish() {
	t.observer.Observe(time.Since(t.begin).Seconds() * 1000.0)
}
