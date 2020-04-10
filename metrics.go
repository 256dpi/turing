package turing

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var systemMetrics = prometheus.NewSummaryVec(prometheus.SummaryOpts{
	Namespace: "turing",
	Subsystem: "",
	Name:      "system",
	Help:      "Internal operation timings in milliseconds.",
}, []string{"name"})

var instructionMetrics = prometheus.NewSummaryVec(prometheus.SummaryOpts{
	Namespace: "turing",
	Subsystem: "",
	Name:      "instructions",
	Help:      "Instruction execution timings in milliseconds.",
}, []string{"name"})

var operatorMetrics = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "turing",
	Subsystem: "",
	Name:      "operators",
	Help:      "Operator execution counter.",
}, []string{"name"})

func init() {
	// register metrics
	prometheus.MustRegister(systemMetrics)
	prometheus.MustRegister(instructionMetrics)
	prometheus.MustRegister(operatorMetrics)
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
	since := float64(time.Since(t.begin)/time.Microsecond) / 1000.0
	t.observer.Observe(since)
}
