package turing

import (
	"sync"
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

type observerCache struct {
	vec   prometheus.ObserverVec
	cache sync.Map
}

func (c *observerCache) get(label string) prometheus.Observer {
	// get observer
	value, ok := c.cache.Load(label)
	if ok {
		return value.(prometheus.Observer)
	}

	// create observer
	observer := c.vec.WithLabelValues(label)

	// store observer
	c.cache.Store(label, observer)

	return observer
}

var instructionObserverCache = &observerCache{
	vec: instructionMetrics,
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
	if t.observer != nil {
		t.observer.Observe(time.Since(t.begin).Seconds() * 1000.0)
	}
}
