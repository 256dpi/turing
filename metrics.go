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

var observerCacheCache sync.Map

var metricsEnabled bool

// EnableMetrics will register and enable prometheus metrics.
func EnableMetrics() {
	// set flag
	metricsEnabled = true

	// register metrics
	prometheus.MustRegister(operationMetrics)
	prometheus.MustRegister(instructionMetrics)
}

func getObserver(summary prometheus.ObserverVec, label string) prometheus.Observer {
	// check if enabled
	if !metricsEnabled {
		return nil
	}

	// get cache from cache
	var cache *sync.Map
	value, ok := observerCacheCache.Load(summary)
	if !ok {
		cache = &sync.Map{}
		observerCacheCache.Store(summary, cache)
	} else {
		cache = value.(*sync.Map)
	}

	// get observer
	var observer prometheus.Observer
	value, ok = cache.Load(label)
	if !ok {
		observer = summary.WithLabelValues(label)
		cache.Store(label, observer)
	} else {
		observer = value.(prometheus.Observer)
	}

	return observer
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
