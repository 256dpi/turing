package turing

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var operationMetrics = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "turing",
	Subsystem: "",
	Name:      "operations",
	Help:      "Internal operation timings in milliseconds.",
}, []string{"name"})

var instructionMetrics = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "turing",
	Subsystem: "",
	Name:      "instructions",
	Help:      "Instruction execution timings in milliseconds.",
}, []string{"name"})

var databaseMetrics = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "turing",
	Subsystem: "",
	Name:      "database",
	Help:      "Various database metrics.",
}, []string{"metric"})

var observerCacheCache sync.Map

// RegisterMetrics will register prometheus metrics.
func RegisterMetrics() {
	prometheus.MustRegister(operationMetrics)
	prometheus.MustRegister(instructionMetrics)
	prometheus.MustRegister(databaseMetrics)
}

func getObserver(summary *prometheus.HistogramVec, label string) prometheus.Observer {
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

func observe(summary *prometheus.HistogramVec, label string) prometheus.Timer {
	// dereference to prevent allocation
	return *prometheus.NewTimer(getObserver(summary, label))
}
