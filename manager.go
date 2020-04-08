package turing

import "sync"

type manager struct {
	observers sync.Map
}

func newManager() *manager {
	return &manager{}
}

func (m *manager) subscribe(observer Observer) {
	// add observer
	m.observers.Store(observer, observer)
}

func (m *manager) init() {
	// call init on all registered observers
	m.observers.Range(func(_, value interface{}) bool {
		value.(Observer).Init()
		return true
	})
}

func (m *manager) process(ins Instruction) {
	// prepare cancelled observers
	var cancelled []Observer

	// call process on all subscribed observers
	m.observers.Range(func(_, value interface{}) bool {
		// get observer
		observer := value.(Observer)

		// process instruction
		if !observer.Process(ins) {
			cancelled = append(cancelled, observer)
		}

		return true
	})

	// delete all cancelled observers
	for _, observer := range cancelled {
		m.observers.Delete(observer)
	}
}

func (m *manager) unsubscribe(observer Observer) {
	// remove observer
	m.observers.Delete(observer)
}
