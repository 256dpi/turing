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

func (m *manager) process(instruction Instruction) {
	// prepare removable observers
	var removable []Observer

	// call process on all subscribed observers
	m.observers.Range(func(_, value interface{}) bool {
		if !value.(Observer).Process(instruction) {
			removable = append(removable, value.(Observer))
		}
		return true
	})

	// delete all removable observers
	for _, observer := range removable {
		m.observers.Delete(observer)
	}
}

func (m *manager) unsubscribe(observer Observer) {
	// remove observer
	m.observers.Delete(observer)
}
