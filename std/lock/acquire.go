package lock

import (
	"encoding/json"
	"time"

	"github.com/256dpi/turing"
)

type Lock struct {
	Value string    `json:"value"`
	Time  time.Time `json:"time"`
}

type Acquire struct {
	Key     []byte    `json:"key"`
	Value   string    `json:"value"`
	Time    time.Time `json:"time"`
	Timeout time.Time `json:"timeout"`
	Locked  bool      `json:"locked"`
}

func (a *Acquire) Describe() turing.Description {
	return turing.Description{
		Name:   "std/lock.Acquire",
		Effect: 1,
	}
}

func (a *Acquire) Execute(txn *turing.Transaction) error {
	// reset fields
	a.Locked = false

	// get lock
	var lock Lock
	err := txn.Use(a.Key, func(value []byte) error {
		return json.Unmarshal(value, &lock)
	})
	if err != nil {
		return err
	}

	// skip if lock exists, is still active and has another value
	if !lock.Time.IsZero() && lock.Time.Before(a.Time) && lock.Value != a.Value {
		return nil
	}

	// configure lock
	lock.Value = a.Value
	lock.Time = a.Time

	// encode lock
	bytes, err := json.Marshal(lock)
	if err != nil {
		return err
	}

	// set new lock
	err = txn.Set(a.Key, bytes)
	if err != nil {
		return err
	}

	// set flag
	a.Locked = true

	return nil
}
