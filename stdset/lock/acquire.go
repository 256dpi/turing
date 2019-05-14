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
		Name:        "stdset/lock.Acquire",
		Cardinality: 1,
	}
}

func (a *Acquire) Execute(txn *turing.Transaction) error {
	// reset fields
	a.Locked = false

	// prepare lock
	var lock Lock

	// get pair
	pair, err := txn.Get(a.Key)
	if err != nil {
		return err
	}

	// check content if missing
	if pair != nil {
		// decode lock
		err = pair.LoadValue(func(value []byte) error {
			_ = json.Unmarshal(value, &lock)
			return nil
		})
		if err != nil {
			return err
		}

		// cancel if lock is still active and has another value
		if lock.Time.Before(a.Time) && lock.Value != a.Value {
			return nil
		}
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
