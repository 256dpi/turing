package lock

import (
	"encoding/json"
	"time"

	"github.com/256dpi/turing"
)

type Release struct {
	Key      []byte    `json:"k,omitempty"`
	Value    string    `json:"v,omitempty"`
	Time     time.Time `json:"t,omitempty"`
	Unlocked bool      `json:"u,omitempty"`
}

func (r *Release) Describe() turing.Description {
	return turing.Description{
		Name:   "std/lock/Release",
		Effect: 1,
	}
}

func (r *Release) Execute(txn *turing.Transaction) error {
	// reset fields
	r.Unlocked = false

	// get lock
	var lock Lock
	err := txn.Use(r.Key, func(value []byte) error {
		return json.Unmarshal(value, &lock)
	})
	if err != nil {
		return err
	}

	// skip if lock is missing or is still active and has another value
	if lock.Time.IsZero() || (lock.Time.Before(r.Time) && lock.Value != r.Value) {
		return nil
	}

	// unset lock
	err = txn.Unset(r.Key)
	if err != nil {
		return err
	}

	// set flag
	r.Unlocked = true

	return nil
}
