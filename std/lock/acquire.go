package lock

import (
	"time"

	"github.com/vmihailenco/msgpack/v4"

	"github.com/256dpi/turing"
)

type Lock struct {
	Value string    `msgpack:"v,omitempty"`
	Time  time.Time `msgpack:"t,omitempty"`
}

type Acquire struct {
	Key     []byte    `msgpack:"k,omitempty"`
	Value   string    `msgpack:"v,omitempty"`
	Time    time.Time `msgpack:"t,omitempty"`
	Timeout time.Time `msgpack:"o,omitempty"`
	Locked  bool      `msgpack:"l,omitempty"`
}

var acquireDesc = &turing.Description{
	Name: "std/lock/Acquire",
}

func (a *Acquire) Describe() *turing.Description {
	return acquireDesc
}

func (a *Acquire) Effect() int {
	return 1
}

func (a *Acquire) Execute(txn *turing.Transaction) error {
	// reset fields
	a.Locked = false

	// get lock
	var lock Lock
	err := txn.Use(a.Key, func(value []byte) error {
		return msgpack.Unmarshal(value, &lock)
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
	bytes, err := msgpack.Marshal(lock)
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

func (a *Acquire) Encode() ([]byte, turing.Ref, error) {
	buf, err := msgpack.Marshal(a)
	return buf, nil, err
}

func (a *Acquire) Decode(bytes []byte) error {
	return msgpack.Unmarshal(bytes, a)
}
