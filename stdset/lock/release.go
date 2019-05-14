package lock

import (
	"encoding/json"
	"time"

	"github.com/256dpi/turing"
)

type Release struct {
	Key      []byte    `json:"key"`
	Value    string    `json:"value"`
	Time     time.Time `json:"time"`
	Unlocked bool      `json:"unlocked"`
}

func (r *Release) Describe() turing.Description {
	return turing.Description{
		Name:        "stdset/lock.Release",
		Cardinality: 1,
	}
}

func (r *Release) Build() turing.Instruction {
	return &Release{}
}

func (r *Release) Encode() ([]byte, error) {
	return json.Marshal(r)
}

func (r *Release) Decode(data []byte) error {
	return json.Unmarshal(data, r)
}

func (r *Release) Execute(txn *turing.Transaction) error {
	// reset fields
	r.Unlocked = false

	// prepare lock
	var lock Lock

	// get pair
	pair, err := txn.Get(r.Key)
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
		if lock.Time.Before(r.Time) && lock.Value != r.Value {
			return nil
		}

		// delete lock
		err = txn.Delete(r.Key)
		if err != nil {
			return err
		}

		// set flag
		r.Unlocked = true
	}

	return nil
}
