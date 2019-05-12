package turing

import (
	"os"
	"time"

	"github.com/dgraph-io/badger"
)

type database = badger.DB

func openDatabase(dir string) (*database, error) {
	// ensure directory
	err := os.MkdirAll(dir, 0777)
	if err != nil {
		return nil, err
	}

	// prepare options
	bo := badger.DefaultOptions
	bo.Dir = dir
	bo.ValueDir = dir
	// bo.Logger = nil

	// open database
	db, err := badger.Open(bo)
	if err != nil {
		return nil, err
	}

	// run gc routine
	go func() {
		for {
			// sleep some time
			time.Sleep(time.Second)

			// run gc
			err = db.RunValueLogGC(0.5)
			if err == badger.ErrRejected {
				return
			} else if err != nil && err != badger.ErrNoRewrite {
				panic(err)
			}
		}
	}()

	return db, nil
}
