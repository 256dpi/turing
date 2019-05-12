package turing

import (
	"io"
	"log"
	"os"
	"time"

	"github.com/dgraph-io/badger"
)

type database = badger.DB

func openDatabase(dir string, logger io.Writer) (*database, error) {
	// ensure directory
	err := os.MkdirAll(dir, 0777)
	if err != nil {
		return nil, err
	}

	// prepare options
	bo := badger.DefaultOptions
	bo.Dir = dir
	bo.ValueDir = dir
	bo.Logger = newDatabaseLogger(logger)

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

type databaseLogger struct {
	logger *log.Logger
}

func newDatabaseLogger(w io.Writer) *databaseLogger {
	return &databaseLogger{
		logger: log.New(w, "", log.LstdFlags),
	}
}

func (l *databaseLogger) Errorf(f string, v ...interface{}) {
	l.logger.Printf("[ERR] badger: "+f, v...)
}

func (l *databaseLogger) Warningf(f string, v ...interface{}) {
	l.logger.Printf("[WARN] badger: "+f, v...)
}

func (l *databaseLogger) Infof(f string, v ...interface{}) {
	l.logger.Printf("[INFO] badger: "+f, v...)
}

func (l *databaseLogger) Debugf(f string, v ...interface{}) {
	l.logger.Printf("[DEBUG] badger: "+f, v...)
}
