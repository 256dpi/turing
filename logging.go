package turing

import (
	"io"
	"log"

	"github.com/lni/dragonboat/v3/logger"
)

// SetLogger can used to set a custom writer for all logs.
func SetLogger(sink io.Writer) {
	// set silent logger if nil
	if sink == nil {
		// set log factory
		logger.SetLoggerFactory(func(string) logger.ILogger {
			return &customLogger{}
		})

		return
	}

	// otherwise set custom logger
	logger.SetLoggerFactory(func(prefix string) logger.ILogger {
		return &customLogger{
			logger: log.New(sink, prefix, log.LstdFlags),
		}
	})
}

type customLogger struct {
	logger *log.Logger
}

func (l *customLogger) SetLevel(logger.LogLevel) {
	// do nothing
}

func (l *customLogger) Debugf(format string, args ...interface{}) {
	if l.logger != nil {
		l.logger.Printf(format, args...)
	}
}

func (l *customLogger) Infof(format string, args ...interface{}) {
	if l.logger != nil {
		l.logger.Printf(format, args...)
	}
}

func (l *customLogger) Warningf(format string, args ...interface{}) {
	if l.logger != nil {
		l.logger.Printf(format, args...)
	}
}

func (l *customLogger) Errorf(format string, args ...interface{}) {
	if l.logger != nil {
		l.logger.Printf(format, args...)
	}
}

func (l *customLogger) Panicf(format string, args ...interface{}) {
	if l.logger != nil {
		l.logger.Panicf(format, args...)
	}
}
