package turing

import (
	"io"
	"io/ioutil"
	"log"
	"os"

	"github.com/lni/dragonboat/logger"
)

var logSink io.Writer = os.Stdout

func SetLogger(sink io.Writer) {
	// set silent logger if nil
	if sink == nil {
		// set sink
		logSink = ioutil.Discard

		// set log factory
		logger.SetLoggerFactory(func(string) logger.ILogger {
			return &silentLogger{}
		})

		return
	}

	// set sink
	logSink = sink

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

func (*customLogger) SetLevel(logger.LogLevel) {
	panic("implement me")
}

func (l *customLogger) Debugf(format string, args ...interface{}) {
	l.logger.Printf(format, args)
}

func (l *customLogger) Infof(format string, args ...interface{}) {
	l.logger.Printf(format, args)
}

func (l *customLogger) Warningf(format string, args ...interface{}) {
	l.logger.Printf(format, args)
}

func (l *customLogger) Errorf(format string, args ...interface{}) {
	l.logger.Printf(format, args)
}

func (l *customLogger) Panicf(format string, args ...interface{}) {
	l.logger.Panicf(format, args)
}

type silentLogger struct{}

func (silentLogger) SetLevel(logger.LogLevel) {}

func (silentLogger) Debugf(format string, args ...interface{}) {}

func (silentLogger) Infof(format string, args ...interface{}) {}

func (silentLogger) Warningf(format string, args ...interface{}) {}

func (silentLogger) Errorf(format string, args ...interface{}) {}

func (silentLogger) Panicf(format string, args ...interface{}) {}
