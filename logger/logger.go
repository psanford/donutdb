package logger

import (
	"log"
	"os"
)

type Logger interface {
	Log(...interface{})
}

type defaultLogger struct {
	logger *log.Logger
}

func (l *defaultLogger) Log(args ...interface{}) {
	l.logger.Println(args...)
}

var StdoutLogger = &defaultLogger{
	logger: log.New(os.Stdout, "", log.LstdFlags),
}
