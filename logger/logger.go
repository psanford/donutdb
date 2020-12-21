package logger

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
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

type LogLevelType uint

const (
	LogOff LogLevelType = 0
	// LogHTTPRequests enables HTTP Request logging
	LogHTTPRequests LogLevelType = 1 << iota
)

func LogFields(l Logger, args ...interface{}) {
	var buf bytes.Buffer

	for i := 0; i < len(args); i += 2 {
		if i != 0 {
			buf.WriteByte(' ')
		}

		var k, v string
		if i+1 >= len(args) {
			k = "KEYVAL_MISMATCH"
			v = logFmtValue(args[i])
		} else {
			var ok bool
			k, ok = args[i].(string)
			if !ok {
				k = fmt.Sprintf("NONSTRING_NAME<%s>", logFmtValue(args[i]))
			}
			v = logFmtValue(args[i+1])
		}

		buf.WriteString(k)
		buf.WriteByte('=')
		buf.WriteString(v)
	}

	l.Log(buf.String())
}

func logFmtValue(value interface{}) string {
	if value == nil {
		return "nil"
	}

	switch v := value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", value)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', 3, 64)
	case float64:
		return strconv.FormatFloat(v, 'f', 3, 64)
	case bool:
		return strconv.FormatBool(v)
	case error:
		return fmt.Sprintf("%q", v.Error())
	case time.Time:
		return v.Format(time.RFC3339Nano)
	case fmt.Stringer:
		return fmt.Sprintf("%q", v.String())
	case string:
		return fmt.Sprintf("%q", v)
	}

	return fmt.Sprintf("%q", (fmt.Sprintf("%+v", value)))
}
