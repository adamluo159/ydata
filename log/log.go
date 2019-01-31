package log

import (
	"fmt"
	"log"
)

type ILog interface {
	Debug(format string, a ...interface{})
	Info(format string, a ...interface{})
	Error(format string, a ...interface{})
}

var (
	logger ILog = nil
)

func Debug(format string, a ...interface{}) {
	if logger == nil {
		log.Printf(fmt.Sprintf("[debug] "+format+"\n", a...))
	} else {
		logger.Debug(format, a...)
	}
}

func Info(format string, a ...interface{}) {
	if logger == nil {
		log.Printf(fmt.Sprintf("[info]  "+format+"\n", a...))
	} else {
		logger.Info(format, a...)
	}
}

func Error(format string, a ...interface{}) {
	if logger == nil {
		log.Printf(fmt.Sprintf("[error] "+format+"\n", a...))
	} else {
		logger.Error(format, a...)
	}
}

func SetLog(l ILog) {
	logger = l
}
