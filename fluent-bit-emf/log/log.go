package log

import (
	"fmt"
	"time"
)

type Level int

const (
	DebugLevel Level = iota
	InfoLevel
	WarnLevel
	ErrorLevel
)

type Logger struct {
	level Level
	name  string
	Debug *levelLogger
	Info  *levelLogger
	Warn  *levelLogger
	Error *levelLogger
}

type levelLogger struct {
	prefix string
	level  Level
	logger *Logger
}

// Global logger instance
var defaultLogger = NewLogger("default", InfoLevel)

// Package-level functions that use the default logger
func Debug() *levelLogger { return defaultLogger.Debug }
func Info() *levelLogger  { return defaultLogger.Info }
func Warn() *levelLogger  { return defaultLogger.Warn }
func Error() *levelLogger { return defaultLogger.Error }

// Function to change the default logger's level
func SetLevel(level Level) {
	defaultLogger.level = level
}

func (l *levelLogger) Printf(format string, v ...interface{}) {
	if l.level >= l.logger.level {
		msg := fmt.Sprintf(format, v...)
		fmt.Printf("[%s] %s%s", time.Now().UTC().Format("2006/01/02 15:04:05"), l.prefix, msg)
	}
}

func (l *levelLogger) Println(v ...interface{}) {
	if l.level >= l.logger.level {
		msg := fmt.Sprintln(v...)
		fmt.Printf("[%s] %s%s", time.Now().UTC().Format("2006/01/02 15:04:05"), l.prefix, msg)
	}
}

func Init(name string, level Level) {
	defaultLogger = NewLogger(name, level)
}

func NewLogger(name string, level Level) *Logger {
	logger := &Logger{
		name:  name,
		level: level,
	}

	logger.Debug = &levelLogger{prefix: fmt.Sprintf("[ debug] [%s]: ", name), level: DebugLevel, logger: logger}
	logger.Info = &levelLogger{prefix: fmt.Sprintf("[ info] [%s]: ", name), level: InfoLevel, logger: logger}
	logger.Warn = &levelLogger{prefix: fmt.Sprintf("[ warn] [%s]: ", name), level: WarnLevel, logger: logger}
	logger.Error = &levelLogger{prefix: fmt.Sprintf("[ error] [%s]: ", name), level: ErrorLevel, logger: logger}

	return logger
}
