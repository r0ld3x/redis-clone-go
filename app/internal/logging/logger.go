package logging

import (
	"fmt"
	"time"
)

type Logger struct {
	component string
}

func NewLogger(component string) *Logger {
	return &Logger{component: component}
}

func (l *Logger) Info(message string, args ...interface{}) {
	timestamp := time.Now().Format("15:04:05.000")
	prefix := fmt.Sprintf("[%s] [%s]", timestamp, l.component)
	fmt.Printf(prefix+" "+message+"\n", args...)
}

func (l *Logger) Error(message string, args ...interface{}) {
	timestamp := time.Now().Format("15:04:05.000")
	prefix := fmt.Sprintf("[%s] [%s] ❌ ERROR:", timestamp, l.component)
	fmt.Printf(prefix+" "+message+"\n", args...)
}

func (l *Logger) Success(message string, args ...interface{}) {
	timestamp := time.Now().Format("15:04:05.000")
	prefix := fmt.Sprintf("[%s] [%s] ✅ SUCCESS:", timestamp, l.component)
	fmt.Printf(prefix+" "+message+"\n", args...)
}

func (l *Logger) Debug(message string, args ...interface{}) {
	timestamp := time.Now().Format("15:04:05.000")
	prefix := fmt.Sprintf("[%s] [%s] 🔍 DEBUG:", timestamp, l.component)
	fmt.Printf(prefix+" "+message+"\n", args...)
}

func (l *Logger) Network(direction, message string, args ...interface{}) {
	timestamp := time.Now().Format("15:04:05.000")
	arrow := "📤 OUT"
	if direction == "IN" {
		arrow = "📥 IN"
	}
	prefix := fmt.Sprintf("[%s] [%s] %s:", timestamp, l.component, arrow)
	fmt.Printf(prefix+" "+message+"\n", args...)
}

// Global logging functions for backward compatibility
func LogInfo(component, message string, args ...interface{}) {
	logger := NewLogger(component)
	logger.Info(message, args...)
}

func LogError(component, message string, args ...interface{}) {
	logger := NewLogger(component)
	logger.Error(message, args...)
}

func LogSuccess(component, message string, args ...interface{}) {
	logger := NewLogger(component)
	logger.Success(message, args...)
}

func LogDebug(component, message string, args ...interface{}) {
	logger := NewLogger(component)
	logger.Debug(message, args...)
}

func LogNetwork(component, direction, message string, args ...interface{}) {
	logger := NewLogger(component)
	logger.Network(direction, message, args...)
}
