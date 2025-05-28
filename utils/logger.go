package utils

import (
	"log"
	"os"
)

// Logger 로깅 기능을 제공하는 구조체
type Logger struct {
	infoLogger  *log.Logger
	warnLogger  *log.Logger
	errorLogger *log.Logger
	fatalLogger *log.Logger
}

// NewLogger 새로운 로거 생성
func NewLogger() *Logger {
	return &Logger{
		infoLogger:  log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile),
		warnLogger:  log.New(os.Stdout, "WARN: ", log.Ldate|log.Ltime|log.Lshortfile),
		errorLogger: log.New(os.Stderr, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile),
		fatalLogger: log.New(os.Stderr, "FATAL: ", log.Ldate|log.Ltime|log.Lshortfile),
	}
}

// Info 정보 로그 출력
func (l *Logger) Info(msg string) {
	l.infoLogger.Println(msg)
}

// Infof 포맷된 정보 로그 출력
func (l *Logger) Infof(format string, args ...interface{}) {
	l.infoLogger.Printf(format, args...)
}

// Warn 경고 로그 출력
func (l *Logger) Warn(msg string) {
	l.warnLogger.Println(msg)
}

// Warnf 포맷된 경고 로그 출력
func (l *Logger) Warnf(format string, args ...interface{}) {
	l.warnLogger.Printf(format, args...)
}

// Error 에러 로그 출력
func (l *Logger) Error(msg string) {
	l.errorLogger.Println(msg)
}

// Errorf 포맷된 에러 로그 출력
func (l *Logger) Errorf(format string, args ...interface{}) {
	l.errorLogger.Printf(format, args...)
}

// Fatal 치명적 에러 로그 출력 후 프로그램 종료
func (l *Logger) Fatal(msg string) {
	l.fatalLogger.Fatal(msg)
}

// Fatalf 포맷된 치명적 에러 로그 출력 후 프로그램 종료
func (l *Logger) Fatalf(format string, args ...interface{}) {
	l.fatalLogger.Fatalf(format, args...)
}