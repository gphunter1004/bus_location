// utils/logger.go - DEBUG 레벨 추가 버전

package utils

import (
	"log"
	"os"
	"strings"
)

// LogLevel 로그 레벨 타입
type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
	FATAL
)

// Logger 로깅 기능을 제공하는 구조체
type Logger struct {
	debugLogger *log.Logger
	infoLogger  *log.Logger
	warnLogger  *log.Logger
	errorLogger *log.Logger
	fatalLogger *log.Logger
	level       LogLevel
}

// NewLogger 새로운 로거 생성
func NewLogger() *Logger {
	// 환경변수에서 로그 레벨 읽기 (기본값: DEBUG)
	logLevelStr := strings.ToUpper(os.Getenv("LOG_LEVEL"))
	if logLevelStr == "" {
		logLevelStr = "DEBUG" // 기본값을 DEBUG로 변경
	}

	var level LogLevel
	switch logLevelStr {
	case "DEBUG":
		level = DEBUG
	case "INFO":
		level = INFO
	case "WARN":
		level = WARN
	case "ERROR":
		level = ERROR
	case "FATAL":
		level = FATAL
	default:
		level = DEBUG // 잘못된 값이면 DEBUG로 설정
	}

	return &Logger{
		debugLogger: log.New(os.Stdout, "DEBUG: ", log.Ldate|log.Ltime|log.Lshortfile),
		infoLogger:  log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile),
		warnLogger:  log.New(os.Stdout, "WARN: ", log.Ldate|log.Ltime|log.Lshortfile),
		errorLogger: log.New(os.Stderr, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile),
		fatalLogger: log.New(os.Stderr, "FATAL: ", log.Ldate|log.Ltime|log.Lshortfile),
		level:       level,
	}
}

// Debug 디버그 로그 출력
func (l *Logger) Debug(msg string) {
	if l.level <= DEBUG {
		l.debugLogger.Println(msg)
	}
}

// Debugf 포맷된 디버그 로그 출력
func (l *Logger) Debugf(format string, args ...interface{}) {
	if l.level <= DEBUG {
		l.debugLogger.Printf(format, args...)
	}
}

// Info 정보 로그 출력
func (l *Logger) Info(msg string) {
	if l.level <= INFO {
		l.infoLogger.Println(msg)
	}
}

// Infof 포맷된 정보 로그 출력
func (l *Logger) Infof(format string, args ...interface{}) {
	if l.level <= INFO {
		l.infoLogger.Printf(format, args...)
	}
}

// Warn 경고 로그 출력
func (l *Logger) Warn(msg string) {
	if l.level <= WARN {
		l.warnLogger.Println(msg)
	}
}

// Warnf 포맷된 경고 로그 출력
func (l *Logger) Warnf(format string, args ...interface{}) {
	if l.level <= WARN {
		l.warnLogger.Printf(format, args...)
	}
}

// Error 에러 로그 출력
func (l *Logger) Error(msg string) {
	if l.level <= ERROR {
		l.errorLogger.Println(msg)
	}
}

// Errorf 포맷된 에러 로그 출력
func (l *Logger) Errorf(format string, args ...interface{}) {
	if l.level <= ERROR {
		l.errorLogger.Printf(format, args...)
	}
}

// Fatal 치명적 에러 로그 출력 후 프로그램 종료
func (l *Logger) Fatal(msg string) {
	l.fatalLogger.Fatal(msg)
}

// Fatalf 포맷된 치명적 에러 로그 출력 후 프로그램 종료
func (l *Logger) Fatalf(format string, args ...interface{}) {
	l.fatalLogger.Fatalf(format, args...)
}

// SetLevel 로그 레벨 설정
func (l *Logger) SetLevel(level LogLevel) {
	l.level = level
}

// GetLevel 현재 로그 레벨 반환
func (l *Logger) GetLevel() LogLevel {
	return l.level
}
