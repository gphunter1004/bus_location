// internal/utils/common.go - 수정된 공용 헬퍼 함수 모음
package utils

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// StringHelpers 문자열 관련 헬퍼 함수들
type StringHelpers struct{}

var String StringHelpers

// Contains 문자열 포함 여부 확인
func (StringHelpers) Contains(s, substr string) bool {
	return strings.Contains(s, substr)
}

// Join 문자열 슬라이스를 구분자로 연결
func (StringHelpers) Join(strs []string, sep string) string {
	return strings.Join(strs, sep)
}

// HasPrefix 문자열이 접두사로 시작하는지 확인
func (StringHelpers) HasPrefix(s, prefix string) bool {
	return strings.HasPrefix(s, prefix)
}

// TrimSpace 문자열 앞뒤 공백 제거
func (StringHelpers) TrimSpace(s string) string {
	return strings.TrimSpace(s)
}

// Split 문자열을 구분자로 분할
func (StringHelpers) Split(s, sep string) []string {
	return strings.Split(s, sep)
}

// MaskSensitive 민감한 정보 마스킹 (API 키, 비밀번호 등)
func (StringHelpers) MaskSensitive(value string, showStart, showEnd int) string {
	if len(value) <= showStart+showEnd {
		return strings.Repeat("*", len(value))
	}

	start := value[:showStart]
	end := value[len(value)-showEnd:]
	middle := strings.Repeat("*", len(value)-showStart-showEnd)

	return start + middle + end
}

// MaskSensitiveURL serviceKey가 포함된 URL에서 민감한 정보 마스킹
func (StringHelpers) MaskSensitiveURL(url, serviceKey string) string {
	if len(serviceKey) > 10 {
		masked := serviceKey[:6] + "***" + serviceKey[len(serviceKey)-4:]
		if len(url) > 80 {
			return fmt.Sprintf("URL: %s... (serviceKey: %s)", url[:80], masked)
		}
		return fmt.Sprintf("URL: %s (serviceKey: %s)", url, masked)
	}
	if len(url) > 80 {
		return url[:80] + "..."
	}
	return url
}

// ConversionHelpers 타입 변환 관련 헬퍼 함수들
type ConversionHelpers struct{}

var Convert ConversionHelpers

// StringToInt 문자열을 int로 변환 (실패시 기본값 반환)
func (ConversionHelpers) StringToInt(s string, defaultValue int) int {
	s = strings.TrimSpace(s)
	if s == "" {
		return defaultValue
	}

	if value, err := strconv.Atoi(s); err == nil {
		return value
	}
	return defaultValue
}

// StringToInt64 문자열을 int64로 변환 (실패시 기본값 반환)
func (ConversionHelpers) StringToInt64(s string, defaultValue int64) int64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return defaultValue
	}

	if value, err := strconv.ParseInt(s, 10, 64); err == nil {
		return value
	}
	return defaultValue
}

// StringToBool 문자열을 bool로 변환 (실패시 기본값 반환)
func (ConversionHelpers) StringToBool(s string, defaultValue bool) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return defaultValue
	}

	if value, err := strconv.ParseBool(s); err == nil {
		return value
	}
	return defaultValue
}

// StringToFloat64 문자열을 float64로 변환 (실패시 기본값 반환)
func (ConversionHelpers) StringToFloat64(s string, defaultValue float64) float64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return defaultValue
	}

	if value, err := strconv.ParseFloat(s, 64); err == nil {
		return value
	}
	return defaultValue
}

// InterfaceToString interface{}를 문자열로 안전하게 변환
func (ConversionHelpers) InterfaceToString(value interface{}) string {
	if str, ok := value.(string); ok {
		return str
	}
	return ""
}

// InterfaceToInt interface{}를 int로 안전하게 변환
func (ConversionHelpers) InterfaceToInt(value interface{}) int {
	switch v := value.(type) {
	case int:
		return v
	case float64:
		return int(v)
	case int64:
		return int(v)
	case string:
		if intVal, err := strconv.Atoi(v); err == nil {
			return intVal
		}
	}
	return 0
}

// InterfaceToInt64 interface{}를 int64로 안전하게 변환
func (ConversionHelpers) InterfaceToInt64(value interface{}) int64 {
	switch v := value.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	case string:
		if int64Val, err := strconv.ParseInt(v, 10, 64); err == nil {
			return int64Val
		}
	}
	return 0
}

// TimeHelpers 시간 관련 헬퍼 함수들
type TimeHelpers struct{}

var Time TimeHelpers

// FormatDuration 기간을 사용자 친화적 형식으로 변환
func (TimeHelpers) FormatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%d초", int(d.Seconds()))
	} else if d < time.Hour {
		return fmt.Sprintf("%d분", int(d.Minutes()))
	} else if d < 24*time.Hour {
		hours := int(d.Hours())
		minutes := int(d.Minutes()) % 60
		return fmt.Sprintf("%d시간 %d분", hours, minutes)
	} else {
		days := int(d.Hours()) / 24
		hours := int(d.Hours()) % 24
		return fmt.Sprintf("%d일 %d시간", days, hours)
	}
}

// CalculateUptime 시작 시간부터 현재까지의 업타임 계산
func (TimeHelpers) CalculateUptime(startTime time.Time) string {
	if startTime.IsZero() {
		return "정보 없음"
	}
	return Time.FormatDuration(time.Since(startTime))
}

// ParseDurationSeconds 초 단위 문자열을 Duration으로 변환
func (TimeHelpers) ParseDurationSeconds(seconds string, defaultSeconds int) time.Duration {
	seconds = strings.TrimSpace(seconds)
	if seconds == "" {
		return time.Duration(defaultSeconds) * time.Second
	}

	if value, err := strconv.Atoi(seconds); err == nil {
		return time.Duration(value) * time.Second
	}
	return time.Duration(defaultSeconds) * time.Second
}

// ParseDurationMinutes 분 단위 문자열을 Duration으로 변환
func (TimeHelpers) ParseDurationMinutes(minutes string, defaultMinutes int) time.Duration {
	minutes = strings.TrimSpace(minutes)
	if minutes == "" {
		return time.Duration(defaultMinutes) * time.Minute
	}

	if value, err := strconv.Atoi(minutes); err == nil {
		return time.Duration(value) * time.Minute
	}
	return time.Duration(defaultMinutes) * time.Minute
}

// SliceHelpers 슬라이스 관련 헬퍼 함수들
type SliceHelpers struct{}

var Slice SliceHelpers

// ContainsString 문자열 슬라이스에 특정 문자열이 포함되어 있는지 확인
func (SliceHelpers) ContainsString(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// RemoveDuplicateStrings 문자열 슬라이스에서 중복 제거
func (SliceHelpers) RemoveDuplicateStrings(slice []string) []string {
	seen := make(map[string]bool)
	var result []string

	for _, item := range slice {
		if !seen[item] {
			seen[item] = true
			result = append(result, item)
		}
	}

	return result
}

// RemoveEmptyStrings 빈 문자열 제거
func (SliceHelpers) RemoveEmptyStrings(slice []string) []string {
	var result []string
	for _, str := range slice {
		if strings.TrimSpace(str) != "" {
			result = append(result, str)
		}
	}
	return result
}

// ValidationHelpers 검증 관련 헬퍼 함수들
type ValidationHelpers struct{}

var Validate ValidationHelpers

// IsEmpty 값이 비어있는지 확인
func (ValidationHelpers) IsEmpty(value interface{}) bool {
	if value == nil {
		return true
	}

	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v) == ""
	case []string:
		return len(v) == 0
	case map[string]interface{}:
		return len(v) == 0
	default:
		return false
	}
}

// IsNumeric 문자열이 숫자인지 확인
func (ValidationHelpers) IsNumeric(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

// IsInteger 문자열이 정수인지 확인
func (ValidationHelpers) IsInteger(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	_, err := strconv.Atoi(s)
	return err == nil
}

// IsValidRouteID 노선 ID가 유효한지 확인 (API1: 숫자, API2: GGB+숫자)
func (ValidationHelpers) IsValidRouteID(routeID, apiType string) bool {
	if routeID == "" {
		return false
	}

	switch apiType {
	case "api1":
		return Validate.IsInteger(routeID)
	case "api2":
		if !strings.HasPrefix(routeID, "GGB") || len(routeID) < 4 {
			return false
		}
		return Validate.IsInteger(routeID[3:])
	default:
		return false
	}
}

// MathHelpers 수학 관련 헬퍼 함수들
type MathHelpers struct{}

var Math MathHelpers

// Max 두 정수 중 큰 값 반환
func (MathHelpers) Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Min 두 정수 중 작은 값 반환
func (MathHelpers) Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// CalculatePercentage 백분율 계산
func (MathHelpers) CalculatePercentage(part, total int) float64 {
	if total == 0 {
		return 0
	}
	return float64(part) / float64(total) * 100
}

// CalculateAverage 평균 계산
func (MathHelpers) CalculateAverage(values []int) float64 {
	if len(values) == 0 {
		return 0
	}

	sum := 0
	for _, value := range values {
		sum += value
	}

	return float64(sum) / float64(len(values))
}

// IDHelpers ID 생성 관련 헬퍼 함수들
type IDHelpers struct{}

var ID IDHelpers

// GenerateRequestID 요청 ID 생성
func (IDHelpers) GenerateRequestID() string {
	return fmt.Sprintf("req_%d", time.Now().UnixNano())
}

// GenerateSessionID 세션 ID 생성
func (IDHelpers) GenerateSessionID() string {
	return fmt.Sprintf("sess_%d", time.Now().UnixNano())
}

// ErrorHelpers 에러 처리 관련 헬퍼 함수들
type ErrorHelpers struct{}

var Error ErrorHelpers

// IsNotFoundError 에러가 "not found" 타입인지 확인
func (ErrorHelpers) IsNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "not found")
}

// IsValidationError 에러가 "validation" 타입인지 확인
func (ErrorHelpers) IsValidationError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "validation")
}

// IsTimeoutError 에러가 "timeout" 타입인지 확인
func (ErrorHelpers) IsTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "timeout") || strings.Contains(errStr, "deadline")
}

// WrapError 에러를 추가 컨텍스트와 함께 래핑
func (ErrorHelpers) WrapError(err error, context string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", context, err)
}
