// internal/utils/helpers.go
package utils

import (
	"fmt"
	"strings"
)

// MaskSensitiveURL serviceKey가 포함된 URL에서 민감한 정보 마스킹
func MaskSensitiveURL(url, serviceKey string) string {
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

// Contains 문자열 포함 여부 확인
func Contains(s, substr string) bool {
	return strings.Contains(s, substr)
}

// JoinStrings 문자열 슬라이스를 구분자로 연결
func JoinStrings(strs []string, sep string) string {
	return strings.Join(strs, sep)
}

// HasPrefix 문자열이 접두사로 시작하는지 확인
func HasPrefix(s, prefix string) bool {
	return strings.HasPrefix(s, prefix)
}

// TrimSpace 문자열 앞뒤 공백 제거
func TrimSpace(s string) string {
	return strings.TrimSpace(s)
}

// Split 문자열을 구분자로 분할
func Split(s, sep string) []string {
	return strings.Split(s, sep)
}
