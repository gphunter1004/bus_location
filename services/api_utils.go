package services

import (
	"fmt"
	"strings"
)

// maskSensitiveURL serviceKey가 포함된 URL에서 민감한 정보 마스킹
func maskSensitiveURL(url, serviceKey string) string {
	// serviceKey 파라미터를 찾아서 마스킹
	if len(serviceKey) > 10 {
		masked := serviceKey[:6] + "***" + serviceKey[len(serviceKey)-4:]
		return fmt.Sprintf("URL: %s... (serviceKey: %s)", url[:80], masked)
	}
	return url[:80] + "..."
}

// contains 문자열 포함 여부 확인 (strings.Contains 대체)
func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}

// joinStrings 문자열 슬라이스를 구분자로 연결 (strings.Join 대체)
func joinStrings(strs []string, sep string) string {
	return strings.Join(strs, sep)
}