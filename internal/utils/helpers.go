// internal/utils/helpers.go - 기존 파일을 공용 헬퍼 사용으로 수정
package utils

// 기존 함수들을 공용 헬퍼로 리다이렉트하여 하위 호환성 유지

// MaskSensitiveURL serviceKey가 포함된 URL에서 민감한 정보 마스킹
// Deprecated: Use utils.String.MaskSensitiveURL instead
func MaskSensitiveURL(url, serviceKey string) string {
	return String.MaskSensitiveURL(url, serviceKey)
}

// Contains 문자열 포함 여부 확인
// Deprecated: Use utils.String.Contains instead
func Contains(s, substr string) bool {
	return String.Contains(s, substr)
}

// JoinStrings 문자열 슬라이스를 구분자로 연결
// Deprecated: Use utils.String.Join instead
func JoinStrings(strs []string, sep string) string {
	return String.Join(strs, sep)
}

// HasPrefix 문자열이 접두사로 시작하는지 확인
// Deprecated: Use utils.String.HasPrefix instead
func HasPrefix(s, prefix string) bool {
	return String.HasPrefix(s, prefix)
}

// TrimSpace 문자열 앞뒤 공백 제거
// Deprecated: Use utils.String.TrimSpace instead
func TrimSpace(s string) string {
	return String.TrimSpace(s)
}

// Split 문자열을 구분자로 분할
// Deprecated: Use utils.String.Split instead
func Split(s, sep string) []string {
	return String.Split(s, sep)
}

// ===== 새로운 공용 헬퍼 사용 예시 =====

// 사용 예시:
// 1. 문자열 헬퍼
//    masked := utils.String.MaskSensitive("secretkey123", 3, 3) // "sec***123"
//
// 2. 타입 변환 헬퍼
//    port := utils.Convert.StringToInt(os.Getenv("PORT"), 8080)
//
// 3. 시간 헬퍼
//    uptime := utils.Time.CalculateUptime(startTime)
//
// 4. 슬라이스 헬퍼
//    if utils.Slice.ContainsString(routeIDs, targetRoute) { ... }
//
// 5. 검증 헬퍼
//    if utils.Validate.IsValidRouteID(routeID, "api1") { ... }
