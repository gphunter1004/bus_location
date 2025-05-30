// services/route_id_converter.go - Route ID 형식 변환 전용 파일
package services

import (
	"fmt"
	"strconv"
	"strings"
)

// RouteIDConverter Route ID 형식 변환 및 통합 관리
type RouteIDConverter struct{}

// NewRouteIDConverter 새로운 Route ID 변환기 생성
func NewRouteIDConverter() *RouteIDConverter {
	return &RouteIDConverter{}
}

// ToUnifiedKey Route ID를 통합 캐시 키로 변환 (항상 숫자 형식)
func (rc *RouteIDConverter) ToUnifiedKey(routeID string) string {
	// GGB로 시작하면 GGB 제거하고 숫자만 반환
	if strings.HasPrefix(routeID, "GGB") {
		return routeID[3:]
	}
	// 이미 숫자면 그대로 반환
	return routeID
}

// ToAPI1Format Route ID를 API1 형식으로 변환 (숫자만)
func (rc *RouteIDConverter) ToAPI1Format(routeID string) string {
	return rc.ToUnifiedKey(routeID)
}

// ToAPI2Format Route ID를 API2 형식으로 변환 (GGB + 숫자)
func (rc *RouteIDConverter) ToAPI2Format(routeID string) string {
	unifiedKey := rc.ToUnifiedKey(routeID)
	if strings.HasPrefix(routeID, "GGB") {
		return routeID // 이미 GGB 형식
	}
	return "GGB" + unifiedKey
}

// ValidateRouteID Route ID 형식 검증
func (rc *RouteIDConverter) ValidateRouteID(routeID, apiType string) error {
	if routeID == "" {
		return fmt.Errorf("routeID가 비어있습니다")
	}

	if apiType == "api1" {
		// API1: 숫자만 허용 (GGB가 있어도 자동 변환 지원)
		unifiedKey := rc.ToUnifiedKey(routeID)
		if _, err := strconv.ParseInt(unifiedKey, 10, 64); err != nil {
			return fmt.Errorf("API1 routeID는 숫자여야 합니다: '%s' (변환된 키: '%s')", routeID, unifiedKey)
		}
	} else if apiType == "api2" {
		// API2: GGB + 숫자 허용 (숫자만 있어도 자동 변환 지원)
		unifiedKey := rc.ToUnifiedKey(routeID)
		if _, err := strconv.ParseInt(unifiedKey, 10, 64); err != nil {
			return fmt.Errorf("API2 routeID의 숫자 부분이 올바르지 않습니다: '%s' (변환된 키: '%s')", routeID, unifiedKey)
		}
	}

	return nil
}

// GetOriginalAndUnified 원본 Route ID와 통합 키를 모두 반환
func (rc *RouteIDConverter) GetOriginalAndUnified(routeID string) (original, unified string) {
	return routeID, rc.ToUnifiedKey(routeID)
}

// GetDisplayName 사용자에게 표시할 Route ID 이름 반환
func (rc *RouteIDConverter) GetDisplayName(routeID string) string {
	if strings.HasPrefix(routeID, "GGB") {
		return fmt.Sprintf("%s (API2 형식)", routeID)
	}
	return fmt.Sprintf("%s (API1 형식)", routeID)
}
