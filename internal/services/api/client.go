// internal/services/api/client.go - 통합 모드 전용
package api

import (
	"bus-tracker/config"
	"bus-tracker/internal/models"
	"bus-tracker/internal/utils"
)

// BusAPIClient 버스 API 클라이언트 공통 인터페이스
type BusAPIClient interface {
	// FetchBusLocationByRoute 특정 routeId로 버스 데이터를 가져옴
	FetchBusLocationByRoute(routeID string) ([]models.BusLocation, error)

	// FetchAllBusLocations 모든 routeId에 대해 버스 데이터를 병렬로 가져옴
	FetchAllBusLocations(routeIDs []string) ([]models.BusLocation, error)

	// GetAPIType API 타입 반환
	GetAPIType() string

	// LoadStationCache 정류소 정보 캐시 로드
	LoadStationCache(routeIDs []string) error

	// GetCacheStatistics 캐시 통계 반환
	GetCacheStatistics() (int, int)

	// GetRouteStationCount 특정 노선의 전체 정류소 개수 반환
	GetRouteStationCount(routeID string) int
}

// APIClientBase 공통 베이스 구조체 (공통 필드와 메서드)
type APIClientBase struct {
	config *config.Config
	logger *utils.Logger
}
