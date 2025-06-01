// internal/services/cache/interface.go - 캐시 인터페이스 정의
package cache

import "bus-tracker/internal/models"

// StationCacheInterface 정류소 캐시 공통 인터페이스
// V1과 V2 캐시 모두 이 인터페이스를 구현
type StationCacheInterface interface {
	// LoadStationCache 정류소 캐시 로드
	LoadStationCache(routeIDs []string) error

	// GetStationInfo 정류소 정보 조회 (NodeOrd 또는 StationSeq 기반)
	GetStationInfo(routeNmOrId string, nodeOrdOrSeq int) (models.StationCache, bool)

	// GetRouteStationCount 노선의 전체 정류소 개수 반환
	GetRouteStationCount(routeNmOrId string) int

	// EnrichBusLocationWithStationInfo 버스 위치 정보에 정류소 정보 보강
	EnrichBusLocationWithStationInfo(busLocation *models.BusLocation, routeNmOrId string)

	// GetCacheStatistics 캐시 통계 반환 (routes, stations)
	GetCacheStatistics() (int, int)
}
