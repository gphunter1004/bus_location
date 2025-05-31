// internal/web/services/cache_service.go
package services

import (
	"fmt"
	"time"

	"bus-tracker/config"
	"bus-tracker/internal/services/cache"
	"bus-tracker/internal/utils"
)

// CacheService 캐시 관련 서비스
type CacheService struct {
	config       *config.Config
	logger       *utils.Logger
	stationCache *cache.StationCacheService
}

// NewCacheService 캐시 서비스 생성
func NewCacheService(cfg *config.Config, logger *utils.Logger, stationCache *cache.StationCacheService) *CacheService {
	return &CacheService{
		config:       cfg,
		logger:       logger,
		stationCache: stationCache,
	}
}

// GetStationCacheData 정류소 캐시 데이터 조회
func (s *CacheService) GetStationCacheData() (map[string]interface{}, error) {
	routes, stations := s.stationCache.GetCacheStatistics()

	// API1 노선별 정류소 정보
	api1Data := make(map[string]interface{})
	if len(s.config.API1Config.RouteIDs) > 0 {
		for _, routeID := range s.config.API1Config.RouteIDs {
			stationCount := s.stationCache.GetRouteStationCount(routeID)
			api1Data[routeID] = map[string]interface{}{
				"stationCount": stationCount,
				"type":         "API1",
				"format":       "숫자형",
				"lastUpdated":  time.Now().Add(-time.Duration(len(routeID)) * time.Minute), // 모의 데이터
			}
		}
	}

	// API2 노선별 정류소 정보
	api2Data := make(map[string]interface{})
	if len(s.config.API2Config.RouteIDs) > 0 {
		for _, routeID := range s.config.API2Config.RouteIDs {
			stationCount := s.stationCache.GetRouteStationCount(routeID)
			api2Data[routeID] = map[string]interface{}{
				"stationCount": stationCount,
				"type":         "API2",
				"format":       "GGB형식",
				"lastUpdated":  time.Now().Add(-time.Duration(len(routeID)) * time.Minute), // 모의 데이터
			}
		}
	}

	cacheData := map[string]interface{}{
		"summary": map[string]interface{}{
			"totalRoutes":   routes,
			"totalStations": stations,
			"api1Count":     len(s.config.API1Config.RouteIDs),
			"api2Count":     len(s.config.API2Config.RouteIDs),
			"lastUpdate":    time.Now(),
		},
		"detailedData": map[string]interface{}{
			"api1Routes": api1Data,
			"api2Routes": api2Data,
		},
		"routeLists": map[string]interface{}{
			"api1RouteIDs": s.config.API1Config.RouteIDs,
			"api2RouteIDs": s.config.API2Config.RouteIDs,
		},
		"performance": map[string]interface{}{
			"hitRate":       92.5,  // 모의 캐시 히트율
			"missRate":      7.5,   // 모의 캐시 미스율
			"averageLookup": "2ms", // 모의 평균 조회 시간
		},
	}

	return cacheData, nil
}

// GetRouteStationData 특정 노선의 정류소 데이터 조회
func (s *CacheService) GetRouteStationData(routeID string) (map[string]interface{}, error) {
	stationCount := s.stationCache.GetRouteStationCount(routeID)

	if stationCount == 0 {
		return nil, fmt.Errorf("노선 %s의 정류소 정보를 찾을 수 없습니다", routeID)
	}

	// 노선 타입 확인
	routeType := "unknown"
	format := "unknown"
	
	// API1 노선인지 확인
	for _, api1RouteID := range s.config.API1Config.RouteIDs {
		if api1RouteID == routeID {
			routeType = "API1"
			format = "숫자형"
			break
		}
	}
	
	// API2 노선인지 확인
	if routeType == "unknown" {
		for _, api2RouteID := range s.config.API2Config.RouteIDs {
			if api2RouteID == routeID {
				routeType = "API2"
				format = "GGB형식"
				break
			}
		}
	}

	stationData := map[string]interface{}{
		"routeId":      routeID,
		"stationCount": stationCount,
		"routeType":    routeType,
		"format":       format,
		"lastUpdate":   time.Now().Add(-10 * time.Minute), // 모의 마지막 업데이트 시간
		"status":       "active",
		"metadata": map[string]interface{}{
			"cacheHit":     true,
			"dataSource":   routeType,
			"refreshable":  true,
		},
	}

	return stationData, nil
}

// ReloadStationCache 정류소 캐시 새로고침
func (s *CacheService) ReloadStationCache() (map[string]interface{}, error) {
	startTime := time.Now()
	s.logger.Info("정류소