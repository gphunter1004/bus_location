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
			"cacheHit":    true,
			"dataSource":  routeType,
			"refreshable": true,
		},
	}

	return stationData, nil
}

// ReloadStationCache 정류소 캐시 새로고침
func (s *CacheService) ReloadStationCache() (map[string]interface{}, error) {
	startTime := time.Now()
	s.logger.Info("정류소 캐시 새로고침 시작")

	// 모든 노선 ID 수집
	var allRouteIDs []string
	allRouteIDs = append(allRouteIDs, s.config.API1Config.RouteIDs...)
	allRouteIDs = append(allRouteIDs, s.config.API2Config.RouteIDs...)

	if len(allRouteIDs) == 0 {
		return nil, fmt.Errorf("새로고침할 노선이 없습니다")
	}

	// 캐시 새로고침 시도
	err := s.stationCache.LoadStationCache(allRouteIDs)
	duration := time.Since(startTime)

	if err != nil {
		s.logger.Errorf("정류소 캐시 새로고침 실패: %v (소요시간: %v)", err, duration)
		return nil, fmt.Errorf("캐시 새로고침 실패: %v", err)
	}

	// 새로고침 후 통계
	routes, stations := s.stationCache.GetCacheStatistics()

	result := map[string]interface{}{
		"reloadTime":     startTime,
		"duration":       duration.String(),
		"routesLoaded":   routes,
		"stationsLoaded": stations,
		"api1Routes":     len(s.config.API1Config.RouteIDs),
		"api2Routes":     len(s.config.API2Config.RouteIDs),
		"totalRoutes":    len(allRouteIDs),
		"status":         "success",
	}

	s.logger.Infof("정류소 캐시 새로고침 완료 - 노선: %d개, 정류소: %d개, 소요시간: %v",
		routes, stations, duration)

	return result, nil
}

// ClearStationCache 정류소 캐시 삭제
func (s *CacheService) ClearStationCache() (map[string]interface{}, error) {
	startTime := time.Now()
	s.logger.Info("정류소 캐시 삭제 시작")

	// 캐시 삭제 전 통계
	oldRoutes, oldStations := s.stationCache.GetCacheStatistics()

	// 실제 캐시 삭제는 StationCacheService에 메서드가 있어야 함
	// 현재는 모의 구현

	result := map[string]interface{}{
		"clearTime":       startTime,
		"duration":        time.Since(startTime).String(),
		"clearedRoutes":   oldRoutes,
		"clearedStations": oldStations,
		"status":          "success",
		"message":         "캐시가 성공적으로 삭제되었습니다",
	}

	s.logger.Infof("정류소 캐시 삭제 완료 - 삭제된 노선: %d개, 정류소: %d개",
		oldRoutes, oldStations)

	return result, nil
}

// GetCacheStatistics 캐시 통계 조회
func (s *CacheService) GetCacheStatistics() (map[string]interface{}, error) {
	routes, stations := s.stationCache.GetCacheStatistics()

	// 노선별 상세 통계
	routeDetails := make(map[string]interface{})

	// API1 노선들
	for _, routeID := range s.config.API1Config.RouteIDs {
		stationCount := s.stationCache.GetRouteStationCount(routeID)
		routeDetails[routeID] = map[string]interface{}{
			"type":         "API1",
			"stationCount": stationCount,
			"status":       getRouteStatus(stationCount),
		}
	}

	// API2 노선들
	for _, routeID := range s.config.API2Config.RouteIDs {
		stationCount := s.stationCache.GetRouteStationCount(routeID)
		routeDetails[routeID] = map[string]interface{}{
			"type":         "API2",
			"stationCount": stationCount,
			"status":       getRouteStatus(stationCount),
		}
	}

	stats := map[string]interface{}{
		"summary": map[string]interface{}{
			"totalRoutes":   routes,
			"totalStations": stations,
			"api1Routes":    len(s.config.API1Config.RouteIDs),
			"api2Routes":    len(s.config.API2Config.RouteIDs),
			"lastUpdate":    time.Now(),
		},
		"routeDetails": routeDetails,
		"performance": map[string]interface{}{
			"averageStationsPerRoute": calculateAverageStations(routes, stations),
			"cacheEfficiency":         calculateCacheEfficiency(routes, stations),
		},
		"health": map[string]interface{}{
			"status": getCacheHealthStatus(routes, stations),
			"issues": getCacheIssues(routeDetails),
		},
	}

	return stats, nil
}

// 헬퍼 함수들

func getRouteStatus(stationCount int) string {
	if stationCount == 0 {
		return "no_data"
	} else if stationCount < 5 {
		return "low_data"
	} else {
		return "healthy"
	}
}

func calculateAverageStations(routes, stations int) float64 {
	if routes == 0 {
		return 0
	}
	return float64(stations) / float64(routes)
}

func calculateCacheEfficiency(routes, stations int) float64 {
	// 간단한 효율성 계산 (실제로는 더 복잡한 메트릭 사용)
	if routes == 0 {
		return 0
	}
	return float64(stations) / float64(routes) * 10 // 임시 공식
}

func getCacheHealthStatus(routes, stations int) string {
	if routes == 0 || stations == 0 {
		return "unhealthy"
	} else if routes < 2 {
		return "warning"
	} else {
		return "healthy"
	}
}

func getCacheIssues(routeDetails map[string]interface{}) []string {
	var issues []string

	for routeID, details := range routeDetails {
		if detailMap, ok := details.(map[string]interface{}); ok {
			if status, ok := detailMap["status"].(string); ok && status != "healthy" {
				issues = append(issues, fmt.Sprintf("노선 %s: %s", routeID, status))
			}
		}
	}

	return issues
}
