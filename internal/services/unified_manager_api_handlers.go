// internal/services/unified_manager_api_handlers.go - Redis 기반으로 단순화
package services

import (
	"bus-tracker/internal/models"
)

// Redis 기반 구조에서는 API 핸들러가 통합 매니저의 메인 메서드로 통합됨
// UpdateAPI1Data와 UpdateAPI2Data가 이미 unified_manager.go에 구현되어 있음

// 이 파일은 기존 호환성을 위해 유지되지만 실제 로직은 unified_manager.go로 이동됨

// 추가 헬퍼 함수들

// validateBusData 버스 데이터 유효성 검증
func (udm *UnifiedDataManagerWithRedis) validateBusData(bus models.BusLocation, source string) bool {
	// 필수 필드 검증
	if bus.PlateNo == "" {
		udm.logger.Warnf("%s 데이터 검증 실패: 차량번호 없음", source)
		return false
	}

	if bus.RouteId == 0 {
		udm.logger.Warnf("%s 데이터 검증 실패: RouteId 없음 - 차량: %s", source, bus.PlateNo)
		return false
	}

	// API2의 경우 추가 검증
	if source == "API2" {
		if bus.NodeOrd <= 0 && bus.StationSeq <= 0 {
			udm.logger.Warnf("%s 데이터 검증 실패: 정류장 순서 없음 - 차량: %s", source, bus.PlateNo)
			return false
		}

		// GPS 좌표 유효성 검증 (한국 좌표 범위)
		if bus.GpsLati < 33.0 || bus.GpsLati > 38.0 || bus.GpsLong < 124.0 || bus.GpsLong > 132.0 {
			udm.logger.Warnf("%s 데이터 검증 실패: GPS 좌표 이상 - 차량: %s (%.6f, %.6f)",
				source, bus.PlateNo, bus.GpsLati, bus.GpsLong)
			// GPS 좌표가 이상해도 데이터는 유지 (다른 정보가 유효하면)
		}
	}

	return true
}

// enrichBusDataFromCache 캐시에서 버스 데이터 보강
func (udm *UnifiedDataManagerWithRedis) enrichBusDataFromCache(bus *models.BusLocation, source string) {
	if udm.stationCache == nil {
		return
	}

	// 정류소 정보 보강
	udm.stationCache.EnrichBusLocationWithStationInfo(bus, bus.GetCacheKey())

	// 전체 정류소 개수 설정
	if bus.TotalStations == 0 {
		bus.TotalStations = udm.stationCache.GetRouteStationCount(bus.GetCacheKey())
	}

	udm.logger.Debugf("%s 데이터 보강 완료 - 차량: %s, 정류장: %s, 전체: %d개",
		source, bus.PlateNo, bus.NodeNm, bus.TotalStations)
}

// logBusProcessingStats 버스 처리 통계 로깅
func (udm *UnifiedDataManagerWithRedis) logBusProcessingStats(source string, total, valid, changed, esReady int) {
	udm.logger.Infof("%s 처리 통계 - 총: %d건, 유효: %d건, 변경: %d건, ES 전송: %d건",
		source, total, valid, changed, esReady)
}

// getDataQualityScore 데이터 품질 점수 계산
func (udm *UnifiedDataManagerWithRedis) getDataQualityScore(bus models.BusLocation, source string) int {
	score := 0

	// 기본 정보 (40점)
	if bus.PlateNo != "" {
		score += 10
	}
	if bus.RouteId > 0 {
		score += 10
	}
	if bus.RouteNm != "" {
		score += 10
	}
	if bus.TripNumber > 0 {
		score += 10
	}

	// 위치 정보 (40점)
	if bus.StationId > 0 || bus.StationSeq > 0 || bus.NodeOrd > 0 {
		score += 20
	}
	if bus.NodeNm != "" {
		score += 10
	}
	if bus.NodeId != "" {
		score += 10
	}

	// GPS 정보 (20점) - API2 전용
	if source == "API2" {
		if bus.GpsLati > 33.0 && bus.GpsLati < 38.0 {
			score += 10
		}
		if bus.GpsLong > 124.0 && bus.GpsLong < 132.0 {
			score += 10
		}
	} else {
		// API1은 GPS 없으므로 기본 점수
		score += 20
	}

	return score
}

// 사용되지 않는 기존 함수들 (호환성 유지)

// buildUnifiedData는 이제 Redis에서 직접 처리되므로 불필요
// getOrCreateUnifiedData는 Redis 매니저에서 처리
// containsString은 utils 패키지로 이동됨

// containsString 헬퍼 함수 (하위 호환성)
func containsString(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
