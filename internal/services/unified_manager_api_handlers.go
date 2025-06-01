// internal/services/unified_manager_api_handlers.go - 단순화 버전
package services

import (
	"bus-tracker/internal/models"
)

// 단순화된 Redis 중심 구조에서는 API 핸들러가 통합 매니저의 메인 메서드로 통합됨
// UpdateAPI1Data와 UpdateAPI2Data가 이미 unified_manager.go에 구현되어 있음

// 추가 헬퍼 함수들

// validateBusData 버스 데이터 유효성 검증
func (udm *SimplifiedUnifiedDataManager) validateBusData(bus models.BusLocation, source string) bool {
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
func (udm *SimplifiedUnifiedDataManager) enrichBusDataFromCache(bus *models.BusLocation, source string) {
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
