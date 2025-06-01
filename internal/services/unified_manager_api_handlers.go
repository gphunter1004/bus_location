// internal/services/unified_manager_api_handlers.go - RouteId 보장 로직
package services

import (
	"time"

	"bus-tracker/internal/models"
)

// UpdateAPI1Data API1 데이터 업데이트 처리 (RouteId 보장)
func (udm *UnifiedDataManagerWithDuplicateCheck) UpdateAPI1Data(busLocations []models.BusLocation) {
	if udm.isFirstRun {
		udm.loadRecentESDataForFirstRun()
	}

	udm.mutex.Lock()
	defer udm.mutex.Unlock()

	now := time.Now()
	var changedBuses []models.BusLocation
	isFirstProcessing := len(udm.dataStore) == 0

	for _, bus := range busLocations {
		plateNo := bus.PlateNo
		if plateNo == "" {
			continue
		}

		// RouteId 검증 (API1은 항상 유효해야 함)
		if bus.RouteId == 0 {
			udm.logger.Errorf("API1 데이터에 유효하지 않은 RouteId - 차량: %s, 건너뛰기", plateNo)
			continue
		}

		// 첫 실행 시 중복 체크
		if isFirstProcessing && udm.isDuplicateDataForFirstRun(plateNo, bus) {
			udm.updateInternalStateAPI1(plateNo, bus, now)
			continue
		}

		// 통합 데이터 가져오기 또는 생성 (RouteId 보장)
		unified := udm.getOrCreateUnifiedData(plateNo, bus.RouteId, now)

		// API1 데이터 업데이트
		unified.API1Data = &API1BusInfo{
			VehId:         bus.VehId,
			StationId:     bus.StationId,
			StationSeq:    bus.StationSeq,
			Crowded:       bus.Crowded,
			RemainSeatCnt: bus.RemainSeatCnt,
			StateCd:       bus.StateCd,
			LowPlate:      bus.LowPlate,
			RouteTypeCd:   bus.RouteTypeCd,
			TaglessCd:     bus.TaglessCd,
			UpdateTime:    now,
		}

		unified.LastAPI1Update = now
		unified.LastUpdate = now

		// API2 정보가 없는 경우에만 API1 정보로 현재 위치 업데이트
		if unified.API2Data == nil {
			unified.CurrentStationSeq = bus.StationSeq
			unified.CurrentStationId = bus.StationId
		}

		if !containsString(unified.DataSources, "api1") {
			unified.DataSources = append(unified.DataSources, "api1")
		}

		// 트래킹 처리 (RouteId 기반)
		cacheKey := bus.GetCacheKey()
		stationOrder := bus.GetStationOrder()

		changed, tripNumber := udm.busTracker.IsStationChanged(plateNo, int64(stationOrder), cacheKey, bus.TotalStations)
		if changed {
			unified.TripNumber = tripNumber

			// 통합 데이터 생성하여 ES 전송
			finalData := udm.buildUnifiedData(unified)
			if finalData != nil {
				unified.FinalData = finalData
				changedBuses = append(changedBuses, *finalData)
			}
		} else {
			udm.busTracker.UpdateLastSeenTime(plateNo)
		}
	}

	if len(changedBuses) > 0 {
		udm.sendChangedBusesToElasticsearch(changedBuses, "API1")
	}
}

// UpdateAPI2Data API2 데이터 업데이트 처리 (RouteId 보장)
func (udm *UnifiedDataManagerWithDuplicateCheck) UpdateAPI2Data(busLocations []models.BusLocation) {
	if udm.isFirstRun {
		udm.loadRecentESDataForFirstRun()
	}

	udm.mutex.Lock()
	defer udm.mutex.Unlock()

	now := time.Now()
	var changedBuses []models.BusLocation
	isFirstProcessing := len(udm.dataStore) == 0

	for _, bus := range busLocations {
		plateNo := bus.PlateNo
		if plateNo == "" {
			continue
		}

		// 🔧 RouteId 검증 (API2에서 추출되었는지 확인)
		if bus.RouteId == 0 {
			udm.logger.Warnf("API2 데이터에서 RouteId 추출 실패 - 차량: %s, RouteNm: %s, 건너뛰기", plateNo, bus.RouteNm)
			continue
		}

		// 첫 실행 시 중복 체크
		if isFirstProcessing && udm.isDuplicateDataForFirstRun(plateNo, bus) {
			udm.updateInternalStateAPI2(plateNo, bus, now)
			continue
		}

		// 🔧 통합 데이터 가져오기 또는 생성 (RouteId 보장)
		unified := udm.getOrCreateUnifiedData(plateNo, bus.RouteId, now)

		// 🔧 API2 데이터 업데이트
		unified.API2Data = &API2BusInfo{
			NodeId:     bus.NodeId,
			NodeNm:     bus.NodeNm,
			NodeOrd:    bus.NodeOrd,
			RouteNm:    bus.RouteNm,
			GpsLati:    bus.GpsLati,
			GpsLong:    bus.GpsLong,
			UpdateTime: now,
		}

		// API2 원본 노선번호 저장
		if bus.RouteNm != "" && bus.RouteNm != "0" {
			unified.RouteNm = bus.RouteNm
		}

		unified.LastAPI2Update = now
		unified.LastUpdate = now

		// API2 정보로 현재 위치 업데이트 (우선)
		unified.CurrentStationSeq = bus.NodeOrd
		unified.CurrentNodeNm = bus.NodeNm
		unified.CurrentNodeId = bus.NodeId

		if bus.StationId > 0 {
			unified.CurrentStationId = bus.StationId
		}

		if !containsString(unified.DataSources, "api2") {
			unified.DataSources = append(unified.DataSources, "api2")
		}

		// 🔧 트래킹 처리 (RouteId 기반)
		cacheKey := bus.GetCacheKey()
		stationOrder := bus.GetStationOrder()

		changed, tripNumber := udm.busTracker.IsStationChanged(plateNo, int64(stationOrder), cacheKey, bus.TotalStations)
		if changed {
			unified.TripNumber = tripNumber

			// 🔧 통합 데이터 생성하여 ES 전송
			finalData := udm.buildUnifiedData(unified)
			if finalData != nil {
				unified.FinalData = finalData
				changedBuses = append(changedBuses, *finalData)
			}
		} else {
			udm.busTracker.UpdateLastSeenTime(plateNo)
		}
	}

	if len(changedBuses) > 0 {
		udm.sendChangedBusesToElasticsearch(changedBuses, "API2")
	}
}

// updateInternalStateAPI1 API1 내부 상태만 업데이트 (중복 체크용)
func (udm *UnifiedDataManagerWithDuplicateCheck) updateInternalStateAPI1(plateNo string, bus models.BusLocation, now time.Time) {
	// 🔧 RouteId 검증
	if bus.RouteId == 0 {
		udm.logger.Errorf("API1 중복 체크 중 유효하지 않은 RouteId - 차량: %s", plateNo)
		return
	}

	unified := udm.getOrCreateUnifiedData(plateNo, bus.RouteId, now)

	unified.API1Data = &API1BusInfo{
		VehId:         bus.VehId,
		StationId:     bus.StationId,
		StationSeq:    bus.StationSeq,
		Crowded:       bus.Crowded,
		RemainSeatCnt: bus.RemainSeatCnt,
		StateCd:       bus.StateCd,
		LowPlate:      bus.LowPlate,
		RouteTypeCd:   bus.RouteTypeCd,
		TaglessCd:     bus.TaglessCd,
		UpdateTime:    now,
	}

	unified.LastAPI1Update = now
	unified.CurrentStationSeq = bus.StationSeq
	unified.CurrentStationId = bus.StationId

	if !containsString(unified.DataSources, "api1") {
		unified.DataSources = append(unified.DataSources, "api1")
	}

	udm.busTracker.UpdateLastSeenTime(plateNo)
}

// updateInternalStateAPI2 API2 내부 상태만 업데이트 (중복 체크용)
func (udm *UnifiedDataManagerWithDuplicateCheck) updateInternalStateAPI2(plateNo string, bus models.BusLocation, now time.Time) {
	// 🔧 RouteId 검증
	if bus.RouteId == 0 {
		udm.logger.Errorf("API2 중복 체크 중 유효하지 않은 RouteId - 차량: %s, RouteNm: %s", plateNo, bus.RouteNm)
		return
	}

	unified := udm.getOrCreateUnifiedData(plateNo, bus.RouteId, now)

	unified.API2Data = &API2BusInfo{
		NodeId:     bus.NodeId,
		NodeNm:     bus.NodeNm,
		NodeOrd:    bus.NodeOrd,
		RouteNm:    bus.RouteNm,
		GpsLati:    bus.GpsLati,
		GpsLong:    bus.GpsLong,
		UpdateTime: now,
	}

	if bus.RouteNm != "" && bus.RouteNm != "0" {
		unified.RouteNm = bus.RouteNm
	}

	unified.LastAPI2Update = now
	unified.CurrentStationSeq = bus.NodeOrd
	unified.CurrentNodeNm = bus.NodeNm
	unified.CurrentNodeId = bus.NodeId

	if bus.StationId > 0 {
		unified.CurrentStationId = bus.StationId
	}

	if !containsString(unified.DataSources, "api2") {
		unified.DataSources = append(unified.DataSources, "api2")
	}

	udm.busTracker.UpdateLastSeenTime(plateNo)
}
