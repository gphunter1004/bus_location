// internal/services/unified_manager_api_handlers.go
package services

import (
	"time"

	"bus-tracker/internal/models"
)

// UpdateAPI1Data API1 데이터 업데이트 처리
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

		// 첫 실행 시 중복 체크
		if isFirstProcessing && udm.isDuplicateDataForFirstRun(plateNo, bus) {
			udm.updateInternalStateOnly(plateNo, bus, now, "API1")
			continue
		}

		unified, exists := udm.dataStore[plateNo]
		if !exists {
			unified = &UnifiedBusData{
				PlateNo:           plateNo,
				RouteId:           bus.RouteId,
				RouteNm:           bus.GetRouteIDString(),
				LastUpdate:        now,
				CurrentStationSeq: bus.StationSeq,
				CurrentStationId:  bus.StationId,
				DataSources:       []string{},
			}
			udm.dataStore[plateNo] = unified
		}

		shouldUpdateStationInfo := true
		newSeq := bus.StationSeq
		currentSeq := unified.CurrentStationSeq

		if currentSeq > 0 && newSeq < currentSeq && !isFirstProcessing {
			shouldUpdateStationInfo = false
		} else if currentSeq > 0 && newSeq == currentSeq && !isFirstProcessing {
			shouldUpdateStationInfo = false
		}

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

		if !containsString(unified.DataSources, "api1") {
			unified.DataSources = append(unified.DataSources, "api1")
		}

		if shouldUpdateStationInfo {
			// API2 정보가 없는 경우에만 API1 정보 사용
			if unified.API2Data == nil || unified.API2Data.NodeNm == "" {
				unified.CurrentStationSeq = newSeq
				unified.CurrentStationId = bus.StationId

				if udm.stationCache != nil {
					if stationInfo, exists := udm.stationCache.GetStationInfo(bus.GetRouteIDString(), newSeq); exists {
						unified.CurrentNodeNm = stationInfo.NodeNm
						unified.CurrentNodeId = stationInfo.NodeId
					}
				}
			}

			routeNm := unified.RouteNm
			if routeNm == "" {
				routeNm = bus.GetRouteIDString()
			}

			changed, tripNumber := udm.busTracker.IsStationChanged(plateNo, int64(newSeq), routeNm, bus.TotalStations)
			if changed {
				unified.TripNumber = tripNumber
				finalData := udm.mergeDataForBus(unified)
				if finalData != nil {
					unified.FinalData = finalData
					changedBuses = append(changedBuses, *finalData)
				}
			}
		} else {
			udm.busTracker.UpdateLastSeenTime(plateNo)
		}
	}

	if len(changedBuses) > 0 {
		udm.sendChangedBusesToElasticsearch(changedBuses, "API1")
	}
}

// UpdateAPI2Data API2 데이터 업데이트 처리
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

		// 첫 실행 시 중복 체크
		if isFirstProcessing && udm.isDuplicateDataForFirstRun(plateNo, bus) {
			udm.updateInternalStateOnly(plateNo, bus, now, "API2")
			continue
		}

		unified, exists := udm.dataStore[plateNo]
		if !exists {
			unified = &UnifiedBusData{
				PlateNo:           plateNo,
				RouteId:           bus.RouteId,
				RouteNm:           bus.RouteNm,
				LastUpdate:        now,
				CurrentStationSeq: bus.NodeOrd,
				CurrentNodeNm:     bus.NodeNm,
				CurrentNodeId:     bus.NodeId,
				DataSources:       []string{},
			}
			udm.dataStore[plateNo] = unified
		}

		shouldUpdateStationInfo := true
		newSeq := bus.NodeOrd
		currentSeq := unified.CurrentStationSeq

		if currentSeq > 0 && newSeq < currentSeq && !isFirstProcessing {
			shouldUpdateStationInfo = false
		} else if currentSeq > 0 && newSeq == currentSeq && !isFirstProcessing {
			shouldUpdateStationInfo = false
		}

		// API2 데이터 업데이트
		unified.API2Data = &API2BusInfo{
			NodeId:     bus.NodeId,
			NodeNm:     bus.NodeNm,
			NodeOrd:    bus.NodeOrd,
			RouteNm:    bus.RouteNm,
			GpsLati:    bus.GpsLati,
			GpsLong:    bus.GpsLong,
			UpdateTime: now,
		}

		if bus.RouteNm != "" {
			unified.RouteNm = bus.RouteNm
		}

		if bus.StationId > 0 {
			unified.CurrentStationId = bus.StationId
		}

		unified.LastAPI2Update = now
		unified.LastUpdate = now

		if !containsString(unified.DataSources, "api2") {
			unified.DataSources = append(unified.DataSources, "api2")
		}

		if shouldUpdateStationInfo {
			// API2 정보로 업데이트 (우선)
			unified.CurrentStationSeq = newSeq
			unified.CurrentNodeNm = bus.NodeNm
			unified.CurrentNodeId = bus.NodeId

			routeNm := unified.RouteNm
			if routeNm == "" {
				routeNm = bus.GetRouteIDString()
			}

			changed, tripNumber := udm.busTracker.IsStationChanged(plateNo, int64(newSeq), routeNm, bus.TotalStations)
			if changed {
				unified.TripNumber = tripNumber
				finalData := udm.mergeDataForBus(unified)
				if finalData != nil {
					unified.FinalData = finalData
					changedBuses = append(changedBuses, *finalData)
				}
			}
		} else {
			udm.busTracker.UpdateLastSeenTime(plateNo)
		}
	}

	if len(changedBuses) > 0 {
		udm.sendChangedBusesToElasticsearch(changedBuses, "API2")
	}
}
