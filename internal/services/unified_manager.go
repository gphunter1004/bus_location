package services

import (
	"sync"
	"time"

	"bus-tracker/internal/models"
	"bus-tracker/internal/services/cache"
	"bus-tracker/internal/services/storage"
	"bus-tracker/internal/services/tracker"
	"bus-tracker/internal/utils"
)

// UnifiedBusData 통합 버스 데이터 구조체
type UnifiedBusData struct {
	PlateNo           string              `json:"plateNo"`
	RouteId           int64               `json:"routeId"`
	RouteNm           string              `json:"routeNm"` // API2 노선번호 추가
	LastUpdate        time.Time           `json:"lastUpdate"`
	CurrentStationSeq int                 `json:"currentStationSeq"`
	CurrentStationId  int64               `json:"currentStationId"`
	CurrentNodeNm     string              `json:"currentNodeNm"`
	CurrentNodeId     string              `json:"currentNodeId"`
	API1Data          *API1BusInfo        `json:"api1Data,omitempty"`
	API2Data          *API2BusInfo        `json:"api2Data,omitempty"`
	FinalData         *models.BusLocation `json:"finalData"`
	DataSources       []string            `json:"dataSources"`
	LastAPI1Update    time.Time           `json:"lastAPI1Update,omitempty"`
	LastAPI2Update    time.Time           `json:"lastAPI2Update,omitempty"`
	TripNumber        int                 `json:"tripNumber"` // 운행 차수 추가
}

type API1BusInfo struct {
	VehId         int64     `json:"vehId"`
	StationId     int64     `json:"stationId"`
	StationSeq    int       `json:"stationSeq"`
	Crowded       int       `json:"crowded"`
	RemainSeatCnt int       `json:"remainSeatCnt"`
	StateCd       int       `json:"stateCd"`
	LowPlate      int       `json:"lowPlate"`
	RouteTypeCd   int       `json:"routeTypeCd"`
	TaglessCd     int       `json:"taglessCd"`
	UpdateTime    time.Time `json:"updateTime"`
}

type API2BusInfo struct {
	NodeId     string    `json:"nodeId"`
	NodeNm     string    `json:"nodeNm"`
	NodeOrd    int       `json:"nodeOrd"`
	RouteNm    string    `json:"routeNm"` // 노선번호 추가
	GpsLati    float64   `json:"gpsLati"`
	GpsLong    float64   `json:"gpsLong"`
	UpdateTime time.Time `json:"updateTime"`
}

type UnifiedDataManager struct {
	dataStore    map[string]*UnifiedBusData
	mutex        sync.RWMutex
	logger       *utils.Logger
	busTracker   *tracker.BusTracker
	stationCache *cache.StationCacheService
	esService    *storage.ElasticsearchService
	indexName    string
}

// NewUnifiedDataManager 생성자
func NewUnifiedDataManager(logger *utils.Logger, busTracker *tracker.BusTracker,
	stationCache *cache.StationCacheService,
	esService *storage.ElasticsearchService, indexName string) *UnifiedDataManager {

	return &UnifiedDataManager{
		dataStore:    make(map[string]*UnifiedBusData),
		logger:       logger,
		busTracker:   busTracker,
		stationCache: stationCache,
		esService:    esService,
		indexName:    indexName,
	}
}

func (udm *UnifiedDataManager) UpdateAPI1Data(busLocations []models.BusLocation) {
	udm.mutex.Lock()
	defer udm.mutex.Unlock()

	now := time.Now()
	var changedBuses []models.BusLocation
	isFirstRun := len(udm.dataStore) == 0

	for _, bus := range busLocations {
		plateNo := bus.PlateNo
		if plateNo == "" {
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

		if currentSeq > 0 && newSeq < currentSeq && !isFirstRun {
			shouldUpdateStationInfo = false
		} else if currentSeq > 0 && newSeq == currentSeq && !isFirstRun {
			shouldUpdateStationInfo = false
		}

		// API1 데이터 업데이트 (API2 정보를 덮어쓰지 않음)
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

			if isFirstRun {
				changed, tripNumber := udm.busTracker.IsStationChanged(plateNo, int64(newSeq), routeNm, bus.TotalStations)
				unified.TripNumber = tripNumber
				if changed {
					finalData := udm.mergeDataForBus(unified)
					if finalData != nil {
						unified.FinalData = finalData
						changedBuses = append(changedBuses, *finalData)
					}
				}
			} else {
				changed, tripNumber := udm.busTracker.IsStationChanged(plateNo, int64(newSeq), routeNm, bus.TotalStations)
				if changed {
					unified.TripNumber = tripNumber
					finalData := udm.mergeDataForBus(unified)
					if finalData != nil {
						unified.FinalData = finalData
						changedBuses = append(changedBuses, *finalData)
					}
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

func (udm *UnifiedDataManager) UpdateAPI2Data(busLocations []models.BusLocation) {
	udm.mutex.Lock()
	defer udm.mutex.Unlock()

	now := time.Now()
	var changedBuses []models.BusLocation
	isFirstRun := len(udm.dataStore) == 0

	for _, bus := range busLocations {
		plateNo := bus.PlateNo
		if plateNo == "" {
			continue
		}

		unified, exists := udm.dataStore[plateNo]
		if !exists {
			unified = &UnifiedBusData{
				PlateNo:           plateNo,
				RouteId:           bus.RouteId,
				RouteNm:           bus.RouteNm, // API2의 RouteNm 사용
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

		if currentSeq > 0 && newSeq < currentSeq && !isFirstRun {
			shouldUpdateStationInfo = false
		} else if currentSeq > 0 && newSeq == currentSeq && !isFirstRun {
			shouldUpdateStationInfo = false
		}

		// API2 데이터 업데이트 (우선 정보)
		unified.API2Data = &API2BusInfo{
			NodeId:     bus.NodeId,
			NodeNm:     bus.NodeNm,
			NodeOrd:    bus.NodeOrd,
			RouteNm:    bus.RouteNm,
			GpsLati:    bus.GpsLati,
			GpsLong:    bus.GpsLong,
			UpdateTime: now,
		}

		// RouteNm 업데이트 (API2 우선)
		if bus.RouteNm != "" {
			unified.RouteNm = bus.RouteNm
		}

		// StationId 업데이트 (API2에서 올바르게 설정된 값 사용)
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

			if isFirstRun {
				changed, tripNumber := udm.busTracker.IsStationChanged(plateNo, int64(newSeq), routeNm, bus.TotalStations)
				unified.TripNumber = tripNumber
				if changed {
					finalData := udm.mergeDataForBus(unified)
					if finalData != nil {
						unified.FinalData = finalData
						changedBuses = append(changedBuses, *finalData)
					}
				}
			} else {
				changed, tripNumber := udm.busTracker.IsStationChanged(plateNo, int64(newSeq), routeNm, bus.TotalStations)
				if changed {
					unified.TripNumber = tripNumber
					finalData := udm.mergeDataForBus(unified)
					if finalData != nil {
						unified.FinalData = finalData
						changedBuses = append(changedBuses, *finalData)
					}
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

func (udm *UnifiedDataManager) sendChangedBusesToElasticsearch(changedBuses []models.BusLocation, source string) {
	if udm.esService == nil {
		return
	}

	for i, bus := range changedBuses {
		udm.logger.Infof("ES 전송 [%d/%d] - 차량: %s, 노선: %s, 정류장: %s (%s), 차수: %d",
			i+1, len(changedBuses), bus.PlateNo, bus.RouteNm, bus.NodeNm, bus.NodeId, bus.TripNumber)
	}

	if err := udm.esService.BulkSendBusLocations(udm.indexName, changedBuses); err != nil {
		udm.logger.Errorf("ES 전송 실패 (%s): %v", source, err)
		return
	}
}

func (udm *UnifiedDataManager) mergeDataForBus(unified *UnifiedBusData) *models.BusLocation {
	if unified.API1Data == nil && unified.API2Data == nil {
		return nil
	}

	final := &models.BusLocation{
		PlateNo:    unified.PlateNo,
		RouteId:    unified.RouteId,
		RouteNm:    unified.RouteNm,          // API2 우선
		StationId:  unified.CurrentStationId, // 통합된 StationId 사용
		Timestamp:  time.Now().Format(time.RFC3339),
		StationSeq: unified.CurrentStationSeq,
		NodeNm:     unified.CurrentNodeNm,
		NodeId:     unified.CurrentNodeId,
		NodeOrd:    unified.CurrentStationSeq,
		TripNumber: unified.TripNumber,
	}

	// API2 정보 우선 적용
	if unified.API2Data != nil {
		final.GpsLati = unified.API2Data.GpsLati
		final.GpsLong = unified.API2Data.GpsLong
		final.NodeNm = unified.API2Data.NodeNm
		final.NodeId = unified.API2Data.NodeId
		final.StationSeq = unified.API2Data.NodeOrd
		final.NodeOrd = unified.API2Data.NodeOrd
		final.RouteNm = unified.API2Data.RouteNm
	}

	// API1 정보는 API2에서 제공하지 않는 정보만 추가
	if unified.API1Data != nil {
		final.VehId = unified.API1Data.VehId
		final.Crowded = unified.API1Data.Crowded
		final.RemainSeatCnt = unified.API1Data.RemainSeatCnt
		final.StateCd = unified.API1Data.StateCd
		final.LowPlate = unified.API1Data.LowPlate
		final.RouteTypeCd = unified.API1Data.RouteTypeCd
		final.TaglessCd = unified.API1Data.TaglessCd

		// API2 정보가 없는 경우에만 API1의 StationId 사용
		if unified.API2Data == nil {
			final.StationId = unified.API1Data.StationId
			final.StationSeq = unified.API1Data.StationSeq
			final.NodeOrd = unified.API1Data.StationSeq
		}
	}

	// 전체 정류소 개수 설정
	if udm.stationCache != nil {
		routeKey := final.RouteNm
		if routeKey == "" {
			routeKey = final.GetRouteIDString()
		}
		final.TotalStations = udm.stationCache.GetRouteStationCount(routeKey)
	}

	return final
}

func (udm *UnifiedDataManager) CleanupOldData(maxAge time.Duration) int {
	udm.mutex.Lock()
	defer udm.mutex.Unlock()

	now := time.Now()
	var removedPlates []string

	for plateNo, data := range udm.dataStore {
		if now.Sub(data.LastUpdate) > maxAge {
			removedPlates = append(removedPlates, plateNo)
		}
	}

	for _, plateNo := range removedPlates {
		delete(udm.dataStore, plateNo)
		udm.busTracker.RemoveFromTracking(plateNo)
	}

	return len(removedPlates)
}

func (udm *UnifiedDataManager) GetStatistics() (int, int, int, int) {
	udm.mutex.RLock()
	defer udm.mutex.RUnlock()

	totalBuses := len(udm.dataStore)
	api1Only := 0
	api2Only := 0
	both := 0

	for _, data := range udm.dataStore {
		hasAPI1 := data.API1Data != nil
		hasAPI2 := data.API2Data != nil

		if hasAPI1 && hasAPI2 {
			both++
		} else if hasAPI1 {
			api1Only++
		} else if hasAPI2 {
			api2Only++
		}
	}

	return totalBuses, api1Only, api2Only, both
}

func containsString(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
