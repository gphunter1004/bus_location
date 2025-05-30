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
	GpsLati    float64   `json:"gpsLati"`
	GpsLong    float64   `json:"gpsLong"`
	UpdateTime time.Time `json:"updateTime"`
}

type UnifiedDataManager struct {
	dataStore    map[string]*UnifiedBusData
	mutex        sync.RWMutex
	logger       *utils.Logger
	busTracker   *tracker.BusTracker
	stationCache *cache.StationCacheService // 🔧 UnifiedStationCacheService → StationCacheService 변경
	esService    *storage.ElasticsearchService
	indexName    string
}

// NewUnifiedDataManager 생성자 수정
func NewUnifiedDataManager(logger *utils.Logger, busTracker *tracker.BusTracker,
	stationCache *cache.StationCacheService, // 🔧 파라미터 타입 변경
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
	updatedCount := 0
	stationIgnoredCount := 0
	var changedBuses []models.BusLocation
	isFirstRun := len(udm.dataStore) == 0 // 🔧 첫 실행 감지

	udm.logger.Infof("API1 데이터 업데이트 시작 - 수신된 버스: %d대", len(busLocations))
	if isFirstRun {
		udm.logger.Info("🆕 첫 실행 감지 - 모든 버스를 ES에 전송합니다")
	}

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

		if currentSeq > 0 && newSeq < currentSeq {
			udm.logger.Warnf("API1 정류장 정보 무시 - 차량번호: %s, 현재 seq: %d > 새 seq: %d",
				plateNo, currentSeq, newSeq)
			shouldUpdateStationInfo = false
			stationIgnoredCount++
		} else if currentSeq > 0 && newSeq == currentSeq && !isFirstRun {
			shouldUpdateStationInfo = false
		}

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
			unified.CurrentStationSeq = newSeq
			unified.CurrentStationId = bus.StationId

			if udm.stationCache != nil {
				if stationInfo, exists := udm.stationCache.GetStationInfo(bus.GetRouteIDString(), newSeq); exists {
					unified.CurrentNodeNm = stationInfo.NodeNm
					unified.CurrentNodeId = stationInfo.NodeId
				}
			}

			// 🔧 첫 실행이거나 정류장이 변경된 경우 ES 전송
			if isFirstRun || udm.busTracker.IsStationChanged(plateNo, int64(newSeq)) {
				finalData := udm.mergeDataForBus(unified)
				if finalData != nil {
					unified.FinalData = finalData
					changedBuses = append(changedBuses, *finalData)
				}
			}
		} else {
			udm.busTracker.UpdateLastSeenTime(plateNo)
		}

		updatedCount++
	}

	udm.logger.Infof("API1 데이터 업데이트 완료: 처리=%d대, 정류장변경=%d대, 정류장정보무시=%d대",
		updatedCount, len(changedBuses), stationIgnoredCount)

	if len(changedBuses) > 0 {
		udm.sendChangedBusesToElasticsearch(changedBuses, "API1")
	}
}

func (udm *UnifiedDataManager) UpdateAPI2Data(busLocations []models.BusLocation) {
	udm.mutex.Lock()
	defer udm.mutex.Unlock()

	now := time.Now()
	updatedCount := 0
	stationIgnoredCount := 0
	var changedBuses []models.BusLocation
	isFirstRun := len(udm.dataStore) == 0 // 🔧 첫 실행 감지

	udm.logger.Infof("API2 데이터 업데이트 시작 - 수신된 버스: %d대", len(busLocations))
	if isFirstRun {
		udm.logger.Info("🆕 첫 실행 감지 - 모든 버스를 ES에 전송합니다")
	}

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

		if currentSeq > 0 && newSeq < currentSeq {
			udm.logger.Warnf("API2 정류장 정보 무시 - 차량번호: %s, 현재 seq: %d > 새 seq: %d",
				plateNo, currentSeq, newSeq)
			shouldUpdateStationInfo = false
			stationIgnoredCount++
		} else if currentSeq > 0 && newSeq == currentSeq && !isFirstRun {
			shouldUpdateStationInfo = false
		}

		unified.API2Data = &API2BusInfo{
			NodeId:     bus.NodeId,
			NodeNm:     bus.NodeNm,
			NodeOrd:    bus.NodeOrd,
			GpsLati:    bus.GpsLati,
			GpsLong:    bus.GpsLong,
			UpdateTime: now,
		}

		unified.LastAPI2Update = now
		unified.LastUpdate = now

		if !containsString(unified.DataSources, "api2") {
			unified.DataSources = append(unified.DataSources, "api2")
		}

		if shouldUpdateStationInfo {
			unified.CurrentStationSeq = newSeq
			unified.CurrentNodeNm = bus.NodeNm
			unified.CurrentNodeId = bus.NodeId

			// 🔧 첫 실행이거나 정류장이 변경된 경우 ES 전송
			if isFirstRun || udm.busTracker.IsStationChanged(plateNo, int64(newSeq)) {
				finalData := udm.mergeDataForBus(unified)
				if finalData != nil {
					unified.FinalData = finalData
					changedBuses = append(changedBuses, *finalData)
				}
			}
		} else {
			udm.busTracker.UpdateLastSeenTime(plateNo)
		}

		updatedCount++
	}

	udm.logger.Infof("API2 데이터 업데이트 완료: 처리=%d대, 정류장변경=%d대, 정류장정보무시=%d대",
		updatedCount, len(changedBuses), stationIgnoredCount)

	if len(changedBuses) > 0 {
		udm.sendChangedBusesToElasticsearch(changedBuses, "API2")
	}
}

func (udm *UnifiedDataManager) sendChangedBusesToElasticsearch(changedBuses []models.BusLocation, source string) {
	if udm.esService == nil {
		return
	}

	udm.logger.Infof("=== ES 전송 시작 (%s: %d대) ===", source, len(changedBuses))

	for i, bus := range changedBuses {
		udm.logger.Infof("ES 전송 [%d/%d] - 차량: %s, 정류장: %s (%s)",
			i+1, len(changedBuses), bus.PlateNo, bus.NodeNm, bus.NodeId)
	}

	startTime := time.Now()
	if err := udm.esService.BulkSendBusLocations(udm.indexName, changedBuses); err != nil {
		udm.logger.Errorf("ES 전송 실패 (%s): %v", source, err)
		return
	}

	duration := time.Since(startTime)
	udm.logger.Infof("=== ES 전송 완료 (%s) - %d대, 소요시간: %v ===", source, len(changedBuses), duration)
}

func (udm *UnifiedDataManager) mergeDataForBus(unified *UnifiedBusData) *models.BusLocation {
	if unified.API1Data == nil && unified.API2Data == nil {
		return nil
	}

	final := &models.BusLocation{
		PlateNo:    unified.PlateNo,
		RouteId:    unified.RouteId,
		Timestamp:  time.Now().Format(time.RFC3339),
		StationSeq: unified.CurrentStationSeq,
		NodeNm:     unified.CurrentNodeNm,
		NodeId:     unified.CurrentNodeId,
		NodeOrd:    unified.CurrentStationSeq,
	}

	if unified.API1Data != nil {
		final.VehId = unified.API1Data.VehId
		final.StationId = unified.API1Data.StationId
		final.Crowded = unified.API1Data.Crowded
		final.RemainSeatCnt = unified.API1Data.RemainSeatCnt
		final.StateCd = unified.API1Data.StateCd
		final.LowPlate = unified.API1Data.LowPlate
		final.RouteTypeCd = unified.API1Data.RouteTypeCd
		final.TaglessCd = unified.API1Data.TaglessCd
	}

	if unified.API2Data != nil {
		final.GpsLati = unified.API2Data.GpsLati
		final.GpsLong = unified.API2Data.GpsLong

		if unified.API1Data == nil {
			final.NodeNm = unified.API2Data.NodeNm
			final.NodeId = unified.API2Data.NodeId
			final.StationSeq = unified.API2Data.NodeOrd
			final.NodeOrd = unified.API2Data.NodeOrd
		}
	}

	// 🔧 통합된 StationCacheService 사용하여 전체 정류소 개수 설정
	if udm.stationCache != nil {
		final.TotalStations = udm.stationCache.GetRouteStationCount(final.GetRouteIDString())
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

	if len(removedPlates) > 0 {
		udm.logger.Infof("통합 데이터 정리 완료: %d개 버스 데이터 제거", len(removedPlates))
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
