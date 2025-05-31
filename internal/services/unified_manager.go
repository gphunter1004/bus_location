// internal/services/unified_manager.go
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

// UnifiedDataManagerInterface 통합 데이터 매니저 인터페이스
type UnifiedDataManagerInterface interface {
	UpdateAPI1Data(busLocations []models.BusLocation)
	UpdateAPI2Data(busLocations []models.BusLocation)
	CleanupOldData(maxAge time.Duration) int
	GetStatistics() (int, int, int, int)
}

// UnifiedDataManagerWithDuplicateCheck 중복 체크 기능이 추가된 통합 데이터 매니저
type UnifiedDataManagerWithDuplicateCheck struct {
	dataStore        map[string]*UnifiedBusData
	mutex            sync.RWMutex
	logger           *utils.Logger
	busTracker       *tracker.BusTrackerWithDuplicateCheck
	stationCache     *cache.StationCacheService
	esService        *storage.ElasticsearchService
	duplicateChecker *storage.ElasticsearchDuplicateChecker
	indexName        string
	isFirstRun       bool
	firstRunMutex    sync.Mutex
	recentESData     map[string]*storage.BusLastData
}

// NewUnifiedDataManagerWithDuplicateCheck 생성자
func NewUnifiedDataManagerWithDuplicateCheck(logger *utils.Logger,
	busTracker *tracker.BusTrackerWithDuplicateCheck,
	stationCache *cache.StationCacheService,
	esService *storage.ElasticsearchService,
	duplicateChecker *storage.ElasticsearchDuplicateChecker,
	indexName string) *UnifiedDataManagerWithDuplicateCheck {

	return &UnifiedDataManagerWithDuplicateCheck{
		dataStore:        make(map[string]*UnifiedBusData),
		logger:           logger,
		busTracker:       busTracker,
		stationCache:     stationCache,
		esService:        esService,
		duplicateChecker: duplicateChecker,
		indexName:        indexName,
		isFirstRun:       true,
		recentESData:     make(map[string]*storage.BusLastData),
	}
}

// loadRecentESDataForFirstRun 첫 실행 시 ES에서 최근 데이터 로드
func (udm *UnifiedDataManagerWithDuplicateCheck) loadRecentESDataForFirstRun() {
	udm.firstRunMutex.Lock()
	defer udm.firstRunMutex.Unlock()

	if !udm.isFirstRun {
		return
	}

	if udm.duplicateChecker == nil {
		udm.logger.Warn("중복 체크 서비스가 없어 첫 실행 중복 체크를 건너뜁니다")
		udm.isFirstRun = false
		return
	}

	udm.logger.Info("통합 모드 첫 실행 - Elasticsearch에서 최근 데이터 조회 중...")

	recentData, err := udm.duplicateChecker.GetRecentBusData(30)
	if err != nil {
		udm.logger.Errorf("통합 모드 첫 실행 ES 데이터 조회 실패: %v", err)
		udm.isFirstRun = false
		return
	}

	udm.recentESData = recentData
	udm.isFirstRun = false

	if len(recentData) > 0 {
		udm.logger.Infof("통합 모드 중복 체크용 데이터 로드 완료 - %d대 버스", len(recentData))
	} else {
		udm.logger.Info("통합 모드 첫 실행 - ES에 최근 데이터 없음")
	}
}

// isDuplicateDataForFirstRun 첫 실행 시 중복 데이터인지 확인
func (udm *UnifiedDataManagerWithDuplicateCheck) isDuplicateDataForFirstRun(plateNo string, bus models.BusLocation) bool {
	if len(udm.recentESData) == 0 {
		return false
	}

	esData, found := udm.recentESData[plateNo]
	if !found {
		return false
	}

	isDuplicate := esData.IsDuplicateData(bus.StationSeq, bus.NodeOrd, bus.StationId, bus.NodeId)

	if isDuplicate {
		udm.logger.Infof("통합모드 중복 데이터 감지 - 차량: %s, 현재: StationSeq=%d/NodeOrd=%d, ES최종: %s",
			plateNo, bus.StationSeq, bus.NodeOrd, esData.LastUpdate.Format("15:04:05"))
	}

	return isDuplicate
}

// updateInternalStateOnly 내부 상태만 업데이트 (ES 전송 없음)
func (udm *UnifiedDataManagerWithDuplicateCheck) updateInternalStateOnly(plateNo string, bus models.BusLocation, now time.Time, apiSource string) {
	unified, exists := udm.dataStore[plateNo]
	if !exists {
		unified = &UnifiedBusData{
			PlateNo:     plateNo,
			RouteId:     bus.RouteId,
			RouteNm:     bus.GetRouteIDString(),
			LastUpdate:  now,
			DataSources: []string{},
		}
		udm.dataStore[plateNo] = unified
	}

	if apiSource == "API1" {
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
	} else if apiSource == "API2" {
		unified.API2Data = &API2BusInfo{
			NodeId:     bus.NodeId,
			NodeNm:     bus.NodeNm,
			NodeOrd:    bus.NodeOrd,
			RouteNm:    bus.RouteNm,
			GpsLati:    bus.GpsLati,
			GpsLong:    bus.GpsLong,
			UpdateTime: now,
		}
		unified.LastAPI2Update = now
		unified.CurrentStationSeq = bus.NodeOrd
		unified.CurrentNodeNm = bus.NodeNm
		unified.CurrentNodeId = bus.NodeId

		if bus.RouteNm != "" {
			unified.RouteNm = bus.RouteNm
		}
		if bus.StationId > 0 {
			unified.CurrentStationId = bus.StationId
		}

		if !containsString(unified.DataSources, "api2") {
			unified.DataSources = append(unified.DataSources, "api2")
		}
	}

	unified.LastUpdate = now
	udm.busTracker.UpdateLastSeenTime(plateNo)
}

// sendChangedBusesToElasticsearch ES에 변경된 버스 데이터 전송
func (udm *UnifiedDataManagerWithDuplicateCheck) sendChangedBusesToElasticsearch(changedBuses []models.BusLocation, source string) {
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

// mergeDataForBus 버스 데이터 병합
func (udm *UnifiedDataManagerWithDuplicateCheck) mergeDataForBus(unified *UnifiedBusData) *models.BusLocation {
	if unified.API1Data == nil && unified.API2Data == nil {
		return nil
	}

	final := &models.BusLocation{
		PlateNo:    unified.PlateNo,
		RouteId:    unified.RouteId,
		RouteNm:    unified.RouteNm,
		StationId:  unified.CurrentStationId,
		Timestamp:  time.Now().Format(time.RFC3339),
		StationSeq: unified.CurrentStationSeq,
		NodeNm:     unified.CurrentNodeNm,
		NodeId:     unified.CurrentNodeId,
		NodeOrd:    unified.CurrentStationSeq,
		TripNumber: unified.TripNumber,
	}

	if unified.API2Data != nil {
		final.GpsLati = unified.API2Data.GpsLati
		final.GpsLong = unified.API2Data.GpsLong
		final.NodeNm = unified.API2Data.NodeNm
		final.NodeId = unified.API2Data.NodeId
		final.StationSeq = unified.API2Data.NodeOrd
		final.NodeOrd = unified.API2Data.NodeOrd
		final.RouteNm = unified.API2Data.RouteNm
	}

	if unified.API1Data != nil {
		final.VehId = unified.API1Data.VehId
		final.Crowded = unified.API1Data.Crowded
		final.RemainSeatCnt = unified.API1Data.RemainSeatCnt
		final.StateCd = unified.API1Data.StateCd
		final.LowPlate = unified.API1Data.LowPlate
		final.RouteTypeCd = unified.API1Data.RouteTypeCd
		final.TaglessCd = unified.API1Data.TaglessCd

		if unified.API2Data == nil {
			final.StationId = unified.API1Data.StationId
			final.StationSeq = unified.API1Data.StationSeq
			final.NodeOrd = unified.API1Data.StationSeq
		}
	}

	if udm.stationCache != nil {
		routeKey := final.RouteNm
		if routeKey == "" {
			routeKey = final.GetRouteIDString()
		}
		final.TotalStations = udm.stationCache.GetRouteStationCount(routeKey)
	}

	return final
}

// 🔧 간소화된 정리 작업 - 새로운 종료 조건 적용
func (udm *UnifiedDataManagerWithDuplicateCheck) CleanupOldData(maxAge time.Duration) int {
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

	// 🔧 새로운 종료 조건에 따른 버스 정리
	// BusTracker에서 config 기반으로 미목격 버스 정리
	cleanedBuses := udm.busTracker.CleanupMissingBuses(udm.logger)
	if cleanedBuses > 0 {
		udm.logger.Infof("버스 트래킹 종료 정리 완료 - %d대", cleanedBuses)
	}

	return len(removedPlates)
}

// GetStatistics 통계 정보 반환
func (udm *UnifiedDataManagerWithDuplicateCheck) GetStatistics() (int, int, int, int) {
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

// containsString 문자열 슬라이스에 특정 문자열이 포함되어 있는지 확인
func containsString(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// 인터페이스 구현 확인
var _ UnifiedDataManagerInterface = (*UnifiedDataManagerWithDuplicateCheck)(nil)
