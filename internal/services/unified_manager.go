// internal/services/unified_manager.go - RouteId 보장 로직
package services

import (
	"strconv"
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
	RouteNm           string              `json:"routeNm"`
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
	TripNumber        int                 `json:"tripNumber"`
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
	RouteNm    string    `json:"routeNm"` // API2 원본 노선번호
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
	stationCache     cache.StationCacheInterface
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
	stationCache cache.StationCacheInterface,
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

// buildUnifiedData 통합 데이터 생성
func (udm *UnifiedDataManagerWithDuplicateCheck) buildUnifiedData(unified *UnifiedBusData) *models.BusLocation {
	if unified.API1Data == nil && unified.API2Data == nil {
		return nil
	}

	// 🔧 RouteId가 0인 경우 오류 방지
	if unified.RouteId == 0 {
		udm.logger.Errorf("RouteId가 0입니다 - 차량: %s", unified.PlateNo)
		return nil
	}

	// 🔧 기본 정보 설정 (RouteId 보장)
	final := &models.BusLocation{
		PlateNo:    unified.PlateNo,
		RouteId:    unified.RouteId, // 🔧 항상 유효한 RouteId
		Timestamp:  time.Now().Format(time.RFC3339),
		TripNumber: unified.TripNumber,
	}

	// 🔧 API2 데이터 우선 적용
	if unified.API2Data != nil {
		final.NodeId = unified.API2Data.NodeId
		final.NodeNm = unified.API2Data.NodeNm
		final.NodeOrd = unified.API2Data.NodeOrd
		final.GpsLati = unified.API2Data.GpsLati
		final.GpsLong = unified.API2Data.GpsLong
		final.StationSeq = unified.API2Data.NodeOrd

		// API2의 실제 노선번호 사용
		final.RouteNm = unified.API2Data.RouteNm

		// StationId 설정
		if unified.CurrentStationId > 0 {
			final.StationId = unified.CurrentStationId
		}
	}

	// 🔧 API1 데이터 적용 (보완)
	if unified.API1Data != nil {
		final.VehId = unified.API1Data.VehId
		final.Crowded = unified.API1Data.Crowded
		final.RemainSeatCnt = unified.API1Data.RemainSeatCnt
		final.StateCd = unified.API1Data.StateCd
		final.LowPlate = unified.API1Data.LowPlate
		final.RouteTypeCd = unified.API1Data.RouteTypeCd
		final.TaglessCd = unified.API1Data.TaglessCd

		// API2 데이터가 없는 경우에만 API1 위치 정보 사용
		if unified.API2Data == nil {
			final.StationId = unified.API1Data.StationId
			final.StationSeq = unified.API1Data.StationSeq
			final.NodeOrd = unified.API1Data.StationSeq

			// 캐시에서 정류장 정보 조회
			if udm.stationCache != nil {
				cacheKey := strconv.FormatInt(unified.RouteId, 10)
				if stationInfo, exists := udm.stationCache.GetStationInfo(cacheKey, unified.API1Data.StationSeq); exists {
					final.NodeNm = stationInfo.NodeNm
					final.NodeId = stationInfo.NodeId
				}
			}
		}
	}

	// 전체 정류소 수 설정
	if udm.stationCache != nil {
		cacheKey := strconv.FormatInt(unified.RouteId, 10)
		final.TotalStations = udm.stationCache.GetRouteStationCount(cacheKey)
	}

	return final
}

// getOrCreateUnifiedData 통합 데이터 가져오기 또는 생성 (RouteId 보장)
func (udm *UnifiedDataManagerWithDuplicateCheck) getOrCreateUnifiedData(plateNo string, routeId int64, now time.Time) *UnifiedBusData {
	unified, exists := udm.dataStore[plateNo]
	if !exists {
		// 🔧 RouteId 검증
		if routeId == 0 {
			udm.logger.Errorf("유효하지 않은 RouteId (0) - 차량: %s", plateNo)
			// 임시 RouteId 할당 (오류 방지)
			routeId = 999999999
		}

		unified = &UnifiedBusData{
			PlateNo:     plateNo,
			RouteId:     routeId, // 항상 유효한 RouteId
			RouteNm:     "",
			LastUpdate:  now,
			DataSources: []string{},
		}
		udm.dataStore[plateNo] = unified

		udm.logger.Debugf("새 통합 데이터 생성 - 차량: %s, RouteId: %d", plateNo, routeId)
	} else {
		// 🔧 기존 데이터의 RouteId가 0인 경우 업데이트
		if unified.RouteId == 0 && routeId > 0 {
			unified.RouteId = routeId
			udm.logger.Infof("RouteId 업데이트 - 차량: %s, 새 RouteId: %d", plateNo, routeId)
		}
	}
	return unified
}

// sendChangedBusesToElasticsearch ES에 변경된 버스 데이터 전송
func (udm *UnifiedDataManagerWithDuplicateCheck) sendChangedBusesToElasticsearch(changedBuses []models.BusLocation, source string) {
	if udm.esService == nil {
		return
	}

	for i, bus := range changedBuses {
		udm.logger.Infof("ES 전송 [%d/%d] - 차량: %s, 노선: %s (RouteId: %d), 정류장: %s, 차수: %d (%s)",
			i+1, len(changedBuses), bus.PlateNo, bus.RouteNm, bus.RouteId, bus.NodeNm, bus.TripNumber, source)
	}

	if err := udm.esService.BulkSendBusLocations(udm.indexName, changedBuses); err != nil {
		udm.logger.Errorf("ES 전송 실패 (%s): %v", source, err)
		return
	}
}

// CleanupOldData 간소화된 정리 작업
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
