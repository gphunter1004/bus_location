// internal/services/unified_manager.go - API1/API2 역할 분리 버전
package services

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"bus-tracker/config"
	"bus-tracker/internal/models"
	"bus-tracker/internal/services/cache"
	"bus-tracker/internal/services/redis"
	"bus-tracker/internal/services/storage"
	"bus-tracker/internal/services/tracker"
	"bus-tracker/internal/utils"
)

// UnifiedDataManagerInterface 통합 데이터 매니저 인터페이스
type UnifiedDataManagerInterface interface {
	UpdateAPI1Data(busLocations []models.BusLocation) // 상태정보만
	UpdateAPI2Data(busLocations []models.BusLocation) // 위치정보 + ES 전송
	CleanupOldData(maxAge time.Duration) int
	GetStatistics() (int, int, int, int)
	StartPeriodicESSync()
	StopPeriodicESSync()
}

// SimplifiedUnifiedDataManager API1/API2 역할 분리된 통합 데이터 매니저
type SimplifiedUnifiedDataManager struct {
	config           *config.Config
	logger           *utils.Logger
	stationCache     cache.StationCacheInterface
	esService        *storage.ElasticsearchService
	redisBusManager  *redis.RedisBusDataManager
	duplicateChecker *storage.ElasticsearchDuplicateChecker
	indexName        string

	// BusTracker는 TripNumber 계산만 담당
	busTracker *tracker.BusTrackerWithDuplicateCheck

	// ES 전송 제어
	batchSize    int
	batchTimeout time.Duration
	lastESSync   time.Time
	syncTicker   *time.Ticker
	stopSyncChan chan struct{}
	syncRunning  bool
	syncMutex    sync.Mutex

	// 첫 실행 중복 체크
	isFirstRun    bool
	firstRunMutex sync.Mutex
	recentESData  map[string]*storage.BusLastData
}

// NewSimplifiedUnifiedDataManager 통합 데이터 매니저 생성
func NewSimplifiedUnifiedDataManager(
	logger *utils.Logger,
	stationCache cache.StationCacheInterface,
	esService *storage.ElasticsearchService,
	redisBusManager *redis.RedisBusDataManager,
	duplicateChecker *storage.ElasticsearchDuplicateChecker,
	indexName string) *SimplifiedUnifiedDataManager {

	return &SimplifiedUnifiedDataManager{
		logger:           logger,
		stationCache:     stationCache,
		esService:        esService,
		redisBusManager:  redisBusManager,
		duplicateChecker: duplicateChecker,
		indexName:        indexName,
		batchSize:        50,
		batchTimeout:     30 * time.Second,
		lastESSync:       time.Now(),
		stopSyncChan:     make(chan struct{}),
		isFirstRun:       true,
		recentESData:     make(map[string]*storage.BusLastData),
	}
}

// InitializeBusTracker BusTracker 초기화
func (udm *SimplifiedUnifiedDataManager) InitializeBusTracker(cfg *config.Config) {
	if udm.duplicateChecker != nil {
		udm.busTracker = tracker.NewBusTrackerWithDuplicateCheck(cfg, udm.duplicateChecker)
		udm.logger.Info("✅ BusTracker with DuplicateCheck 초기화 완료")
	} else {
		basicTracker := tracker.NewBusTracker(cfg)
		udm.busTracker = &tracker.BusTrackerWithDuplicateCheck{
			BusTracker: basicTracker,
		}
		udm.logger.Info("✅ 기본 BusTracker 초기화 완료")
	}
	udm.config = cfg
}

// NewUnifiedDataManagerWithRedis 호환성을 위한 별칭
func NewUnifiedDataManagerWithRedis(
	logger *utils.Logger,
	stationCache cache.StationCacheInterface,
	esService *storage.ElasticsearchService,
	redisBusManager *redis.RedisBusDataManager,
	duplicateChecker *storage.ElasticsearchDuplicateChecker,
	indexName string) *SimplifiedUnifiedDataManager {

	return NewSimplifiedUnifiedDataManager(logger, stationCache, esService, redisBusManager, duplicateChecker, indexName)
}

// loadRecentESDataForFirstRun 첫 실행 시 ES에서 최근 데이터 로드
func (udm *SimplifiedUnifiedDataManager) loadRecentESDataForFirstRun() {
	udm.firstRunMutex.Lock()
	defer udm.firstRunMutex.Unlock()

	if !udm.isFirstRun {
		return
	}

	udm.isFirstRun = false

	if udm.duplicateChecker == nil {
		return
	}

	udm.logger.Info("🔍 첫 실행 중복 체크 데이터 로딩...")

	recentData, err := udm.duplicateChecker.GetRecentBusData(30)
	if err != nil {
		udm.logger.Errorf("첫 실행 ES 데이터 조회 실패: %v", err)
		return
	}

	udm.recentESData = recentData

	if len(recentData) > 0 {
		udm.logger.Infof("✅ 중복 체크 데이터 로드 완료: %d대", len(recentData))
	}
}

func (udm *SimplifiedUnifiedDataManager) UpdateAPI1Data(busLocations []models.BusLocation) {
	if len(busLocations) == 0 {
		return
	}

	// TripNumber 설정
	enrichedBuses := udm.enrichBusesWithTripNumber(busLocations, "API1")

	processedCount := 0
	statusUpdatedCount := 0

	var processedVehicles []string
	var statusUpdatedVehicles []string

	// API1: 상태정보만 Redis 업데이트 (ES 전송 없음)
	for _, bus := range enrichedBuses {
		plateNo := bus.PlateNo
		if plateNo == "" || bus.RouteId == 0 {
			continue
		}

		processedVehicles = append(processedVehicles, plateNo)

		// 🔧 Redis 상태만 업데이트 (forceStatusOnly = true)
		statusUpdated, err := udm.redisBusManager.UpdateBusStatusOnly(bus, []string{"api1"})
		if err != nil {
			udm.logger.Errorf("Redis 상태 업데이트 실패 - 차량: %s, 오류: %v", plateNo, err)
			continue
		}

		processedCount++

		if statusUpdated {
			statusUpdatedCount++
			statusUpdatedVehicles = append(statusUpdatedVehicles, plateNo)
		}
	}

	// 로그 출력 (ES 전송 없음)
	if statusUpdatedCount > 0 {
		udm.logger.Infof("API1 상태처리완료: 수신=%d, 처리=%d, 상태갱신=%d(차량:%s)",
			len(busLocations), processedCount, statusUpdatedCount,
			udm.formatVehicleList(statusUpdatedVehicles))
	} else {
		udm.logger.Infof("API1 상태처리완료: 수신=%d, 처리=%d, 상태변경=0 (상태 동일)",
			len(busLocations), processedCount)
	}
}

// 🔧 API2 데이터 처리 - 위치정보 + ES 전송
func (udm *SimplifiedUnifiedDataManager) UpdateAPI2Data(busLocations []models.BusLocation) {
	if len(busLocations) == 0 {
		return
	}

	if udm.isFirstRun {
		udm.loadRecentESDataForFirstRun()
	}

	// TripNumber 설정
	enrichedBuses := udm.enrichBusesWithTripNumber(busLocations, "API2")

	var esReadyBuses []models.BusLocation
	processedCount := 0
	locationChangedCount := 0
	duplicateCount := 0

	var processedVehicles []string
	var locationChangedVehicles []string
	var duplicateVehicles []string

	// API2: 위치정보 처리 + ES 전송 조건 확인
	for _, bus := range enrichedBuses {
		plateNo := bus.PlateNo
		if plateNo == "" || bus.RouteId == 0 {
			continue
		}

		processedVehicles = append(processedVehicles, plateNo)

		// 첫 실행 시 중복 체크
		if udm.isDuplicateDataForFirstRun(plateNo, bus) {
			duplicateCount++
			duplicateVehicles = append(duplicateVehicles, plateNo)
			// 중복 데이터도 Redis에는 저장 (상태 추적용)
			udm.redisBusManager.UpdateBusLocation(bus, []string{"api2"})
			continue
		}

		// 🔧 Redis 위치정보 업데이트
		locationChanged, err := udm.redisBusManager.UpdateBusLocation(bus, []string{"api2"})
		if err != nil {
			udm.logger.Errorf("Redis 위치 업데이트 실패 - 차량: %s, 오류: %v", plateNo, err)
			continue
		}

		processedCount++

		// 🔧 위치 변경된 경우만 ES 전송 대상
		if locationChanged {
			locationChangedCount++
			locationChangedVehicles = append(locationChangedVehicles, plateNo)
			esReadyBuses = append(esReadyBuses, bus)
		}
	}

	// ES 배치 전송 (위치 변경된 경우만)
	if len(esReadyBuses) > 0 {
		udm.sendBatchToElasticsearch(esReadyBuses, "API2")
	}

	// 로그 출력
	if duplicateCount > 0 {
		udm.logger.Infof("API2 위치처리완료: 수신=%d, 처리=%d, 중복=%d(차량:%s), 위치변경=%d, ES전송=%d",
			len(busLocations), processedCount, duplicateCount,
			udm.formatVehicleList(duplicateVehicles), locationChangedCount, len(esReadyBuses))
	} else if locationChangedCount > 0 {
		udm.logger.Infof("API2 위치처리완료: 수신=%d, 처리=%d, 위치변경=%d(차량:%s), ES전송=%d",
			len(busLocations), processedCount, locationChangedCount,
			udm.formatVehicleList(locationChangedVehicles), len(esReadyBuses))
	} else {
		udm.logger.Infof("API2 위치처리완료: 수신=%d, 처리=%d, 위치변경=0, ES전송=0 (모두 동일위치)",
			len(busLocations), processedCount)
	}
}

// isDuplicateDataForFirstRun 첫 실행 시 중복 데이터인지 확인
func (udm *SimplifiedUnifiedDataManager) isDuplicateDataForFirstRun(plateNo string, bus models.BusLocation) bool {
	if len(udm.recentESData) == 0 {
		return false
	}

	esData, found := udm.recentESData[plateNo]
	if !found {
		return false
	}

	isDup := esData.IsDuplicateData(bus.StationSeq, bus.NodeOrd, bus.StationId, bus.NodeId)
	if isDup {
		udm.logger.Infof("🔄 중복 데이터 감지 - 차량: %s (ES 최종위치와 동일)", plateNo)
	}
	return isDup
}

// BusTracker로 TripNumber만 설정
func (udm *SimplifiedUnifiedDataManager) enrichBusesWithTripNumber(busLocations []models.BusLocation, source string) []models.BusLocation {
	if udm.busTracker == nil {
		udm.logger.Errorf("BusTracker가 초기화되지 않았습니다")
		return busLocations
	}

	// 모든 버스의 LastSeenTime 업데이트
	for _, bus := range busLocations {
		udm.busTracker.UpdateLastSeenTime(bus.PlateNo)
	}

	// TripNumber 설정
	enrichedBuses := make([]models.BusLocation, len(busLocations))
	for i, bus := range busLocations {
		// StationId를 위치 정보로 사용
		currentPosition := bus.StationId
		if currentPosition <= 0 {
			currentPosition = int64(bus.GetStationOrder())
		}

		cacheKey := bus.GetCacheKey()

		// TripNumber 조회/설정 (위치 변경 여부는 확인하지 않음)
		_, tripNumber := udm.busTracker.IsStationChanged(bus.PlateNo, currentPosition, cacheKey, bus.TotalStations)

		enrichedBus := bus
		enrichedBus.TripNumber = tripNumber
		enrichedBuses[i] = enrichedBus
	}

	udm.logger.Debugf("🔢 %s TripNumber 설정 완료: %d건", source, len(enrichedBuses))
	return enrichedBuses
}

// sendBatchToElasticsearch ES 배치 전송
func (udm *SimplifiedUnifiedDataManager) sendBatchToElasticsearch(buses []models.BusLocation, source string) {
	if udm.esService == nil || len(buses) == 0 {
		return
	}

	// 차량 목록 수집 (TripNumber 포함)
	var vehicles []string
	for _, bus := range buses {
		vehicles = append(vehicles, fmt.Sprintf("%s(T%d:%s)", bus.PlateNo, bus.TripNumber, bus.NodeNm))
	}

	udm.logger.Infof("📤 ES 전송 시작: %s %d건", source, len(buses))
	udm.logger.Debugf("📤 전송 상세: %s", udm.formatVehicleList(vehicles))

	start := time.Now()
	if err := udm.esService.BulkSendBusLocations(udm.indexName, buses); err != nil {
		udm.logger.Errorf("❌ ES 전송 실패 (%s): %v", source, err)
		return
	}
	duration := time.Since(start)

	// ES 전송 성공 시 Redis에 동기화 마킹
	var plateNos []string
	for _, bus := range buses {
		plateNos = append(plateNos, bus.PlateNo)
	}

	if err := udm.redisBusManager.MarkAsSynced(plateNos); err != nil {
		udm.logger.Errorf("Redis 동기화 마킹 실패: %v", err)
	}

	udm.lastESSync = time.Now()

	udm.logger.Infof("✅ ES 전송 완료: %s %d건 (소요: %v)",
		source, len(buses), duration.Round(time.Millisecond))
}

// StartPeriodicESSync 주기적 ES 동기화 시작
func (udm *SimplifiedUnifiedDataManager) StartPeriodicESSync() {
	udm.syncMutex.Lock()
	defer udm.syncMutex.Unlock()

	if udm.syncRunning {
		return
	}

	udm.syncTicker = time.NewTicker(udm.batchTimeout)
	udm.syncRunning = true

	go func() {
		udm.logger.Info("🔄 Redis->ES 주기 동기화 시작")
		defer udm.logger.Info("🔄 Redis->ES 주기 동기화 종료")

		for {
			select {
			case <-udm.stopSyncChan:
				return
			case <-udm.syncTicker.C:
				udm.syncRedisToES()
			}
		}
	}()
}

// StopPeriodicESSync 주기적 ES 동기화 중지
func (udm *SimplifiedUnifiedDataManager) StopPeriodicESSync() {
	udm.syncMutex.Lock()
	defer udm.syncMutex.Unlock()

	if !udm.syncRunning {
		return
	}

	close(udm.stopSyncChan)
	if udm.syncTicker != nil {
		udm.syncTicker.Stop()
	}
	udm.syncRunning = false
}

// syncRedisToES Redis에서 ES로 동기화
func (udm *SimplifiedUnifiedDataManager) syncRedisToES() {
	changedBuses, err := udm.redisBusManager.GetChangedBusesForES()
	if err != nil {
		udm.logger.Errorf("Redis 변경 데이터 조회 실패: %v", err)
		return
	}

	if len(changedBuses) == 0 {
		return
	}

	// 차량 목록 수집
	var vehicles []string
	for _, bus := range changedBuses {
		vehicles = append(vehicles, bus.PlateNo)
	}

	udm.logger.Infof("🔄 Redis 동기화: %d건 발견 (차량: %s)",
		len(changedBuses), udm.formatVehicleList(vehicles))

	// 배치 크기로 나누어 전송
	for i := 0; i < len(changedBuses); i += udm.batchSize {
		end := i + udm.batchSize
		if end > len(changedBuses) {
			end = len(changedBuses)
		}

		batch := changedBuses[i:end]
		udm.sendBatchToElasticsearch(batch, "Redis-Sync")

		if end < len(changedBuses) {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// CleanupOldData 정리 작업 - BusTracker 정리 포함
func (udm *SimplifiedUnifiedDataManager) CleanupOldData(maxAge time.Duration) int {
	// Redis 정리
	cleanedCount, err := udm.redisBusManager.CleanupInactiveBuses(maxAge)
	if err != nil {
		udm.logger.Errorf("Redis 정리 실패: %v", err)
	}

	// BusTracker 정리
	if udm.busTracker != nil {
		trackerCleaned := udm.busTracker.CleanupMissingBuses(udm.logger)
		cleanedCount += trackerCleaned
	}

	if cleanedCount > 0 {
		udm.logger.Infof("🧹 데이터 정리 완료: %d대 제거", cleanedCount)
	}

	return cleanedCount
}

// GetStatistics 통계 정보 반환 - BusTracker 통계 포함
func (udm *SimplifiedUnifiedDataManager) GetStatistics() (int, int, int, int) {
	if udm.busTracker != nil {
		trackedBuses := udm.busTracker.GetTrackedBusCount()
		tripStats := udm.busTracker.GetDailyTripStatistics()
		udm.logger.Debugf("📊 BusTracker 통계 - 추적중: %d대, 운행차수: %d건", trackedBuses, len(tripStats))
		return trackedBuses, 0, 0, 0
	}
	return 0, 0, 0, 0
}

// GetActiveBusesByRoute 노선별 활성 버스 조회
func (udm *SimplifiedUnifiedDataManager) GetActiveBusesByRoute(routeId int64) ([]string, error) {
	return udm.redisBusManager.GetActiveBusesByRoute(routeId)
}

// GetBusTrackerStatistics BusTracker 통계 조회
func (udm *SimplifiedUnifiedDataManager) GetBusTrackerStatistics() map[string]interface{} {
	if udm.busTracker == nil {
		return map[string]interface{}{
			"tracker_enabled": false,
			"error":           "BusTracker 초기화되지 않음",
		}
	}

	tripStats := udm.busTracker.GetDailyTripStatistics()
	trackedCount := udm.busTracker.GetTrackedBusCount()
	currentDate := udm.busTracker.GetCurrentOperatingDate()

	return map[string]interface{}{
		"tracker_enabled":        true,
		"tracked_buses":          trackedCount,
		"daily_trip_count":       len(tripStats),
		"current_operating_date": currentDate,
		"trip_statistics":        tripStats,
	}
}

// 차량 목록 포맷팅 헬퍼 함수
func (udm *SimplifiedUnifiedDataManager) formatVehicleList(vehicles []string) string {
	if len(vehicles) == 0 {
		return "없음"
	}
	if len(vehicles) <= 2 {
		return strings.Join(vehicles, ",")
	}
	return fmt.Sprintf("%s 외 %d대", strings.Join(vehicles[:1], ","), len(vehicles)-1)
}

// 인터페이스 구현 확인
var _ UnifiedDataManagerInterface = (*SimplifiedUnifiedDataManager)(nil)
