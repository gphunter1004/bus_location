// internal/services/unified_manager.go - 로그 정리 버전
package services

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"bus-tracker/internal/models"
	"bus-tracker/internal/services/cache"
	"bus-tracker/internal/services/redis"
	"bus-tracker/internal/services/storage"
	"bus-tracker/internal/utils"
)

// UnifiedDataManagerInterface 통합 데이터 매니저 인터페이스
type UnifiedDataManagerInterface interface {
	UpdateAPI1Data(busLocations []models.BusLocation)
	UpdateAPI2Data(busLocations []models.BusLocation)
	CleanupOldData(maxAge time.Duration) int
	GetStatistics() (int, int, int, int)
	StartPeriodicESSync()
	StopPeriodicESSync()
}

// SimplifiedUnifiedDataManager Redis 중심 단순화된 통합 데이터 매니저
type SimplifiedUnifiedDataManager struct {
	logger           *utils.Logger
	stationCache     cache.StationCacheInterface
	esService        *storage.ElasticsearchService
	redisBusManager  *redis.RedisBusDataManager
	duplicateChecker *storage.ElasticsearchDuplicateChecker
	indexName        string

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

// NewSimplifiedUnifiedDataManager 단순화된 통합 데이터 매니저 생성
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
		batchSize:        100,
		batchTimeout:     30 * time.Second,
		lastESSync:       time.Now(),
		stopSyncChan:     make(chan struct{}),
		isFirstRun:       true,
		recentESData:     make(map[string]*storage.BusLastData),
	}
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

	// 로그 최소화: 첫 실행 시에만 출력
	udm.logger.Info("첫 실행 중복 체크 데이터 로딩...")

	recentData, err := udm.duplicateChecker.GetRecentBusData(30)
	if err != nil {
		udm.logger.Errorf("첫 실행 ES 데이터 조회 실패: %v", err)
		return
	}

	udm.recentESData = recentData

	if len(recentData) > 0 {
		udm.logger.Infof("중복 체크 데이터 로드 완료: %d대", len(recentData))
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

	return esData.IsDuplicateData(bus.StationSeq, bus.NodeOrd, bus.StationId, bus.NodeId)
}

// UpdateAPI1Data API1 데이터 업데이트 처리
func (udm *SimplifiedUnifiedDataManager) UpdateAPI1Data(busLocations []models.BusLocation) {
	if udm.isFirstRun {
		udm.loadRecentESDataForFirstRun()
	}

	var esReadyBuses []models.BusLocation
	processedCount := 0
	duplicateCount := 0
	redisChangedCount := 0
	redisNoChangeCount := 0

	var processedVehicles []string
	var changedVehicles []string

	for _, bus := range busLocations {
		plateNo := bus.PlateNo
		if plateNo == "" || bus.RouteId == 0 {
			continue
		}

		processedVehicles = append(processedVehicles, plateNo)

		if udm.isDuplicateDataForFirstRun(plateNo, bus) {
			duplicateCount++
			udm.redisBusManager.UpdateBusData(bus, []string{"api1"})
			continue
		}

		_, hasLocationChanged, err := udm.redisBusManager.UpdateBusData(bus, []string{"api1"})
		if err != nil {
			continue
		}

		processedCount++

		if hasLocationChanged {
			redisChangedCount++
			changedVehicles = append(changedVehicles, plateNo)
			if udm.shouldSendToES(plateNo, bus) {
				esReadyBuses = append(esReadyBuses, bus)
			}
		} else {
			redisNoChangeCount++
		}
	}

	if len(esReadyBuses) > 0 {
		udm.sendBatchToElasticsearch(esReadyBuses, "API1")
	}

	// 🆕 개선된 로그
	if redisChangedCount == 0 && len(esReadyBuses) == 0 && redisNoChangeCount > 0 {
		udm.logger.Infof("API1 처리완료: 수신=%d, 처리=%d, 위치변경=%d, ES전송=%d (동일위치: %s)",
			len(busLocations), processedCount, redisChangedCount, len(esReadyBuses),
			udm.formatVehicleList(processedVehicles))
	} else if len(changedVehicles) > 0 {
		udm.logger.Infof("API1 처리완료: 수신=%d, 처리=%d, 위치변경=%d(차량: %s), ES전송=%d",
			len(busLocations), processedCount, redisChangedCount,
			udm.formatVehicleList(changedVehicles), len(esReadyBuses))
	} else {
		udm.logger.Infof("API1 처리완료: 수신=%d, 처리=%d, 중복=%d, 변경=%d, ES전송=%d",
			len(busLocations), processedCount, duplicateCount, redisChangedCount, len(esReadyBuses))
	}
}

// UpdateAPI2Data API2 데이터 업데이트 처리
func (udm *SimplifiedUnifiedDataManager) UpdateAPI2Data(busLocations []models.BusLocation) {
	if udm.isFirstRun {
		udm.loadRecentESDataForFirstRun()
	}

	var esReadyBuses []models.BusLocation
	processedCount := 0
	duplicateCount := 0
	redisChangedCount := 0
	redisNoChangeCount := 0 // 🆕 위치 변경 없는 차량 수

	// 🆕 차량 목록 수집 (로그용)
	var processedVehicles []string
	var changedVehicles []string

	for _, bus := range busLocations {
		plateNo := bus.PlateNo
		if plateNo == "" || bus.RouteId == 0 {
			continue
		}

		processedVehicles = append(processedVehicles, plateNo)

		// 첫 실행 시 중복 체크
		if udm.isDuplicateDataForFirstRun(plateNo, bus) {
			duplicateCount++
			udm.redisBusManager.UpdateBusData(bus, []string{"api2"})
			continue
		}

		// Redis 업데이트
		_, hasLocationChanged, err := udm.redisBusManager.UpdateBusData(bus, []string{"api2"})
		if err != nil {
			continue
		}

		processedCount++

		if hasLocationChanged {
			redisChangedCount++
			changedVehicles = append(changedVehicles, plateNo)
			if udm.shouldSendToES(plateNo, bus) {
				esReadyBuses = append(esReadyBuses, bus)
			}
		} else {
			redisNoChangeCount++
		}
	}

	// ES 배치 전송
	if len(esReadyBuses) > 0 {
		udm.sendBatchToElasticsearch(esReadyBuses, "API2")
	}

	// 🆕 개선된 로그 - 상황 설명 추가
	if redisChangedCount == 0 && len(esReadyBuses) == 0 && redisNoChangeCount > 0 {
		udm.logger.Infof("API2 처리완료: 수신=%d, 처리=%d, 위치변경=%d, ES전송=%d (동일위치: %s)",
			len(busLocations), processedCount, redisChangedCount, len(esReadyBuses),
			udm.formatVehicleList(processedVehicles))
	} else if len(changedVehicles) > 0 {
		udm.logger.Infof("API2 처리완료: 수신=%d, 처리=%d, 위치변경=%d(차량: %s), ES전송=%d",
			len(busLocations), processedCount, redisChangedCount,
			udm.formatVehicleList(changedVehicles), len(esReadyBuses))
	} else {
		udm.logger.Infof("API2 처리완료: 수신=%d, 처리=%d, 중복=%d, 변경=%d, ES전송=%d",
			len(busLocations), processedCount, duplicateCount, redisChangedCount, len(esReadyBuses))
	}
}

// sendBatchToElasticsearch ES 배치 전송
func (udm *SimplifiedUnifiedDataManager) sendBatchToElasticsearch(buses []models.BusLocation, source string) {
	if udm.esService == nil || len(buses) == 0 {
		return
	}

	// 🎯 핵심 로그만: ES 전송 시작과 결과
	udm.logger.Infof("ES 전송 시작: %s %d건", source, len(buses))

	if err := udm.esService.BulkSendBusLocations(udm.indexName, buses); err != nil {
		udm.logger.Errorf("ES 전송 실패 (%s): %v", source, err)
		return
	}

	// ES 전송 성공 시 Redis에 동기화 마킹
	var plateNos []string
	for _, bus := range buses {
		plateNos = append(plateNos, bus.PlateNo)
	}

	if err := udm.redisBusManager.MarkAsSynced(plateNos); err != nil {
		udm.logger.Errorf("Redis 동기화 마킹 실패: %v", err)
	}

	udm.lastESSync = time.Now()

	// 🎯 핵심 로그만: 성공 메시지
	udm.logger.Infof("ES 전송 완료: %s %d건", source, len(buses))
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
		udm.logger.Info("Redis->ES 주기 동기화 시작")
		defer udm.logger.Info("Redis->ES 주기 동기화 종료")

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

	// 🆕 차량 목록 수집
	var vehicles []string
	for _, bus := range changedBuses {
		vehicles = append(vehicles, bus.PlateNo)
	}

	udm.logger.Infof("Redis 동기화: %d건 발견 (차량: %s)", len(changedBuses), udm.formatVehicleList(vehicles))

	// 배치 크기로 나누어 전송
	for i := 0; i < len(changedBuses); i += udm.batchSize {
		end := i + udm.batchSize
		if end > len(changedBuses) {
			end = len(changedBuses)
		}

		batch := changedBuses[i:end]
		udm.sendBatchToElasticsearch(batch, "Redis-Sync")

		time.Sleep(100 * time.Millisecond)
	}
}

// CleanupOldData 정리 작업
func (udm *SimplifiedUnifiedDataManager) CleanupOldData(maxAge time.Duration) int {
	cleanedCount, err := udm.redisBusManager.CleanupInactiveBuses(maxAge)
	if err != nil {
		udm.logger.Errorf("Redis 정리 실패: %v", err)
		return 0
	}

	// 🎯 핵심 로그만: 정리 결과 (정리된 것이 있을 때만)
	if cleanedCount > 0 {
		udm.logger.Infof("데이터 정리 완료: %d대 제거", cleanedCount)
	}

	return cleanedCount
}

// GetStatistics 통계 정보 반환
func (udm *SimplifiedUnifiedDataManager) GetStatistics() (int, int, int, int) {
	return 0, 0, 0, 0
}

// shouldSendToES ES 전송 전 최종 체크
func (udm *SimplifiedUnifiedDataManager) shouldSendToES(plateNo string, bus models.BusLocation) bool {
	// 최소 간격 체크 (10초)
	minInterval := 10 * time.Second

	lastSyncTime, err := udm.redisBusManager.GetLastESSyncTime(plateNo)
	if err == nil && !lastSyncTime.IsZero() {
		timeSinceLastSync := time.Since(lastSyncTime)
		if timeSinceLastSync < minInterval {
			return false
		}
	}

	return udm.isLocationChanged(plateNo, bus)
}

// isLocationChanged 위치 변경 여부 확인
func (udm *SimplifiedUnifiedDataManager) isLocationChanged(plateNo string, bus models.BusLocation) bool {
	lastESData, err := udm.redisBusManager.GetLastESData(plateNo)
	if err != nil || lastESData == nil {
		return true // 첫 전송 또는 데이터 없음
	}

	// 정류장 순서 비교
	currentOrder := bus.GetStationOrder()
	var lastOrder int
	if lastESData.NodeOrd > 0 {
		lastOrder = lastESData.NodeOrd
	} else if lastESData.StationSeq > 0 {
		lastOrder = lastESData.StationSeq
	} else {
		return true
	}

	// 위치 변경 체크 (상세 로그 제거)
	return currentOrder != lastOrder ||
		(bus.StationId != lastESData.StationId && bus.StationId > 0 && lastESData.StationId > 0) ||
		(bus.NodeId != lastESData.NodeId && bus.NodeId != "" && lastESData.NodeId != "")
}

// GetActiveBusesByRoute 노선별 활성 버스 조회
func (udm *SimplifiedUnifiedDataManager) GetActiveBusesByRoute(routeId int64) ([]string, error) {
	return udm.redisBusManager.GetActiveBusesByRoute(routeId)
}

// 🆕 차량 목록 포맷팅 헬퍼 함수
func (udm *SimplifiedUnifiedDataManager) formatVehicleList(vehicles []string) string {
	if len(vehicles) == 0 {
		return "없음"
	}
	if len(vehicles) <= 3 {
		return strings.Join(vehicles, ",")
	}
	return fmt.Sprintf("%s 외 %d대", strings.Join(vehicles[:2], ","), len(vehicles)-2)
}

// 인터페이스 구현 확인
var _ UnifiedDataManagerInterface = (*SimplifiedUnifiedDataManager)(nil)
