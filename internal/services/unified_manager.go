// internal/services/unified_manager.go - Redis 중심 단순화 버전
package services

import (
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

	// 첫 실행 중복 체크 (단순화)
	isFirstRun    bool
	firstRunMutex sync.Mutex
	recentESData  map[string]*storage.BusLastData
}

// NewSimplifiedUnifiedDataManager -> NewUnifiedDataManagerWithRedis로 이름 변경 (호환성)
func NewUnifiedDataManagerWithRedis(
	logger *utils.Logger,
	stationCache cache.StationCacheInterface,
	esService *storage.ElasticsearchService,
	redisBusManager *redis.RedisBusDataManager,
	duplicateChecker *storage.ElasticsearchDuplicateChecker,
	indexName string) *SimplifiedUnifiedDataManager {

	return NewSimplifiedUnifiedDataManager(logger, stationCache, esService, redisBusManager, duplicateChecker, indexName)
}
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

// loadRecentESDataForFirstRun 첫 실행 시 ES에서 최근 데이터 로드 (단순화)
func (udm *SimplifiedUnifiedDataManager) loadRecentESDataForFirstRun() {
	udm.firstRunMutex.Lock()
	defer udm.firstRunMutex.Unlock()

	if !udm.isFirstRun {
		return
	}

	udm.isFirstRun = false // 먼저 설정하여 중복 호출 방지

	if udm.duplicateChecker == nil {
		udm.logger.Warn("중복 체크 서비스가 없어 첫 실행 중복 체크를 건너뜁니다")
		return
	}

	udm.logger.Info("단순화된 통합 모드 첫 실행 - Elasticsearch에서 최근 데이터 조회 중...")

	recentData, err := udm.duplicateChecker.GetRecentBusData(30)
	if err != nil {
		udm.logger.Errorf("첫 실행 ES 데이터 조회 실패: %v", err)
		return
	}

	udm.recentESData = recentData

	if len(recentData) > 0 {
		udm.logger.Infof("중복 체크용 데이터 로드 완료 - %d대 버스", len(recentData))
	} else {
		udm.logger.Info("첫 실행 - ES에 최근 데이터 없음")
	}
}

// isDuplicateDataForFirstRun 첫 실행 시 중복 데이터인지 확인 (단순화)
func (udm *SimplifiedUnifiedDataManager) isDuplicateDataForFirstRun(plateNo string, bus models.BusLocation) bool {
	if len(udm.recentESData) == 0 {
		return false
	}

	esData, found := udm.recentESData[plateNo]
	if !found {
		return false
	}

	isDuplicate := esData.IsDuplicateData(bus.StationSeq, bus.NodeOrd, bus.StationId, bus.NodeId)

	if isDuplicate {
		udm.logger.Debugf("중복 데이터 감지 - 차량: %s, 현재: StationSeq=%d/NodeOrd=%d, ES최종: %s",
			plateNo, bus.StationSeq, bus.NodeOrd, esData.LastUpdate.Format("15:04:05"))
	}

	return isDuplicate
}

// UpdateAPI1Data API1 데이터 업데이트 처리 (단순화)
func (udm *SimplifiedUnifiedDataManager) UpdateAPI1Data(busLocations []models.BusLocation) {
	if udm.isFirstRun {
		udm.loadRecentESDataForFirstRun()
	}

	var esReadyBuses []models.BusLocation
	processedCount := 0
	duplicateCount := 0
	redisChangedCount := 0

	udm.logger.Infof("🟦 API1 데이터 처리 시작 - 총 %d건", len(busLocations))

	for _, bus := range busLocations {
		plateNo := bus.PlateNo
		if plateNo == "" {
			udm.logger.Warnf("⚠️ API1 데이터에 차량번호 없음, 건너뛰기")
			continue
		}

		// RouteId 검증
		if bus.RouteId == 0 {
			udm.logger.Warnf("⚠️ API1 데이터에 유효하지 않은 RouteId - 차량: %s, 건너뛰기", plateNo)
			continue
		}

		// 첫 실행 시 중복 체크
		if udm.isDuplicateDataForFirstRun(plateNo, bus) {
			duplicateCount++
			// 중복이어도 Redis에는 상태 업데이트 (TTL 연장 등)
			udm.redisBusManager.UpdateBusData(bus, []string{"api1"})
			continue
		}

		// Redis에 업데이트하고 위치 변경 여부 확인
		_, hasLocationChanged, err := udm.redisBusManager.UpdateBusData(bus, []string{"api1"})
		if err != nil {
			udm.logger.Errorf("❌ Redis 업데이트 실패 (API1, 차량: %s): %v", plateNo, err)
			continue
		}

		processedCount++

		if hasLocationChanged {
			redisChangedCount++
			udm.logger.Debugf("🔄 Redis 위치 변경 감지 - 차량: %s", plateNo)

			// Redis에서 위치 변경이 있으면 ES 전송 조건 확인 (동일한 로직 사용)
			if udm.shouldSendToES(plateNo, bus) {
				esReadyBuses = append(esReadyBuses, bus)
				udm.logger.Debugf("🎯 ES 전송 대상 추가 (API1) - 차량: %s, RouteId: %d", plateNo, bus.RouteId)
			} else {
				udm.logger.Debugf("⏸️ ES 전송 조건 미충족 - 차량: %s (간격 부족 또는 기타)", plateNo)
			}
		} else {
			udm.logger.Debugf("🔄 Redis 위치 변경 없음 - 차량: %s", plateNo)
		}
	}

	// ES 배치 전송
	if len(esReadyBuses) > 0 {
		udm.sendBatchToElasticsearch(esReadyBuses, "API1")
	}

	udm.logger.Infof("🟦 API1 배치 처리 완료 - 총: %d건, 처리: %d건, 중복: %d건, Redis변경: %d건, 🔥 ES 전송: %d건",
		len(busLocations), processedCount, duplicateCount, redisChangedCount, len(esReadyBuses))
}

// UpdateAPI2Data API2 데이터 업데이트 처리 (단순화)
func (udm *SimplifiedUnifiedDataManager) UpdateAPI2Data(busLocations []models.BusLocation) {
	if udm.isFirstRun {
		udm.loadRecentESDataForFirstRun()
	}

	var esReadyBuses []models.BusLocation
	processedCount := 0
	duplicateCount := 0
	redisChangedCount := 0

	udm.logger.Infof("🟨 API2 데이터 처리 시작 - 총 %d건", len(busLocations))

	for _, bus := range busLocations {
		plateNo := bus.PlateNo
		if plateNo == "" {
			udm.logger.Warnf("⚠️ API2 데이터에 차량번호 없음, 건너뛰기")
			continue
		}

		// RouteId 검증
		if bus.RouteId == 0 {
			udm.logger.Warnf("⚠️ API2 데이터에서 RouteId 추출 실패 - 차량: %s, RouteNm: %s, 건너뛰기", plateNo, bus.RouteNm)
			continue
		}

		// 첫 실행 시 중복 체크
		if udm.isDuplicateDataForFirstRun(plateNo, bus) {
			duplicateCount++
			// 중복이어도 Redis에는 상태 업데이트 (TTL 연장 등)
			udm.redisBusManager.UpdateBusData(bus, []string{"api2"})
			continue
		}

		// Redis에 업데이트하고 위치 변경 여부 확인
		_, hasLocationChanged, err := udm.redisBusManager.UpdateBusData(bus, []string{"api2"})
		if err != nil {
			udm.logger.Errorf("❌ Redis 업데이트 실패 (API2, 차량: %s): %v", plateNo, err)
			continue
		}

		processedCount++

		if hasLocationChanged {
			redisChangedCount++
			udm.logger.Debugf("🔄 Redis 위치 변경 감지 - 차량: %s", plateNo)

			// Redis에서 위치 변경이 있으면 ES 전송 조건 확인 (동일한 로직 사용)
			if udm.shouldSendToES(plateNo, bus) {
				esReadyBuses = append(esReadyBuses, bus)
				udm.logger.Debugf("🎯 ES 전송 대상 추가 (API2) - 차량: %s, NodeOrd: %d, GPS: (%.6f, %.6f)",
					plateNo, bus.NodeOrd, bus.GpsLati, bus.GpsLong)
			} else {
				udm.logger.Debugf("⏸️ ES 전송 조건 미충족 - 차량: %s (간격 부족 또는 기타)", plateNo)
			}
		} else {
			udm.logger.Debugf("🔄 Redis 위치 변경 없음 - 차량: %s", plateNo)
		}
	}

	// ES 배치 전송
	if len(esReadyBuses) > 0 {
		udm.sendBatchToElasticsearch(esReadyBuses, "API2")
	}

	udm.logger.Infof("🟨 API2 배치 처리 완료 - 총: %d건, 처리: %d건, 중복: %d건, Redis변경: %d건, 🔥 ES 전송: %d건",
		len(busLocations), processedCount, duplicateCount, redisChangedCount, len(esReadyBuses))
}

// sendBatchToElasticsearch ES 배치 전송 (단순화)
func (udm *SimplifiedUnifiedDataManager) sendBatchToElasticsearch(buses []models.BusLocation, source string) {
	if udm.esService == nil || len(buses) == 0 {
		return
	}

	udm.logger.Infof("🔥 ES 배치 전송 (%s): %d건", source, len(buses))

	// 상세 로그 (처음 3개만) - 특별한 이모지 사용
	for i, bus := range buses {
		if i >= 3 {
			break
		}
		location := "정보없음"
		if bus.NodeNm != "" {
			location = bus.NodeNm
		} else if bus.NodeId != "" {
			location = bus.NodeId
		}

		// 소스별 다른 이모지 사용
		var sourceEmoji string
		switch source {
		case "API1":
			sourceEmoji = "🟦" // 파란 사각형
		case "API2":
			sourceEmoji = "🟨" // 노란 사각형
		case "Redis-Sync":
			sourceEmoji = "🟪" // 보라 사각형
		default:
			sourceEmoji = "⚫" // 검은 원
		}

		udm.logger.Infof("⚡ ES 전송 [%d/%d] %s 차량: %s, 노선: %s (RouteId: %d), 위치: %s",
			i+1, len(buses), sourceEmoji, bus.PlateNo, bus.RouteNm, bus.RouteId, location)
	}

	if err := udm.esService.BulkSendBusLocations(udm.indexName, buses); err != nil {
		udm.logger.Errorf("❌ ES 배치 전송 실패 (%s): %v", source, err)
		return
	}

	// ES 전송 성공 시 Redis에 동기화 마킹
	var plateNos []string
	for _, bus := range buses {
		plateNos = append(plateNos, bus.PlateNo)
	}

	if err := udm.redisBusManager.MarkAsSynced(plateNos); err != nil {
		udm.logger.Errorf("⚠️ Redis 동기화 마킹 실패: %v", err)
	}

	udm.lastESSync = time.Now()

	// 성공 메시지도 눈에 띄게
	udm.logger.Infof("✅ ES 배치 전송 성공 (%s): %d건 완료", source, len(buses))
}

// StartPeriodicESSync 주기적 ES 동기화 시작 (단순화)
func (udm *SimplifiedUnifiedDataManager) StartPeriodicESSync() {
	udm.syncMutex.Lock()
	defer udm.syncMutex.Unlock()

	if udm.syncRunning {
		return
	}

	udm.syncTicker = time.NewTicker(udm.batchTimeout)
	udm.syncRunning = true

	go func() {
		udm.logger.Info("🔄 Redis -> ES 주기적 동기화 시작")
		defer udm.logger.Info("🔄 Redis -> ES 주기적 동기화 종료")

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

// StopPeriodicESSync 주기적 ES 동기화 중지 (단순화)
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

// syncRedisToES Redis에서 ES로 동기화 (단순화)
func (udm *SimplifiedUnifiedDataManager) syncRedisToES() {
	changedBuses, err := udm.redisBusManager.GetChangedBusesForES()
	if err != nil {
		udm.logger.Errorf("❌ Redis 변경 데이터 조회 실패: %v", err)
		return
	}

	if len(changedBuses) == 0 {
		udm.logger.Debugf("🔄 Redis -> ES 동기화: 변경된 데이터 없음")
		return
	}

	udm.logger.Infof("🟪 Redis -> ES 동기화: %d건 발견", len(changedBuses))

	// 배치 크기로 나누어 전송
	for i := 0; i < len(changedBuses); i += udm.batchSize {
		end := i + udm.batchSize
		if end > len(changedBuses) {
			end = len(changedBuses)
		}

		batch := changedBuses[i:end]
		udm.sendBatchToElasticsearch(batch, "Redis-Sync")

		// 배치 간 잠시 대기
		time.Sleep(100 * time.Millisecond)
	}
}

// CleanupOldData 정리 작업 (Redis 중심)
func (udm *SimplifiedUnifiedDataManager) CleanupOldData(maxAge time.Duration) int {
	cleanedCount, err := udm.redisBusManager.CleanupInactiveBuses(maxAge)
	if err != nil {
		udm.logger.Errorf("Redis 정리 실패: %v", err)
		return 0
	}

	if cleanedCount > 0 {
		udm.logger.Infof("Redis 데이터 정리 완료 - 제거된 버스: %d대", cleanedCount)
	}

	return cleanedCount
}

// GetStatistics 통계 정보 반환 (단순화)
func (udm *SimplifiedUnifiedDataManager) GetStatistics() (int, int, int, int) {
	// 단순히 0 반환 (통계 기능 제거)
	return 0, 0, 0, 0
}

// shouldSendToES ES 전송 전 최종 체크 (단순화)
func (udm *SimplifiedUnifiedDataManager) shouldSendToES(plateNo string, bus models.BusLocation) bool {
	// 최소 간격 체크 (10초)
	minInterval := 10 * time.Second

	lastSyncTime, err := udm.redisBusManager.GetLastESSyncTime(plateNo)
	if err == nil && !lastSyncTime.IsZero() {
		timeSinceLastSync := time.Since(lastSyncTime)
		if timeSinceLastSync < minInterval {
			udm.logger.Debugf("ES 전송 간격 부족 - 차량: %s, 경과시간: %v (최소: %v)",
				plateNo, timeSinceLastSync, minInterval)
			return false
		}
	}

	// 위치 변경 확인 (단순화)
	return udm.isLocationChanged(plateNo, bus)
}

// isLocationChanged 위치 변경 여부 확인 (단순화)
func (udm *SimplifiedUnifiedDataManager) isLocationChanged(plateNo string, bus models.BusLocation) bool {
	lastESData, err := udm.redisBusManager.GetLastESData(plateNo)
	if err != nil || lastESData == nil {
		udm.logger.Debugf("🆕 첫 ES 전송 또는 마지막 데이터 없음 - 차량: %s", plateNo)
		return true
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

	if currentOrder != lastOrder {
		udm.logger.Infof("🚏 정류장 순서 변경 감지 - 차량: %s, %d -> %d", plateNo, lastOrder, currentOrder)
		return true
	}

	// StationId 비교
	if bus.StationId != lastESData.StationId && bus.StationId > 0 && lastESData.StationId > 0 {
		udm.logger.Infof("🆔 StationId 변경 감지 - 차량: %s, %d -> %d", plateNo, lastESData.StationId, bus.StationId)
		return true
	}

	// NodeId 비교 (API2용)
	if bus.NodeId != lastESData.NodeId && bus.NodeId != "" && lastESData.NodeId != "" {
		udm.logger.Infof("🏷️ NodeId 변경 감지 - 차량: %s, %s -> %s", plateNo, lastESData.NodeId, bus.NodeId)
		return true
	}

	udm.logger.Debugf("🔄 위치 변경 없음 - 차량: %s (순서: %d, StationId: %d, NodeId: %s)",
		plateNo, currentOrder, bus.StationId, bus.NodeId)
	return false
}

// GetActiveBusesByRoute 노선별 활성 버스 조회
func (udm *SimplifiedUnifiedDataManager) GetActiveBusesByRoute(routeId int64) ([]string, error) {
	return udm.redisBusManager.GetActiveBusesByRoute(routeId)
}

// 인터페이스 구현 확인
var _ UnifiedDataManagerInterface = (*SimplifiedUnifiedDataManager)(nil)
