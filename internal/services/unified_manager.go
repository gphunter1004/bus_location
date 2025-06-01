// internal/services/unified_manager.go - Redis 중심 데이터 플로우로 수정
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

// UnifiedDataManagerWithRedis Redis 기반 통합 데이터 매니저 (단순화)
type UnifiedDataManagerWithRedis struct {
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

// NewUnifiedDataManagerWithRedis Redis 기반 통합 데이터 매니저 생성 (단순화)
func NewUnifiedDataManagerWithRedis(
	logger *utils.Logger,
	stationCache cache.StationCacheInterface,
	esService *storage.ElasticsearchService,
	redisBusManager *redis.RedisBusDataManager,
	duplicateChecker *storage.ElasticsearchDuplicateChecker,
	indexName string) *UnifiedDataManagerWithRedis {

	return &UnifiedDataManagerWithRedis{
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

// loadRecentESDataForFirstRun 첫 실행 시 ES에서 최근 데이터 로드
func (udm *UnifiedDataManagerWithRedis) loadRecentESDataForFirstRun() {
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

	udm.logger.Info("Redis 통합 모드 첫 실행 - Elasticsearch에서 최근 데이터 조회 중...")

	recentData, err := udm.duplicateChecker.GetRecentBusData(30)
	if err != nil {
		udm.logger.Errorf("Redis 통합 모드 첫 실행 ES 데이터 조회 실패: %v", err)
		udm.isFirstRun = false
		return
	}

	udm.recentESData = recentData
	udm.isFirstRun = false

	if len(recentData) > 0 {
		udm.logger.Infof("Redis 통합 모드 중복 체크용 데이터 로드 완료 - %d대 버스", len(recentData))
	} else {
		udm.logger.Info("Redis 통합 모드 첫 실행 - ES에 최근 데이터 없음")
	}
}

// isDuplicateDataForFirstRun 첫 실행 시 중복 데이터인지 확인
func (udm *UnifiedDataManagerWithRedis) isDuplicateDataForFirstRun(plateNo string, bus models.BusLocation) bool {
	if len(udm.recentESData) == 0 {
		return false
	}

	esData, found := udm.recentESData[plateNo]
	if !found {
		return false
	}

	isDuplicate := esData.IsDuplicateData(bus.StationSeq, bus.NodeOrd, bus.StationId, bus.NodeId)

	if isDuplicate {
		udm.logger.Infof("Redis 통합모드 중복 데이터 감지 - 차량: %s, 현재: StationSeq=%d/NodeOrd=%d, ES최종: %s",
			plateNo, bus.StationSeq, bus.NodeOrd, esData.LastUpdate.Format("15:04:05"))
	}

	return isDuplicate
}

// UpdateAPI1Data API1 데이터 업데이트 처리 (Redis 중심, 단순화)
func (udm *UnifiedDataManagerWithRedis) UpdateAPI1Data(busLocations []models.BusLocation) {
	if udm.isFirstRun {
		udm.loadRecentESDataForFirstRun()
	}

	now := time.Now()
	var esReadyBuses []models.BusLocation
	isFirstProcessing := udm.isFirstRun

	udm.logger.Infof("API1 데이터 처리 시작 - 총 %d건", len(busLocations))

	for _, bus := range busLocations {
		plateNo := bus.PlateNo
		if plateNo == "" {
			udm.logger.Warnf("API1 데이터에 차량번호 없음, 건너뛰기")
			continue
		}

		// RouteId 검증 (API1은 항상 유효해야 함)
		if bus.RouteId == 0 {
			udm.logger.Errorf("API1 데이터에 유효하지 않은 RouteId - 차량: %s, 건너뛰기", plateNo)
			continue
		}

		// 첫 실행 시 중복 체크
		if isFirstProcessing && udm.isDuplicateDataForFirstRun(plateNo, bus) {
			udm.updateInternalStateOnly(plateNo, bus, now, []string{"api1"})
			continue
		}

		// Redis에 업데이트하고 실제 위치 변경 여부 확인
		udm.logger.Debugf("Redis 저장 시도 - 차량: %s", plateNo)

		_, hasLocationChanged, err := udm.redisBusManager.UpdateBusData(
			bus, []string{"api1"})

		if err != nil {
			udm.logger.Errorf("Redis 업데이트 실패 (API1, 차량: %s): %v", plateNo, err)
			continue
		}

		// 위치 변경이 있으면 ES 전송 대상에 추가
		if hasLocationChanged {
			// 최종 중복 체크: ES 전송 전 마지막 확인
			if udm.shouldSendToES(plateNo, bus) {
				esReadyBuses = append(esReadyBuses, bus)
				udm.logger.Infof("ES 전송 대상 추가 - 차량: %s, StationId: %d, 정류장: %s",
					plateNo, bus.StationId, bus.NodeNm)
			} else {
				udm.logger.Debugf("최종 중복 체크에서 제외 - 차량: %s", plateNo)
			}
		} else {
			udm.logger.Debugf("Redis 저장 성공, 위치 변경 없음 - 차량: %s", plateNo)
		}

		udm.logger.Debugf("API1 처리 완료 - 차량: %s, Redis변경: %t, ES대상: %t",
			plateNo, hasLocationChanged, hasLocationChanged)
	}

	// ES 배치 전송
	if len(esReadyBuses) > 0 {
		udm.sendBatchToElasticsearch(esReadyBuses, "API1")
	}

	udm.logger.Infof("API1 배치 처리 완료 - 총: %d건, ES 전송: %d건", len(busLocations), len(esReadyBuses))
}

// UpdateAPI2Data API2 데이터 업데이트 처리 (Redis 중심, 정확한 ES 전송 조건)
func (udm *UnifiedDataManagerWithRedis) UpdateAPI2Data(busLocations []models.BusLocation) {
	if udm.isFirstRun {
		udm.loadRecentESDataForFirstRun()
	}

	now := time.Now()
	var esReadyBuses []models.BusLocation
	isFirstProcessing := udm.isFirstRun

	udm.logger.Infof("API2 데이터 처리 시작 - 총 %d건", len(busLocations))

	for _, bus := range busLocations {
		plateNo := bus.PlateNo
		if plateNo == "" {
			udm.logger.Warnf("API2 데이터에 차량번호 없음, 건너뛰기")
			continue
		}

		// RouteId 검증 (API2에서 추출되었는지 확인)
		if bus.RouteId == 0 {
			udm.logger.Warnf("API2 데이터에서 RouteId 추출 실패 - 차량: %s, RouteNm: %s, 건너뛰기", plateNo, bus.RouteNm)
			continue
		}

		// 첫 실행 시 중복 체크
		if isFirstProcessing && udm.isDuplicateDataForFirstRun(plateNo, bus) {
			udm.updateInternalStateOnly(plateNo, bus, now, []string{"api2"})
			continue
		}

		// 1. Redis에 무조건 업데이트하고 실제 위치 변경 여부 확인
		udm.logger.Debugf("Redis 저장 시도 - 차량: %s", plateNo)

		_, hasLocationChanged, err := udm.redisBusManager.UpdateBusData(
			bus, []string{"api2"})

		if err != nil {
			udm.logger.Errorf("Redis 업데이트 실패 (API2, 차량: %s): %v", plateNo, err)
			continue // Redis 실패 시 이 버스는 건너뛰기
		}

		// 2. 버스 추적 처리 (Redis 변경이 성공한 경우에만)
		if hasLocationChanged {
			cacheKey := bus.GetCacheKey()
			stationOrder := bus.GetStationOrder()

			// 실제 정류장 변경이 있는 경우에만 추적 업데이트
			changed, tripNumber := udm.busTracker.IsStationChanged(
				bus.PlateNo, int64(stationOrder), cacheKey, bus.TotalStations)

			bus.TripNumber = tripNumber

			if changed {
				// 3. 최종 중복 체크: ES 전송 전 마지막 확인
				if udm.shouldSendToES(plateNo, bus) {
					esReadyBuses = append(esReadyBuses, bus)
					udm.logger.Infof("ES 전송 대상 추가 - 차량: %s, NodeOrd: %d, 정류장: %s, GPS: (%.6f, %.6f)",
						plateNo, bus.NodeOrd, bus.NodeNm, bus.GpsLati, bus.GpsLong)
				} else {
					udm.logger.Debugf("최종 중복 체크에서 제외 - 차량: %s", plateNo)
				}
			} else {
				udm.logger.Debugf("Redis 위치 변경 있지만 추적 변경 없음 - 차량: %s", plateNo)
			}
		} else {
			// Redis에는 저장되었지만 위치 변경이 없음
			udm.busTracker.UpdateLastSeenTime(bus.PlateNo)
			udm.logger.Debugf("Redis 저장 성공, 위치 변경 없음 - 차량: %s", plateNo)
		}

		udm.logger.Debugf("API2 처리 완료 - 차량: %s, Redis변경: %t, ES대상: %t",
			plateNo, hasLocationChanged, hasLocationChanged)
	}

	// 4. ES 배치 전송
	if len(esReadyBuses) > 0 {
		udm.sendBatchToElasticsearch(esReadyBuses, "API2")
	}

	udm.logger.Infof("API2 배치 처리 완료 - 총: %d건, ES 전송: %d건", len(busLocations), len(esReadyBuses))
}

// updateInternalStateOnly 내부 상태만 업데이트 (중복 체크용, 단순화)
func (udm *UnifiedDataManagerWithRedis) updateInternalStateOnly(plateNo string, bus models.BusLocation, now time.Time, dataSources []string) {
	// Redis에 상태만 업데이트 (ES 전송하지 않음)
	udm.logger.Debugf("중복 데이터 - Redis 상태만 업데이트: 차량 %s", plateNo)

	_, _, err := udm.redisBusManager.UpdateBusData(bus, dataSources)
	if err != nil {
		udm.logger.Errorf("Redis 상태 업데이트 실패 (중복처리, 차량: %s): %v", plateNo, err)
	}
}

// sendBatchToElasticsearch ES 배치 전송
func (udm *UnifiedDataManagerWithRedis) sendBatchToElasticsearch(buses []models.BusLocation, source string) {
	if udm.esService == nil || len(buses) == 0 {
		return
	}

	udm.logger.Infof("ES 배치 전송 (%s): %d건", source, len(buses))

	// 상세 로그 (처음 3개만)
	for i, bus := range buses {
		if i >= 3 {
			break
		}
		udm.logger.Infof("ES 전송 [%d/%d] - 차량: %s, 노선: %s (RouteId: %d), 정류장: %s, 차수: %d (%s)",
			i+1, len(buses), bus.PlateNo, bus.RouteNm, bus.RouteId, bus.NodeNm, bus.TripNumber, source)
	}

	if err := udm.esService.BulkSendBusLocations(udm.indexName, buses); err != nil {
		udm.logger.Errorf("ES 배치 전송 실패 (%s): %v", source, err)
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
}

// StartPeriodicESSync 주기적 ES 동기화 시작
func (udm *UnifiedDataManagerWithRedis) StartPeriodicESSync() {
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

// StopPeriodicESSync 주기적 ES 동기화 중지
func (udm *UnifiedDataManagerWithRedis) StopPeriodicESSync() {
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
func (udm *UnifiedDataManagerWithRedis) syncRedisToES() {
	// Redis에서 미동기화된 데이터 조회
	changedBuses, err := udm.redisBusManager.GetChangedBusesForES()
	if err != nil {
		udm.logger.Errorf("Redis 변경 데이터 조회 실패: %v", err)
		return
	}

	if len(changedBuses) == 0 {
		udm.logger.Debugf("Redis -> ES 동기화: 변경된 데이터 없음")
		return
	}

	udm.logger.Infof("Redis -> ES 동기화: %d건 발견", len(changedBuses))

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

// CleanupOldData 정리 작업 (Redis 중심으로 단순화)
func (udm *UnifiedDataManagerWithRedis) CleanupOldData(maxAge time.Duration) int {
	// Redis 비활성 버스 정리
	cleanedCount, err := udm.redisBusManager.CleanupInactiveBuses(maxAge)
	if err != nil {
		udm.logger.Errorf("Redis 정리 실패: %v", err)
	}

	if cleanedCount > 0 {
		udm.logger.Infof("Redis 데이터 정리 완료 - 제거된 버스: %d대", cleanedCount)
	}

	return cleanedCount
}

// GetStatistics 통계 정보 반환
func (udm *UnifiedDataManagerWithRedis) GetStatistics() (int, int, int, int) {
	// Redis 통계 조회
	redisStats, err := udm.redisBusManager.GetBusStatistics()
	if err != nil {
		udm.logger.Errorf("Redis 통계 조회 실패: %v", err)
		return 0, 0, 0, 0
	}

	totalBuses := 0
	if total, ok := redisStats["total_buses"].(int); ok {
		totalBuses = total
	}

	// 현재는 Redis 중심이므로 API별 분리 통계는 단순화
	// 향후 Redis에서 데이터 소스별 통계 수집 가능
	api1Only := totalBuses / 3 // 임시 분배
	api2Only := totalBuses / 3 // 임시 분배
	both := totalBuses - api1Only - api2Only

	return totalBuses, api1Only, api2Only, both
}

// GetRedisStatistics Redis 통계 정보 반환
func (udm *UnifiedDataManagerWithRedis) GetRedisStatistics() (map[string]interface{}, error) {
	return udm.redisBusManager.GetBusStatistics()
}

// shouldSendToES ES 전송 전 최종 중복 체크
func (udm *UnifiedDataManagerWithRedis) shouldSendToES(plateNo string, bus models.BusLocation) bool {
	// 최소 간격 체크 (같은 버스가 너무 자주 ES에 전송되는 것 방지)
	minInterval := 10 * time.Second // 최소 10초 간격

	// Redis에서 마지막 ES 전송 시간 확인
	lastSyncTime, err := udm.redisBusManager.GetLastESSyncTime(plateNo)
	if err == nil && !lastSyncTime.IsZero() {
		timeSinceLastSync := time.Since(lastSyncTime)
		if timeSinceLastSync < minInterval {
			udm.logger.Debugf("ES 전송 간격 부족 - 차량: %s, 경과시간: %v (최소: %v)",
				plateNo, timeSinceLastSync, minInterval)
			return false
		}
	}

	// 위치 기반 중복 체크
	return udm.isLocationSignificantlyChanged(plateNo, bus)
}

// isLocationSignificantlyChanged 정류장 순서 기준으로 의미있는 변경인지 확인
func (udm *UnifiedDataManagerWithRedis) isLocationSignificantlyChanged(plateNo string, bus models.BusLocation) bool {
	// Redis에서 마지막 ES 전송 데이터 조회
	lastESData, err := udm.redisBusManager.GetLastESData(plateNo)
	if err != nil || lastESData == nil {
		udm.logger.Debugf("첫 ES 전송 또는 마지막 데이터 없음 - 차량: %s", plateNo)
		return true // 첫 번째 전송이거나 데이터가 없으면 전송
	}

	// 정류장 순서 변경 확인 (우선순위: NodeOrd > StationSeq)
	currentOrder := bus.GetStationOrder()
	var lastOrder int
	if lastESData.NodeOrd > 0 {
		lastOrder = lastESData.NodeOrd
	} else if lastESData.StationSeq > 0 {
		lastOrder = lastESData.StationSeq
	} else {
		// 마지막 데이터에 순서 정보가 없으면 전송
		udm.logger.Debugf("마지막 데이터에 순서 정보 없음 - 차량: %s", plateNo)
		return true
	}

	if currentOrder != lastOrder {
		udm.logger.Infof("정류장 순서 변경 감지 - 차량: %s, %d -> %d", plateNo, lastOrder, currentOrder)
		return true
	}

	// StationId 변경 확인 (정류장 순서가 같아도 다른 정류장일 수 있음)
	if bus.StationId != lastESData.StationId && bus.StationId > 0 && lastESData.StationId > 0 {
		udm.logger.Infof("StationId 변경 감지 - 차량: %s, %d -> %d", plateNo, lastESData.StationId, bus.StationId)
		return true
	}

	// NodeId 변경 확인 (API2용)
	if bus.NodeId != lastESData.NodeId && bus.NodeId != "" && lastESData.NodeId != "" {
		udm.logger.Infof("NodeId 변경 감지 - 차량: %s, %s -> %s", plateNo, lastESData.NodeId, bus.NodeId)
		return true
	}

	udm.logger.Debugf("정류장 위치 변경 없음 - 차량: %s (순서: %d, StationId: %d, NodeId: %s)",
		plateNo, currentOrder, bus.StationId, bus.NodeId)
	return false
}

// GetActiveBusesByRoute 노선별 활성 버스 조회
func (udm *UnifiedDataManagerWithRedis) GetActiveBusesByRoute(routeId int64) ([]string, error) {
	return udm.redisBusManager.GetActiveBusesByRoute(routeId)
}

// 인터페이스 구현 확인
var _ UnifiedDataManagerInterface = (*UnifiedDataManagerWithRedis)(nil)
