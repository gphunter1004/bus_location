// internal/services/unified_manager.go - 메모리 사용 제거, Redis 전환
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
	UpdateAPI1Data(busLocations []models.BusLocation)
	UpdateAPI2Data(busLocations []models.BusLocation)
	CleanupOldData(maxAge time.Duration) int
	StartPeriodicESSync()
	StopPeriodicESSync()
}

// UnifiedDataManager Redis 중심 통합 데이터 매니저
type UnifiedDataManager struct {
	config           *config.Config
	logger           *utils.Logger
	stationCache     cache.StationCacheInterface
	esService        *storage.ElasticsearchService
	redisBusManager  *redis.RedisBusDataManager
	duplicateChecker *storage.ElasticsearchDuplicateChecker
	indexName        string
	busTracker       *tracker.BusTrackerWithDuplicateCheck

	// 첫 실행 중복 체크 (필수 - ES 중복 방지용)
	isFirstRun    bool
	firstRunMutex sync.Mutex
	recentESData  map[string]*storage.BusLastData
}

// NewUnifiedDataManager 통합 데이터 매니저 생성
func NewUnifiedDataManager(
	logger *utils.Logger,
	stationCache cache.StationCacheInterface,
	esService *storage.ElasticsearchService,
	redisBusManager *redis.RedisBusDataManager,
	duplicateChecker *storage.ElasticsearchDuplicateChecker,
	indexName string) *UnifiedDataManager {

	return &UnifiedDataManager{
		logger:           logger,
		stationCache:     stationCache,
		esService:        esService,
		redisBusManager:  redisBusManager,
		duplicateChecker: duplicateChecker,
		indexName:        indexName,
		isFirstRun:       true,
		recentESData:     make(map[string]*storage.BusLastData),
	}
}

// InitializeBusTracker BusTracker 초기화
func (udm *UnifiedDataManager) InitializeBusTracker(cfg *config.Config) {
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

// UpdateAPI1Data API1 상태정보 처리 - Redis에만 저장
func (udm *UnifiedDataManager) UpdateAPI1Data(busLocations []models.BusLocation) {
	if len(busLocations) == 0 {
		return
	}

	enrichedBuses := udm.enrichBusesWithTripNumber(busLocations, "API1")

	processedCount := 0
	statusUpdatedCount := 0
	var statusUpdatedVehicles []string

	for _, bus := range enrichedBuses {
		if bus.PlateNo == "" || bus.RouteId == 0 {
			continue
		}

		// Redis 상태 업데이트만 (메모리 버퍼 제거)
		statusUpdated, err := udm.redisBusManager.UpdateBusStatusOnly(bus, []string{"api1"})
		if err != nil {
			udm.logger.Errorf("Redis 상태 업데이트 실패 - 차량: %s, 오류: %v", bus.PlateNo, err)
			continue
		}

		processedCount++

		if statusUpdated {
			statusUpdatedCount++
			statusUpdatedVehicles = append(statusUpdatedVehicles, bus.PlateNo)
		}
	}

	// 로그 출력 (병합버퍼 제거)
	if statusUpdatedCount > 0 {
		udm.logger.Infof("API1 상태처리: 수신=%d, 처리=%d, 상태갱신=%d(차량:%s)",
			len(busLocations), processedCount, statusUpdatedCount,
			udm.formatVehicleList(statusUpdatedVehicles))
	} else {
		udm.logger.Infof("API1 상태처리: 수신=%d, 처리=%d, 상태변경=0",
			len(busLocations), processedCount)
	}
}

// UpdateAPI2Data API2 위치정보 처리 - Redis 기반 병합 + 벌크 전송
func (udm *UnifiedDataManager) UpdateAPI2Data(busLocations []models.BusLocation) {
	if len(busLocations) == 0 {
		return
	}

	if udm.isFirstRun {
		udm.loadRecentESDataForFirstRun()
	}

	enrichedBuses := udm.enrichBusesWithTripNumber(busLocations, "API2")

	processedCount := 0
	locationChangedCount := 0
	duplicateCount := 0
	var locationChangedPlates []string
	var duplicateVehicles []string

	// Step 1: Redis에 저장하고 위치 변경된 차량들 수집
	for _, bus := range enrichedBuses {
		if bus.PlateNo == "" || bus.RouteId == 0 {
			continue
		}

		// 첫 실행 시 중복 체크 (필수 - ES 중복 방지)
		if udm.isDuplicateDataForFirstRun(bus.PlateNo, bus) {
			duplicateCount++
			duplicateVehicles = append(duplicateVehicles, bus.PlateNo)
			udm.redisBusManager.UpdateBusLocation(bus, []string{"api2"})
			continue
		}

		// 🔧 API1 상태정보 Redis 기반 병합 (메모리 버퍼 제거)
		mergedBus := udm.mergeWithRedisAPI1Data(bus)

		// Redis 위치정보 업데이트 (병합된 데이터로)
		locationChanged, err := udm.redisBusManager.UpdateBusLocation(mergedBus, []string{"api2"})
		if err != nil {
			udm.logger.Errorf("Redis 위치 업데이트 실패 - 차량: %s, 오류: %v", bus.PlateNo, err)
			continue
		}

		processedCount++

		// 위치 변경된 차량 수집
		if locationChanged {
			locationChangedCount++
			locationChangedPlates = append(locationChangedPlates, bus.PlateNo)
		}
	}

	// Step 2: 위치 변경된 차량들을 Redis에서 읽어서 벌크 전송
	if len(locationChangedPlates) > 0 {
		udm.sendBulkToElasticsearch(locationChangedPlates)
	}

	// 로그 출력
	if duplicateCount > 0 {
		udm.logger.Infof("API2 위치처리: 수신=%d, 처리=%d, 중복=%d(차량:%s), 위치변경=%d, ES벌크전송=%d",
			len(busLocations), processedCount, duplicateCount,
			udm.formatVehicleList(duplicateVehicles), locationChangedCount, len(locationChangedPlates))
	} else if locationChangedCount > 0 {
		udm.logger.Infof("API2 위치처리: 수신=%d, 처리=%d, 위치변경=%d(차량:%s), ES벌크전송=%d",
			len(busLocations), processedCount, locationChangedCount,
			udm.formatVehicleList(locationChangedPlates), len(locationChangedPlates))
	} else {
		udm.logger.Infof("API2 위치처리: 수신=%d, 처리=%d, 위치변경=0, ES전송=0",
			len(busLocations), processedCount)
	}
}

// 🔧 Redis 기반 API1 상태정보 병합 (메모리 버퍼 대체)
func (udm *UnifiedDataManager) mergeWithRedisAPI1Data(api2Bus models.BusLocation) models.BusLocation {
	// Redis에서 현재 버스의 최신 데이터 조회
	redisData, err := udm.redisBusManager.GetBusLocationData(api2Bus.PlateNo)
	if err != nil || redisData == nil {
		// Redis에 데이터가 없으면 API2 데이터 그대로 사용
		udm.logger.Debugf("Redis 데이터 없음 - 차량: %s, API2 데이터 사용", api2Bus.PlateNo)
		return api2Bus
	}

	// Redis에서 가져온 API1 상태정보와 API2 위치정보 병합
	mergedBus := api2Bus

	// API1의 상태정보 병합 (Redis에서 가져온 값, 유효한 값만)
	if redisData.Crowded > 0 {
		mergedBus.Crowded = redisData.Crowded
	}
	if redisData.LowPlate > 0 {
		mergedBus.LowPlate = redisData.LowPlate
	}
	if redisData.RemainSeatCnt >= 0 {
		mergedBus.RemainSeatCnt = redisData.RemainSeatCnt
	}
	if redisData.RouteTypeCd > 0 {
		mergedBus.RouteTypeCd = redisData.RouteTypeCd
	}
	if redisData.StateCd > 0 {
		mergedBus.StateCd = redisData.StateCd
	}
	if redisData.TaglessCd > 0 {
		mergedBus.TaglessCd = redisData.TaglessCd
	}
	if redisData.VehId > 0 {
		mergedBus.VehId = redisData.VehId
	}

	udm.logger.Debugf("🔗 Redis 기반 데이터 병합 - 차량: %s", api2Bus.PlateNo)
	return mergedBus
}

// sendBulkToElasticsearch 위치 변경된 버스들을 Redis에서 읽어서 벌크 전송
func (udm *UnifiedDataManager) sendBulkToElasticsearch(plateNos []string) {
	if len(plateNos) == 0 || udm.esService == nil {
		return
	}

	var esReadyBuses []models.BusLocation
	var failedPlates []string

	// Redis에서 최신 데이터 읽기
	for _, plateNo := range plateNos {
		redisData, err := udm.redisBusManager.GetBusLocationData(plateNo)
		if err != nil {
			udm.logger.Errorf("Redis 데이터 조회 실패 - 차량: %s, 오류: %v", plateNo, err)
			failedPlates = append(failedPlates, plateNo)
			continue
		}

		if redisData != nil {
			esReadyBuses = append(esReadyBuses, redisData.BusLocation)
		} else {
			udm.logger.Warnf("Redis에서 데이터 없음 - 차량: %s", plateNo)
			failedPlates = append(failedPlates, plateNo)
		}
	}

	if len(failedPlates) > 0 {
		udm.logger.Warnf("Redis 데이터 조회 실패: %d건", len(failedPlates))
	}

	// ES 벌크 전송
	if len(esReadyBuses) > 0 {
		udm.logger.Infof("📤 ES 벌크 전송 시작: %d건 (Redis 기반)", len(esReadyBuses))

		// 처음 3개만 상세 로그
		for i, bus := range esReadyBuses {
			if i < 3 {
				udm.logger.Infof("📋 ES 전송[%d]: 차량=%s, 노선=%s, 위치=%s(%d/%d), 좌석=%d, 혼잡=%d, 상태=%d, 차수=T%d",
					i+1, bus.PlateNo, bus.RouteNm, bus.NodeNm,
					udm.getStationOrder(bus), bus.TotalStations,
					bus.RemainSeatCnt, bus.Crowded, bus.StateCd, bus.TripNumber)
			} else if i == 3 {
				udm.logger.Infof("📋 ... 외 %d건 더", len(esReadyBuses)-3)
				break
			}
		}

		start := time.Now()
		if err := udm.esService.BulkSendBusLocations(udm.indexName, esReadyBuses); err != nil {
			udm.logger.Errorf("❌ ES 벌크 전송 실패: %v", err)
			return
		}
		duration := time.Since(start)

		// ES 동기화 마킹
		var syncPlateNos []string
		for _, bus := range esReadyBuses {
			syncPlateNos = append(syncPlateNos, bus.PlateNo)
		}

		if err := udm.redisBusManager.MarkAsSynced(syncPlateNos); err != nil {
			udm.logger.Errorf("Redis 동기화 마킹 실패: %v", err)
		}

		udm.logger.Infof("✅ ES 벌크 전송 완료: %d건 (소요: %v)",
			len(esReadyBuses), duration.Round(time.Millisecond))
	}
}

// enrichBusesWithTripNumber BusTracker로 TripNumber 설정
func (udm *UnifiedDataManager) enrichBusesWithTripNumber(busLocations []models.BusLocation, source string) []models.BusLocation {
	if udm.busTracker == nil {
		udm.logger.Errorf("BusTracker가 초기화되지 않았습니다")
		return busLocations
	}

	// LastSeenTime 업데이트
	for _, bus := range busLocations {
		udm.busTracker.UpdateLastSeenTime(bus.PlateNo)
	}

	// TripNumber 설정
	enrichedBuses := make([]models.BusLocation, len(busLocations))
	for i, bus := range busLocations {
		currentPosition := bus.StationId
		if currentPosition <= 0 {
			currentPosition = int64(bus.GetStationOrder())
		}

		cacheKey := bus.GetCacheKey()
		_, tripNumber := udm.busTracker.IsStationChanged(bus.PlateNo, currentPosition, cacheKey, bus.TotalStations)

		enrichedBus := bus
		enrichedBus.TripNumber = tripNumber
		enrichedBuses[i] = enrichedBus
	}

	udm.logger.Debugf("🔢 %s TripNumber 설정 완료: %d건", source, len(enrichedBuses))
	return enrichedBuses
}

// loadRecentESDataForFirstRun 첫 실행 시 ES에서 최근 데이터 로드 (필수 - ES 중복 방지)
func (udm *UnifiedDataManager) loadRecentESDataForFirstRun() {
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

// isDuplicateDataForFirstRun 첫 실행 시 중복 데이터 확인 (필수 - ES 중복 방지)
func (udm *UnifiedDataManager) isDuplicateDataForFirstRun(plateNo string, bus models.BusLocation) bool {
	if len(udm.recentESData) == 0 {
		return false
	}

	esData, found := udm.recentESData[plateNo]
	if !found {
		return false
	}

	isDup := esData.IsDuplicateData(bus.StationSeq, bus.NodeOrd, bus.StationId, bus.NodeId)
	if isDup {
		udm.logger.Infof("🔄 중복 데이터 감지 - 차량: %s", plateNo)
	}
	return isDup
}

// getStationOrder 정류장 순서 추출
func (udm *UnifiedDataManager) getStationOrder(bus models.BusLocation) int {
	if bus.NodeOrd > 0 {
		return bus.NodeOrd
	} else if bus.StationSeq > 0 {
		return bus.StationSeq
	}
	return 0
}

// CleanupOldData 정리 작업
func (udm *UnifiedDataManager) CleanupOldData(maxAge time.Duration) int {
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

// StartPeriodicESSync ES 동기화 시작 (비활성화)
func (udm *UnifiedDataManager) StartPeriodicESSync() {
	udm.logger.Info("ℹ️ 주기적 ES 동기화 비활성화 - API2 위치변경시 벌크 전송")
}

// StopPeriodicESSync ES 동기화 중지 (비활성화)
func (udm *UnifiedDataManager) StopPeriodicESSync() {
	udm.logger.Debug("ℹ️ ES 동기화 중지 - 비활성화 상태")
}

// formatVehicleList 차량 목록 포맷팅
func (udm *UnifiedDataManager) formatVehicleList(vehicles []string) string {
	if len(vehicles) == 0 {
		return "없음"
	}
	if len(vehicles) <= 2 {
		return strings.Join(vehicles, ",")
	}
	return fmt.Sprintf("%s 외 %d대", strings.Join(vehicles[:1], ","), len(vehicles)-1)
}

// 인터페이스 구현 확인
var _ UnifiedDataManagerInterface = (*UnifiedDataManager)(nil)
