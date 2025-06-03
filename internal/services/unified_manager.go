// internal/services/unified_manager.go
package services

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"bus-tracker/config"
	"bus-tracker/internal/models"
	"bus-tracker/internal/services/cache"
	"bus-tracker/internal/services/redis"
	"bus-tracker/internal/services/storage"
	"bus-tracker/internal/services/tracker"
	"bus-tracker/internal/utils"
)

// UnifiedDataManager Redis 중심 통합 데이터 매니저 (tracker 전용 tripNumber 관리)
type UnifiedDataManager struct {
	config           *config.Config
	logger           *utils.Logger
	stationCache     cache.StationCacheInterface
	esService        *storage.ElasticsearchService
	redisBusManager  *redis.RedisBusDataManager
	duplicateChecker *storage.ElasticsearchDuplicateChecker
	indexName        string

	// 🔧 단순화된 버스 추적기 (tripNumber 전담 관리)
	simpleBusTracker *tracker.SimpleBusTracker

	// 최초 데이터 로딩 상태 관리
	initialLoadingDone int32
	initialLoadMutex   sync.Mutex
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
		logger:             logger,
		stationCache:       stationCache,
		esService:          esService,
		redisBusManager:    redisBusManager,
		duplicateChecker:   duplicateChecker,
		indexName:          indexName,
		initialLoadingDone: 0,
	}
}

// InitializeBusTracker BusTracker 초기화 (tripNumber 전담 관리)
func (udm *UnifiedDataManager) InitializeBusTracker(cfg *config.Config) {
	udm.config = cfg

	// 🔧 단순화된 버스 추적기 사용 (tripNumber 전담)
	udm.simpleBusTracker = tracker.NewSimpleBusTracker(cfg, udm.logger)
	udm.logger.Info("✅ SimpleBusTracker 초기화 완료 (tripNumber 전담 관리)")
}

// 🔧 PerformInitialDataLoading 최초 데이터 로딩 (tripNumber는 tracker에서만 관리)
func (udm *UnifiedDataManager) PerformInitialDataLoading(api1Client interface{}, api2Client interface{}) error {
	udm.initialLoadMutex.Lock()
	defer udm.initialLoadMutex.Unlock()

	if atomic.LoadInt32(&udm.initialLoadingDone) == 1 {
		udm.logger.Info("ℹ️ 최초 데이터 로딩 이미 완료됨")
		return nil
	}

	udm.logger.Info("🚀 최초 데이터 로딩 시작 (tripNumber는 tracker 전담)")
	startTime := time.Now()

	// Step 1: ES에서 최신 데이터 조회 (위치 비교용, tripNumber 제외)
	esData, err := udm.loadESLatestDataForComparison()
	if err != nil {
		udm.logger.Warnf("ES 데이터 조회 실패, 계속 진행: %v", err)
		esData = make(map[string]*storage.BusLastData)
	}

	// Step 2: API1 호출하여 Redis에 적재
	api1Data, err := udm.loadAPI1DataToRedis(api1Client)
	if err != nil {
		return fmt.Errorf("API1 데이터 로딩 실패: %v", err)
	}

	// Step 3: API2 호출하여 Redis에 적재
	api2Data, err := udm.loadAPI2DataToRedis(api2Client)
	if err != nil {
		return fmt.Errorf("API2 데이터 로딩 실패: %v", err)
	}

	// Step 4: tracker에서 tripNumber 할당
	allBusData := append(api1Data, api2Data...)
	trackerProcessedBuses := udm.processWithTracker(allBusData, "초기로딩")

	// Step 5: Redis vs ES 비교하여 선택적 ES 저장
	savedCount := udm.compareAndSaveToES(trackerProcessedBuses, esData)

	atomic.StoreInt32(&udm.initialLoadingDone, 1)

	udm.logger.Infof("✅ 최초 데이터 로딩 완료 - API1: %d건, API2: %d건, Tracker처리: %d건, ES저장: %d건 (소요: %v)",
		len(api1Data), len(api2Data), len(trackerProcessedBuses), savedCount, time.Since(startTime).Round(time.Millisecond))

	return nil
}

// 🔧 loadESLatestDataForComparison ES에서 최신 데이터 조회 (위치 비교용만)
func (udm *UnifiedDataManager) loadESLatestDataForComparison() (map[string]*storage.BusLastData, error) {
	udm.logger.Info("📥 Step 1: ES 최신 데이터 조회 중... (위치 비교용, tripNumber 제외)")

	if udm.duplicateChecker == nil {
		udm.logger.Warn("⚠️ ES 중복 체크 서비스 없음")
		return make(map[string]*storage.BusLastData), nil
	}

	// ES에서 최근 30분 데이터 조회
	esData, err := udm.duplicateChecker.GetRecentBusData(30)
	if err != nil {
		return nil, fmt.Errorf("ES 데이터 조회 실패: %v", err)
	}

	udm.logger.Infof("📥 ES 최신 데이터: %d건 (위치 비교용만)", len(esData))
	return esData, nil
}

// 🔧 loadAPI1DataToRedis API1 데이터를 Redis에 로딩
func (udm *UnifiedDataManager) loadAPI1DataToRedis(api1Client interface{}) ([]models.BusLocation, error) {
	udm.logger.Info("📥 Step 2: API1 데이터 로딩 중... ")

	// API1Client 타입 확인 및 호출
	client, ok := api1Client.(interface {
		FetchAllBusLocations([]string) ([]models.BusLocation, error)
	})
	if !ok {
		return nil, fmt.Errorf("API1 클라이언트 타입 오류")
	}

	// API1 데이터 가져오기
	busLocations, err := client.FetchAllBusLocations(udm.config.API1Config.RouteIDs)
	if err != nil {
		return nil, fmt.Errorf("API1 호출 실패: %v", err)
	}

	udm.logger.Infof("📥 API1 데이터 수신: %d건", len(busLocations))

	// 🔧 tripNumber 초기화 (tracker에서 나중에 설정)
	successCount := 0
	for i := range busLocations {
		if busLocations[i].PlateNo == "" || busLocations[i].RouteId == 0 {
			continue
		}

		// tripNumber는 0으로 초기화 (tracker에서 설정)
		busLocations[i].TripNumber = 0
		udm.logger.Debugf("🔧 API1 tripNumber 초기화 - %s: T0 (tracker에서 설정 예정)", busLocations[i].PlateNo)

		_, err := udm.redisBusManager.UpdateBusStatusOnly(busLocations[i], []string{"api1_initial"})
		if err != nil {
			udm.logger.Debugf("API1 Redis 저장 실패 - 차량: %s", busLocations[i].PlateNo)
			continue
		}
		successCount++
	}

	udm.logger.Infof("✅ API1 Redis 저장 완료: %d/%d건 ", successCount, len(busLocations))
	return busLocations, nil
}

// 🔧 loadAPI2DataToRedis API2 데이터를 Redis에 로딩
func (udm *UnifiedDataManager) loadAPI2DataToRedis(api2Client interface{}) ([]models.BusLocation, error) {
	udm.logger.Info("📥 Step 3: API2 데이터 로딩 중... ")

	// API2Client 타입 확인 및 호출
	client, ok := api2Client.(interface {
		FetchAllBusLocations([]string) ([]models.BusLocation, error)
	})
	if !ok {
		return nil, fmt.Errorf("API2 클라이언트 타입 오류")
	}

	// API2 데이터 가져오기
	busLocations, err := client.FetchAllBusLocations(udm.config.API2Config.RouteIDs)
	if err != nil {
		return nil, fmt.Errorf("API2 호출 실패: %v", err)
	}

	udm.logger.Infof("📥 API2 데이터 수신: %d건", len(busLocations))

	// 🔧 tripNumber 초기화 (tracker에서 나중에 설정)
	successCount := 0
	for i := range busLocations {
		if busLocations[i].PlateNo == "" || busLocations[i].RouteId == 0 {
			continue
		}

		// tripNumber는 0으로 초기화 (tracker에서 설정)
		busLocations[i].TripNumber = 0
		udm.logger.Debugf("🔧 API2 tripNumber 초기화 - %s: T0 (tracker에서 설정 예정)", busLocations[i].PlateNo)

		// API1 데이터와 병합
		mergedBus := udm.mergeWithRedisAPI1Data(busLocations[i])

		_, err := udm.redisBusManager.UpdateBusLocation(mergedBus, []string{"api2_initial"})
		if err != nil {
			udm.logger.Debugf("API2 Redis 저장 실패 - 차량: %s", busLocations[i].PlateNo)
			continue
		}
		successCount++
	}

	udm.logger.Infof("✅ API2 Redis 저장 완료: %d/%d건 ", successCount, len(busLocations))
	return busLocations, nil
}

// 🔧 processWithTracker tracker에서 tripNumber 할당 및 변경 감지 (상세 로깅)
func (udm *UnifiedDataManager) processWithTracker(busLocations []models.BusLocation, phase string) []models.BusLocation {
	udm.logger.Infof("🎯 Step 4: Tracker에서 tripNumber 할당 중... (%s)", phase)

	// tracker에서 변경된 버스만 반환 (tripNumber 포함)
	changedBuses := udm.simpleBusTracker.FilterChangedBuses(busLocations)

	// 🔧 tracker에서 설정한 tripNumber를 Redis에 반영
	for _, bus := range changedBuses {
		if bus.TripNumber > 0 {
			// Redis에서 최신 데이터 가져와서 tripNumber만 업데이트
			if redisData, err := udm.redisBusManager.GetBusLocationData(bus.PlateNo); err == nil && redisData != nil {
				oldTripNumber := redisData.BusLocation.TripNumber
				updatedBus := redisData.BusLocation
				updatedBus.TripNumber = bus.TripNumber

				// Redis에 다시 저장
				if bus.IsAPI2Data() {
					udm.redisBusManager.UpdateBusLocation(updatedBus, []string{"tracker_tripnumber"})
				} else {
					udm.redisBusManager.UpdateBusStatusOnly(updatedBus, []string{"tracker_tripnumber"})
				}

				if oldTripNumber != bus.TripNumber {
					udm.logger.Infof("🔢 [TRIPNUMBER_SYNC] Tracker→Redis 동기화 - 차량:%s, T%d→T%d (%s)",
						bus.PlateNo, oldTripNumber, bus.TripNumber, phase)
				} else {
					udm.logger.Debugf("🔒 [TRIPNUMBER_SAME] Tracker→Redis 동일값 - 차량:%s, T%d (%s)",
						bus.PlateNo, bus.TripNumber, phase)
				}
			}
		}
	}

	udm.logger.Infof("✅ Tracker 처리 완료 - 전체: %d건, 변경: %d건 (%s)", len(busLocations), len(changedBuses), phase)
	return changedBuses
}

// 🔧 compareAndSaveToES Redis vs ES 비교하여 선택적 저장
func (udm *UnifiedDataManager) compareAndSaveToES(changedBuses []models.BusLocation, esData map[string]*storage.BusLastData) int {
	udm.logger.Info("🔍 Step 5: Redis vs ES 비교 및 선택적 저장...")

	var busesToSave []models.BusLocation
	skipCount := 0

	for _, bus := range changedBuses {
		plateNo := bus.PlateNo

		// Redis에서 최신 데이터 조회 (tripNumber 포함)
		redisData, err := udm.redisBusManager.GetBusLocationData(plateNo)
		if err != nil || redisData == nil {
			udm.logger.Debugf("Redis 데이터 없음 - 차량: %s", plateNo)
			continue
		}

		// ES와 위치 비교
		if udm.isLocationSameAsES(plateNo, redisData.BusLocation, esData) {
			skipCount++
			udm.logger.Debugf("🔄 위치 동일, ES 저장 스킵 - 차량: %s", plateNo)
			continue
		}

		// 위치가 다르면 ES 저장 대상
		busesToSave = append(busesToSave, redisData.BusLocation)
		udm.logger.Infof("🆕 위치 변경, ES 저장 대상 - 차량: %s, TripNumber: T%d", plateNo, redisData.BusLocation.TripNumber)
	}

	// ES 벌크 저장
	if len(busesToSave) > 0 {
		udm.logger.Infof("📤 ES 저장 시작: %d건 (스킵: %d건)", len(busesToSave), skipCount)

		if err := udm.esService.BulkSendBusLocations(udm.indexName, busesToSave); err != nil {
			udm.logger.Errorf("❌ ES 저장 실패: %v", err)
			return 0
		}

		// Redis 동기화 마킹
		var plateNos []string
		for _, bus := range busesToSave {
			plateNos = append(plateNos, bus.PlateNo)
		}
		udm.redisBusManager.MarkAsSynced(plateNos)

		udm.logger.Infof("✅ ES 저장 완료: %d건 (tracker에서 tripNumber 관리)", len(busesToSave))
	} else {
		udm.logger.Info("ℹ️ ES 저장할 데이터 없음 (모든 데이터 위치 동일)")
	}

	return len(busesToSave)
}

// isLocationSameAsES Redis 데이터와 ES 데이터의 위치가 동일한지 확인
func (udm *UnifiedDataManager) isLocationSameAsES(plateNo string, redisBus models.BusLocation, esData map[string]*storage.BusLastData) bool {
	esItem, exists := esData[plateNo]
	if !exists {
		// ES에 데이터 없으면 새로운 데이터로 간주
		return false
	}

	// 위치 비교 (우선순위: NodeOrd > StationSeq > StationId)
	var redisPosition, esPosition int64

	// Redis 위치 추출
	if redisBus.NodeOrd > 0 {
		redisPosition = int64(redisBus.NodeOrd)
	} else if redisBus.StationSeq > 0 {
		redisPosition = int64(redisBus.StationSeq)
	} else {
		redisPosition = redisBus.StationId
	}

	// ES 위치 추출
	if esItem.NodeOrd > 0 {
		esPosition = int64(esItem.NodeOrd)
	} else if esItem.StationSeq > 0 {
		esPosition = int64(esItem.StationSeq)
	} else {
		esPosition = esItem.StationId
	}

	return redisPosition == esPosition
}

// 🔧 mergeWithRedisAPI1Data API1과 API2 데이터 병합
func (udm *UnifiedDataManager) mergeWithRedisAPI1Data(api2Bus models.BusLocation) models.BusLocation {
	redisData, err := udm.redisBusManager.GetBusLocationData(api2Bus.PlateNo)
	if err != nil || redisData == nil {
		return api2Bus
	}

	mergedBus := api2Bus

	// API1 상태정보 병합
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

	return mergedBus
}

// IsInitialLoadingDone 최초 로딩 완료 여부 확인
func (udm *UnifiedDataManager) IsInitialLoadingDone() bool {
	return atomic.LoadInt32(&udm.initialLoadingDone) == 1
}

// 🔧 일반 운영 모드 (최초 로딩 완료 후) - tripNumber는 변경하지 않음
// UpdateAPI1Data API1 상태정보 처리 (일반 모드 - tripNumber는 tracker에서만 관리)
func (udm *UnifiedDataManager) UpdateAPI1Data(busLocations []models.BusLocation) {
	if len(busLocations) == 0 {
		return
	}

	// 최초 로딩이 완료된 후에만 일반 처리
	if !udm.IsInitialLoadingDone() {
		udm.logger.Debug("최초 로딩 미완료 - API1 일반 처리 건너뛰기")
		return
	}

	// 🔧 모든 버스에 대해 목격 시간 업데이트 (tripNumber는 건드리지 않음)
	for _, bus := range busLocations {
		if bus.PlateNo != "" {
			udm.simpleBusTracker.UpdateLastSeenTime(bus.PlateNo)
		}
	}

	// 🔧 tripNumber는 건드리지 않고 Redis에 상태정보만 업데이트
	processedCount := 0
	for _, bus := range busLocations {
		if bus.PlateNo == "" || bus.RouteId == 0 {
			continue
		}

		// 🔧 tripNumber는 전혀 설정하지 않음 - tracker 전담
		// bus 구조체의 기본값(0) 그대로 사용

		_, err := udm.redisBusManager.UpdateBusStatusOnly(bus, []string{"api1"})
		if err == nil {
			processedCount++
		}
	}

	udm.logger.Infof("API1 일반처리: 수신=%d, 처리=%d (tripNumber 미관여)", len(busLocations), processedCount)
}

// UpdateAPI2Data API2 위치정보 처리 (일반 모드 - tripNumber는 tracker에서만 관리)
func (udm *UnifiedDataManager) UpdateAPI2Data(busLocations []models.BusLocation) {
	if len(busLocations) == 0 {
		return
	}

	// 최초 로딩이 완료된 후에만 일반 처리
	if !udm.IsInitialLoadingDone() {
		udm.logger.Debug("최초 로딩 미완료 - API2 일반 처리 건너뛰기")
		return
	}

	// 🔧 모든 버스에 대해 목격 시간 업데이트 (tripNumber는 건드리지 않음)
	for _, bus := range busLocations {
		if bus.PlateNo != "" {
			udm.simpleBusTracker.UpdateLastSeenTime(bus.PlateNo)
		}
	}

	// 🔧 tracker에서 변경된 버스만 필터링 (여기서 tripNumber가 할당됨)
	changedBuses := udm.simpleBusTracker.FilterChangedBuses(busLocations)

	processedCount := 0
	locationChangedCount := 0
	var locationChangedPlates []string

	for _, bus := range busLocations {
		if bus.PlateNo == "" || bus.RouteId == 0 {
			continue
		}

		// 🔧 tripNumber는 설정하지 않음 - tracker에서 관리하도록 위임
		// API1 데이터와 병합
		mergedBus := udm.mergeWithRedisAPI1Data(bus)

		locationChanged, err := udm.redisBusManager.UpdateBusLocation(mergedBus, []string{"api2"})
		if err != nil {
			continue
		}

		processedCount++
		if locationChanged {
			locationChangedCount++
			locationChangedPlates = append(locationChangedPlates, bus.PlateNo)
		}
	}

	// ES 전송 (tracker에서 변경 감지되고 tripNumber가 할당된 버스만)
	if len(changedBuses) > 0 {
		udm.sendBulkToElasticsearch(changedBuses)
	}

	udm.logger.Infof("API2 일반처리: 수신=%d, 처리=%d, 위치변경=%d, Tracker변경=%d, ES전송=%d",
		len(busLocations), processedCount, locationChangedCount, len(changedBuses), len(changedBuses))
}

// sendBulkToElasticsearch ES 벌크 전송 (tracker에서 tripNumber 할당된 버스만)
func (udm *UnifiedDataManager) sendBulkToElasticsearch(changedBuses []models.BusLocation) {
	if len(changedBuses) == 0 || udm.esService == nil {
		return
	}

	udm.logger.Infof("📤 ES 전송 시작: %d건 (tracker에서 tripNumber 할당 완료)", len(changedBuses))

	// 🔧 changedBuses는 이미 tracker에서 tripNumber가 할당된 상태
	// Redis와 별도로 동기화할 필요 없이 바로 ES 전송
	if err := udm.esService.BulkSendBusLocations(udm.indexName, changedBuses); err != nil {
		udm.logger.Errorf("ES 전송 실패: %v", err)
		return
	}

	// Redis에서 해당 버스들의 tripNumber를 tracker 값으로 업데이트
	for _, bus := range changedBuses {
		if redisData, err := udm.redisBusManager.GetBusLocationData(bus.PlateNo); err == nil && redisData != nil {
			if redisData.TripNumber != bus.TripNumber {
				updatedBus := redisData.BusLocation
				updatedBus.TripNumber = bus.TripNumber

				if bus.IsAPI2Data() {
					udm.redisBusManager.UpdateBusLocation(updatedBus, []string{"tracker_sync"})
				} else {
					udm.redisBusManager.UpdateBusStatusOnly(updatedBus, []string{"tracker_sync"})
				}

				udm.logger.Debugf("🔢 Redis tripNumber 동기화 - %s: T%d (tracker→redis)",
					bus.PlateNo, bus.TripNumber)
			}
		}
	}

	// ES 동기화 마킹
	var syncPlateNos []string
	for _, bus := range changedBuses {
		syncPlateNos = append(syncPlateNos, bus.PlateNo)
	}
	udm.redisBusManager.MarkAsSynced(syncPlateNos)

	udm.logger.Infof("✅ ES 전송 완료: %d건 (tracker 전담 tripNumber 관리)", len(changedBuses))
}

// CleanupOldData 정리 작업
func (udm *UnifiedDataManager) CleanupOldData(maxAge time.Duration) int {
	cleanedCount, _ := udm.redisBusManager.CleanupInactiveBuses(maxAge)

	if udm.simpleBusTracker != nil {
		trackerCleaned := udm.simpleBusTracker.CleanupMissingBuses()
		cleanedCount += trackerCleaned

		// 주기적으로 추적기 통계 출력
		udm.simpleBusTracker.PrintStatistics()
	}

	return cleanedCount
}

// StartPeriodicESSync ES 동기화 시작
func (udm *UnifiedDataManager) StartPeriodicESSync() {
	udm.logger.Info("ℹ️ 주기적 ES 동기화 비활성화 (실시간 처리 모드)")
}

// StopPeriodicESSync ES 동기화 중지
func (udm *UnifiedDataManager) StopPeriodicESSync() {
	udm.logger.Debug("ℹ️ ES 동기화 중지")
}

// SetTripNumber 특정 버스의 tripNumber 강제 설정 (tracker를 통해서만 + 상세 로깅)
func (udm *UnifiedDataManager) SetTripNumber(plateNo string, tripNumber int) error {
	// 1. tracker에서 tripNumber 설정
	if udm.simpleBusTracker != nil {
		udm.simpleBusTracker.SetTripNumber(plateNo, tripNumber)
		udm.logger.Infof("🔢 [TRIPNUMBER_MANUAL] Tracker 수동설정 - 차량:%s, T%d (소스:수동요청)", plateNo, tripNumber)
	}

	// 2. Redis 업데이트
	redisData, err := udm.redisBusManager.GetBusLocationData(plateNo)
	if err != nil || redisData == nil {
		return fmt.Errorf("Redis에서 차량 데이터 없음: %s", plateNo)
	}

	oldTripNumber := redisData.BusLocation.TripNumber

	updatedBus := redisData.BusLocation
	updatedBus.TripNumber = tripNumber

	_, err = udm.redisBusManager.UpdateBusLocation(updatedBus, []string{"manual_tripnumber_update"})
	if err != nil {
		udm.logger.Errorf("❌ 수동 tripNumber Redis 업데이트 실패 - 차량: %s, 오류: %v", plateNo, err)
		return fmt.Errorf("Redis 업데이트 실패: %v", err)
	}

	if oldTripNumber != tripNumber {
		udm.logger.Infof("🔢 [TRIPNUMBER_SYNC] 수동설정 Tracker→Redis - 차량:%s, T%d→T%d (소스:수동동기화)",
			plateNo, oldTripNumber, tripNumber)
	} else {
		udm.logger.Infof("🔒 [TRIPNUMBER_SAME] 수동설정 동일값 - 차량:%s, T%d (변경없음)", plateNo, tripNumber)
	}

	return nil
}

// GetTripNumber 특정 버스의 현재 tripNumber 조회
func (udm *UnifiedDataManager) GetTripNumber(plateNo string) (int, error) {
	// 1. tracker에서 먼저 조회
	if udm.simpleBusTracker != nil {
		if trackerTripNumber := udm.simpleBusTracker.GetTripNumber(plateNo); trackerTripNumber > 0 {
			udm.logger.Debugf("🎯 Tracker에서 tripNumber 조회 - %s: T%d", plateNo, trackerTripNumber)
			return trackerTripNumber, nil
		}
	}

	// 2. Redis에서 조회 (fallback)
	redisData, err := udm.redisBusManager.GetBusLocationData(plateNo)
	if err != nil || redisData == nil {
		return 0, fmt.Errorf("Tracker 및 Redis에서 차량 데이터 없음: %s", plateNo)
	}

	udm.logger.Debugf("📦 Redis에서 tripNumber 조회 - %s: T%d", plateNo, redisData.BusLocation.TripNumber)
	return redisData.BusLocation.TripNumber, nil
}

// 인터페이스 구현 확인
var _ UnifiedDataManagerInterface = (*UnifiedDataManager)(nil)
