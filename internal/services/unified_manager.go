// internal/services/unified_manager.go - 새로운 최초 데이터 로딩 플로우 (tripNumber 로깅 강화)
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

// UnifiedDataManager Redis 중심 통합 데이터 매니저
type UnifiedDataManager struct {
	config           *config.Config
	logger           *utils.Logger
	stationCache     cache.StationCacheInterface
	esService        *storage.ElasticsearchService
	redisBusManager  *redis.RedisBusDataManager
	duplicateChecker *storage.ElasticsearchDuplicateChecker
	indexName        string

	// 트립카운트 전용 BusTracker
	busTracker *tracker.BusTrackerWithDuplicateCheck

	// 🔧 최초 데이터 로딩 상태 관리
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

// InitializeBusTracker BusTracker 초기화
func (udm *UnifiedDataManager) InitializeBusTracker(cfg *config.Config) {
	udm.config = cfg

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
}

// 🔧 PerformInitialDataLoading 최초 데이터 로딩 (API1 → API2 → ES 비교)
func (udm *UnifiedDataManager) PerformInitialDataLoading(api1Client interface{}, api2Client interface{}) error {
	udm.initialLoadMutex.Lock()
	defer udm.initialLoadMutex.Unlock()

	if atomic.LoadInt32(&udm.initialLoadingDone) == 1 {
		udm.logger.Info("ℹ️ 최초 데이터 로딩 이미 완료됨")
		return nil
	}

	udm.logger.Info("🚀 최초 데이터 로딩 시작 - API1 → API2 → ES 비교")
	startTime := time.Now()

	// Step 1: API1 호출하여 Redis에 적재
	api1Data, err := udm.loadAPI1DataToRedis(api1Client)
	if err != nil {
		return fmt.Errorf("API1 데이터 로딩 실패: %v", err)
	}

	// Step 2: API2 호출하여 Redis에 적재
	api2Data, err := udm.loadAPI2DataToRedis(api2Client)
	if err != nil {
		return fmt.Errorf("API2 데이터 로딩 실패: %v", err)
	}

	// Step 3: ES 최신 데이터 조회 및 tripNumber 캐시 로드
	esData, err := udm.loadESLatestData()
	if err != nil {
		udm.logger.Warnf("ES 데이터 조회 실패, 계속 진행: %v", err)
		esData = make(map[string]*storage.BusLastData)
	}

	// 🔧 Step 4: tripNumber를 ES 최신값으로 설정 (비교 전에 먼저 실행)
	udm.updateTripNumbersFromES(esData)

	// 🔧 Step 5: Redis vs ES 비교하여 선택적 ES 저장 (tripNumber 설정 후)
	savedCount := udm.compareAndSaveToES(api1Data, api2Data, esData)

	atomic.StoreInt32(&udm.initialLoadingDone, 1)

	udm.logger.Infof("✅ 최초 데이터 로딩 완료 - API1: %d건, API2: %d건, ES 저장: %d건 (소요: %v)",
		len(api1Data), len(api2Data), savedCount, time.Since(startTime).Round(time.Millisecond))

	return nil
}

// 🔧 loadAPI1DataToRedis API1 데이터를 Redis에 로딩
func (udm *UnifiedDataManager) loadAPI1DataToRedis(api1Client interface{}) ([]models.BusLocation, error) {
	udm.logger.Info("📥 Step 1: API1 데이터 로딩 중...")

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

	// Redis에 저장
	successCount := 0
	for _, bus := range busLocations {
		if bus.PlateNo == "" || bus.RouteId == 0 {
			continue
		}

		_, err := udm.redisBusManager.UpdateBusStatusOnly(bus, []string{"api1_initial"})
		if err != nil {
			udm.logger.Debugf("API1 Redis 저장 실패 - 차량: %s", bus.PlateNo)
			continue
		}
		successCount++
	}

	udm.logger.Infof("✅ API1 Redis 저장 완료: %d/%d건", successCount, len(busLocations))
	return busLocations, nil
}

// 🔧 loadAPI2DataToRedis API2 데이터를 Redis에 로딩
func (udm *UnifiedDataManager) loadAPI2DataToRedis(api2Client interface{}) ([]models.BusLocation, error) {
	udm.logger.Info("📥 Step 2: API2 데이터 로딩 중...")

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

	// Redis에 저장 (API1과 병합)
	successCount := 0
	for _, bus := range busLocations {
		if bus.PlateNo == "" || bus.RouteId == 0 {
			continue
		}

		// API1 데이터와 병합
		mergedBus := udm.mergeWithRedisAPI1Data(bus)

		_, err := udm.redisBusManager.UpdateBusLocation(mergedBus, []string{"api2_initial"})
		if err != nil {
			udm.logger.Debugf("API2 Redis 저장 실패 - 차량: %s", bus.PlateNo)
			continue
		}
		successCount++
	}

	udm.logger.Infof("✅ API2 Redis 저장 완료: %d/%d건", successCount, len(busLocations))
	return busLocations, nil
}

// 🔧 loadESLatestData ES에서 최신 데이터 조회
func (udm *UnifiedDataManager) loadESLatestData() (map[string]*storage.BusLastData, error) {
	udm.logger.Info("📥 Step 3: ES 최신 데이터 조회 중...")

	if udm.duplicateChecker == nil {
		udm.logger.Warn("⚠️ ES 중복 체크 서비스 없음")
		return make(map[string]*storage.BusLastData), nil
	}

	// ES에서 최근 30분 데이터 조회
	esData, err := udm.duplicateChecker.GetRecentBusData(30)
	if err != nil {
		return nil, fmt.Errorf("ES 데이터 조회 실패: %v", err)
	}

	udm.logger.Infof("📥 ES 최신 데이터: %d건", len(esData))
	return esData, nil
}

// 🔧 updateTripNumbersFromES ES의 tripNumber를 Redis에 업데이트 (로깅 강화)
func (udm *UnifiedDataManager) updateTripNumbersFromES(esData map[string]*storage.BusLastData) {
	udm.logger.Info("🔢 Step 4: ES tripNumber를 Redis에 업데이트...")

	updateCount := 0
	skipCount := 0

	for plateNo, esItem := range esData {
		if esItem.TripNumber <= 0 {
			udm.logger.Debugf("⚠️ ES tripNumber 없음 - 차량: %s, TripNumber: %d", plateNo, esItem.TripNumber)
			continue
		}

		// Redis에서 현재 데이터 조회
		redisData, err := udm.redisBusManager.GetBusLocationData(plateNo)
		if err != nil || redisData == nil {
			udm.logger.Debugf("⚠️ Redis 데이터 없음 - 차량: %s", plateNo)
			continue
		}

		oldTripNumber := redisData.BusLocation.TripNumber
		esTripNumber := esItem.TripNumber

		// 🔧 Redis의 tripNumber가 0이거나 ES보다 작을 때만 업데이트
		if oldTripNumber == 0 || oldTripNumber < esTripNumber {
			// tripNumber만 ES 값으로 업데이트
			updatedBus := redisData.BusLocation
			updatedBus.TripNumber = esTripNumber

			// Redis에 다시 저장
			_, err = udm.redisBusManager.UpdateBusLocation(updatedBus, []string{"tripnumber_sync"})
			if err != nil {
				udm.logger.Errorf("❌ ES→Redis tripNumber 업데이트 실패 - 차량: %s, 오류: %v", plateNo, err)
				continue
			}

			updateCount++
			udm.logger.Infof("🔢 ES→Redis tripNumber 업데이트 - 차량: %s, T%d → T%d",
				plateNo, oldTripNumber, esTripNumber)

			// 🔍 업데이트 후 확인
			if verifyData, verifyErr := udm.redisBusManager.GetBusLocationData(plateNo); verifyErr == nil && verifyData != nil {
				udm.logger.Debugf("✅ Redis 업데이트 확인 - 차량: %s, 현재 TripNumber: T%d",
					plateNo, verifyData.BusLocation.TripNumber)
			}
		} else {
			skipCount++
			udm.logger.Debugf("🔒 ES→Redis tripNumber 스킵 - 차량: %s, Redis: T%d >= ES: T%d",
				plateNo, oldTripNumber, esTripNumber)
		}
	}

	if updateCount > 0 || skipCount > 0 {
		udm.logger.Infof("✅ ES→Redis tripNumber 업데이트 완료: %d건 업데이트, %d건 스킵", updateCount, skipCount)
	} else {
		udm.logger.Warn("⚠️ ES→Redis tripNumber 업데이트할 데이터 없음")
	}
}

// 🔧 compareAndSaveToES Redis vs ES 비교하여 선택적 저장 (tripNumber 설정 후)
func (udm *UnifiedDataManager) compareAndSaveToES(api1Data, api2Data []models.BusLocation, esData map[string]*storage.BusLastData) int {
	udm.logger.Info("🔍 Step 5: Redis vs ES 비교 및 선택적 저장...")

	// 모든 버스 번호 수집
	allPlateNos := make(map[string]bool)
	for _, bus := range api1Data {
		if bus.PlateNo != "" {
			allPlateNos[bus.PlateNo] = true
		}
	}
	for _, bus := range api2Data {
		if bus.PlateNo != "" {
			allPlateNos[bus.PlateNo] = true
		}
	}

	var busesToSave []models.BusLocation
	skipCount := 0

	for plateNo := range allPlateNos {
		// Redis에서 현재 데이터 조회
		redisData, err := udm.redisBusManager.GetBusLocationData(plateNo)
		if err != nil || redisData == nil {
			udm.logger.Debugf("Redis 데이터 없음 - 차량: %s", plateNo)
			continue
		}

		// 🔍 tripNumber 디버깅 로그 추가
		udm.logger.Debugf("🔍 Redis 데이터 확인 - 차량: %s, TripNumber: T%d", plateNo, redisData.BusLocation.TripNumber)

		// ES와 위치 비교
		if udm.isLocationSameAsES(plateNo, redisData.BusLocation, esData) {
			skipCount++
			udm.logger.Debugf("🔄 위치 동일, ES 저장 스킵 - 차량: %s", plateNo)
			continue
		}

		// 위치가 다르면 ES 저장 대상
		busesToSave = append(busesToSave, redisData.BusLocation)
		udm.logger.Debugf("🆕 위치 변경, ES 저장 대상 - 차량: %s, TripNumber: T%d", plateNo, redisData.BusLocation.TripNumber)
	}

	// ES 벌크 저장
	if len(busesToSave) > 0 {
		udm.logger.Infof("📤 ES 저장 시작: %d건 (스킵: %d건)", len(busesToSave), skipCount)

		// 🔍 저장 전 최종 tripNumber 확인
		for i, bus := range busesToSave {
			udm.logger.Debugf("📤 저장 데이터 확인 [%d] %s: TripNumber=T%d", i+1, bus.PlateNo, bus.TripNumber)
		}

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

		udm.logger.Infof("✅ ES 저장 완료: %d건", len(busesToSave))
	} else {
		udm.logger.Info("ℹ️ ES 저장할 데이터 없음 (모든 데이터 위치 동일)")
	}

	return len(busesToSave)
}

// 🔧 isLocationSameAsES Redis 데이터와 ES 데이터의 위치가 동일한지 확인
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

// mergeWithRedisAPI1Data API1과 API2 데이터 병합
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

// 🔧 일반 운영 모드 (최초 로딩 완료 후)
// UpdateAPI1Data API1 상태정보 처리 (일반 모드)
func (udm *UnifiedDataManager) UpdateAPI1Data(busLocations []models.BusLocation) {
	if len(busLocations) == 0 {
		return
	}

	// 최초 로딩이 완료된 후에만 일반 처리
	if !udm.IsInitialLoadingDone() {
		udm.logger.Debug("최초 로딩 미완료 - API1 일반 처리 건너뛰기")
		return
	}

	// 기존 일반 처리 로직
	enrichedBuses := udm.addTripNumbers(busLocations, "API1")

	processedCount := 0
	for _, bus := range enrichedBuses {
		if bus.PlateNo == "" || bus.RouteId == 0 {
			continue
		}

		_, err := udm.redisBusManager.UpdateBusStatusOnly(bus, []string{"api1"})
		if err == nil {
			processedCount++
		}
	}

	udm.logger.Infof("API1 일반처리: 수신=%d, 처리=%d", len(busLocations), processedCount)
}

// UpdateAPI2Data API2 위치정보 처리 (일반 모드)
func (udm *UnifiedDataManager) UpdateAPI2Data(busLocations []models.BusLocation) {
	if len(busLocations) == 0 {
		return
	}

	// 최초 로딩이 완료된 후에만 일반 처리
	if !udm.IsInitialLoadingDone() {
		udm.logger.Debug("최초 로딩 미완료 - API2 일반 처리 건너뛰기")
		return
	}

	// 기존 일반 처리 로직
	enrichedBuses := udm.addTripNumbers(busLocations, "API2")

	processedCount := 0
	locationChangedCount := 0
	var locationChangedPlates []string

	for _, bus := range enrichedBuses {
		if bus.PlateNo == "" || bus.RouteId == 0 {
			continue
		}

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

	// ES 전송
	if len(locationChangedPlates) > 0 {
		udm.sendBulkToElasticsearch(locationChangedPlates)
	}

	udm.logger.Infof("API2 일반처리: 수신=%d, 처리=%d, 위치변경=%d",
		len(busLocations), processedCount, locationChangedCount)
}

// addTripNumbers 트립카운트 추가 (BusTracker 활용)
func (udm *UnifiedDataManager) addTripNumbers(busLocations []models.BusLocation, source string) []models.BusLocation {
	if udm.busTracker == nil {
		return busLocations
	}

	enrichedBuses := make([]models.BusLocation, len(busLocations))

	for i, bus := range busLocations {
		currentPosition := bus.StationId
		if currentPosition <= 0 {
			currentPosition = int64(bus.GetStationOrder())
		}

		cacheKey := bus.GetCacheKey()

		// 기존 tripNumber 우선 사용
		if existingTripNumber := udm.getExistingTripNumber(bus.PlateNo); existingTripNumber > 0 {
			bus.TripNumber = existingTripNumber
		} else {
			// 새로운 버스인 경우만 트래커에서 할당
			_, tripNumber := udm.busTracker.IsStationChanged(bus.PlateNo, currentPosition, cacheKey, bus.TotalStations)
			bus.TripNumber = tripNumber
		}

		enrichedBuses[i] = bus
		udm.busTracker.UpdateLastSeenTime(bus.PlateNo)
	}

	return enrichedBuses
}

// getExistingTripNumber 기존 tripNumber 조회 헬퍼 함수
func (udm *UnifiedDataManager) getExistingTripNumber(plateNo string) int {
	if redisData, err := udm.redisBusManager.GetBusLocationData(plateNo); err == nil && redisData != nil {
		return redisData.BusLocation.TripNumber
	}
	return 0
}

// sendBulkToElasticsearch ES 벌크 전송
func (udm *UnifiedDataManager) sendBulkToElasticsearch(plateNos []string) {
	if len(plateNos) == 0 || udm.esService == nil {
		return
	}

	udm.logger.Infof("📤 ES 전송 준비: %d건", len(plateNos))

	var esReadyBuses []models.BusLocation

	for _, plateNo := range plateNos {
		if redisData, err := udm.redisBusManager.GetBusLocationData(plateNo); err == nil && redisData != nil {
			esReadyBuses = append(esReadyBuses, redisData.BusLocation)
		}
	}

	if len(esReadyBuses) > 0 {
		if err := udm.esService.BulkSendBusLocations(udm.indexName, esReadyBuses); err != nil {
			udm.logger.Errorf("ES 전송 실패: %v", err)
			return
		}

		var syncPlateNos []string
		for _, bus := range esReadyBuses {
			syncPlateNos = append(syncPlateNos, bus.PlateNo)
		}
		udm.redisBusManager.MarkAsSynced(syncPlateNos)
	}
}

// CleanupOldData 정리 작업
func (udm *UnifiedDataManager) CleanupOldData(maxAge time.Duration) int {
	cleanedCount, _ := udm.redisBusManager.CleanupInactiveBuses(maxAge)
	if udm.busTracker != nil {
		trackerCleaned := udm.busTracker.CleanupMissingBuses(udm.logger)
		cleanedCount += trackerCleaned
	}
	return cleanedCount
}

// StartPeriodicESSync ES 동기화 시작
func (udm *UnifiedDataManager) StartPeriodicESSync() {
	udm.logger.Info("ℹ️ 주기적 ES 동기화 비활성화")
}

// StopPeriodicESSync ES 동기화 중지
func (udm *UnifiedDataManager) StopPeriodicESSync() {
	udm.logger.Debug("ℹ️ ES 동기화 중지")
}

// SetTripNumber 특정 버스의 tripNumber 강제 설정 (로깅 강화)
func (udm *UnifiedDataManager) SetTripNumber(plateNo string, tripNumber int) error {
	// 1. Redis 현재 상태 조회
	redisData, err := udm.redisBusManager.GetBusLocationData(plateNo)
	if err != nil || redisData == nil {
		return fmt.Errorf("Redis에서 차량 데이터 없음: %s", plateNo)
	}

	oldTripNumber := redisData.BusLocation.TripNumber

	// 2. Redis 업데이트
	updatedBus := redisData.BusLocation
	updatedBus.TripNumber = tripNumber

	_, err = udm.redisBusManager.UpdateBusLocation(updatedBus, []string{"manual_tripnumber_update"})
	if err != nil {
		udm.logger.Errorf("❌ 수동 tripNumber Redis 업데이트 실패 - 차량: %s, 오류: %v", plateNo, err)
		return fmt.Errorf("Redis 업데이트 실패: %v", err)
	}

	// 3. BusTracker 캐시도 업데이트 (공개 메서드 사용)
	if udm.busTracker != nil {
		udm.busTracker.BusTracker.SetTripNumberDirectly(plateNo, tripNumber)
		udm.logger.Debugf("🔧 BusTracker 캐시 동기화 - 차량: %s, T%d", plateNo, tripNumber)
	}

	udm.logger.Infof("🔧 수동 tripNumber 설정 완료 - 차량: %s, T%d → T%d",
		plateNo, oldTripNumber, tripNumber)

	// 4. 업데이트 후 확인
	if verifyData, verifyErr := udm.redisBusManager.GetBusLocationData(plateNo); verifyErr == nil && verifyData != nil {
		udm.logger.Infof("✅ 수동 설정 확인 - 차량: %s, 현재 Redis TripNumber: T%d",
			plateNo, verifyData.BusLocation.TripNumber)
	}

	return nil
}

// GetTripNumber 특정 버스의 현재 tripNumber 조회
func (udm *UnifiedDataManager) GetTripNumber(plateNo string) (int, error) {
	redisData, err := udm.redisBusManager.GetBusLocationData(plateNo)
	if err != nil || redisData == nil {
		return 0, fmt.Errorf("Redis에서 차량 데이터 없음: %s", plateNo)
	}

	return redisData.BusLocation.TripNumber, nil
}

// 인터페이스 구현 확인
var _ UnifiedDataManagerInterface = (*UnifiedDataManager)(nil)
