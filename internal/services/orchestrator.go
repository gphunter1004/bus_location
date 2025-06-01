package services

import (
	"context"
	"sync"
	"time"

	"bus-tracker/config"
	"bus-tracker/internal/services/api"
	"bus-tracker/internal/utils"
)

// MultiAPIOrchestrator 통합 캐시를 지원하는 다중 API 오케스트레이터 (Redis 기반)
type MultiAPIOrchestrator struct {
	config      *config.Config
	logger      *utils.Logger
	api1Client  *api.API1Client
	api2Client  *api.API2Client
	dataManager UnifiedDataManagerInterface

	// 제어 관련
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	isRunning bool
	mutex     sync.RWMutex
}

// NewMultiAPIOrchestrator 새로운 다중 API 오케스트레이터 생성
func NewMultiAPIOrchestrator(cfg *config.Config, logger *utils.Logger,
	api1Client *api.API1Client, api2Client *api.API2Client,
	dataManager UnifiedDataManagerInterface) *MultiAPIOrchestrator {

	ctx, cancel := context.WithCancel(context.Background())

	return &MultiAPIOrchestrator{
		config:      cfg,
		logger:      logger,
		api1Client:  api1Client,
		api2Client:  api2Client,
		dataManager: dataManager,
		ctx:         ctx,
		cancel:      cancel,
		isRunning:   false,
	}
}

// Start 오케스트레이터 시작
func (mao *MultiAPIOrchestrator) Start() error {
	mao.mutex.Lock()
	defer mao.mutex.Unlock()

	if mao.isRunning {
		return nil
	}

	// Redis 기반 주기적 ES 동기화 시작
	mao.dataManager.StartPeriodicESSync()

	// API1 워커 시작 (노선이 설정된 경우)
	if len(mao.config.API1Config.RouteIDs) > 0 && mao.api1Client != nil {
		mao.wg.Add(1)
		go mao.runAPI1Worker()
	}

	// API2 워커 시작 (노선이 설정된 경우)
	if len(mao.config.API2Config.RouteIDs) > 0 && mao.api2Client != nil {
		mao.wg.Add(1)
		go mao.runAPI2Worker()
	}

	// 정리 워커 시작
	mao.wg.Add(1)
	go mao.runCleanupWorker()

	mao.isRunning = true
	mao.logger.Info("✅ 멀티 API 오케스트레이터 시작 완료 (Redis 기반)")

	return nil
}

// runAPI1Worker API1 워커 실행
func (mao *MultiAPIOrchestrator) runAPI1Worker() {
	defer mao.wg.Done()

	ticker := time.NewTicker(mao.config.API1Config.Interval)
	defer ticker.Stop()

	mao.logger.Infof("🔄 API1 워커 시작 - 주기: %v, 노선: %d개",
		mao.config.API1Config.Interval, len(mao.config.API1Config.RouteIDs))

	// 첫 번째 즉시 실행
	if mao.config.IsOperatingTime(time.Now()) {
		mao.processAPI1Call()
	}

	for {
		select {
		case <-mao.ctx.Done():
			mao.logger.Info("🔄 API1 워커 종료")
			return
		case <-ticker.C:
			if mao.config.IsOperatingTime(time.Now()) {
				mao.processAPI1Call()
			} else {
				mao.logger.Debugf("API1 호출 건너뛰기 - 운영시간 외")
			}
		}
	}
}

// runAPI2Worker API2 워커 실행
func (mao *MultiAPIOrchestrator) runAPI2Worker() {
	defer mao.wg.Done()

	ticker := time.NewTicker(mao.config.API2Config.Interval)
	defer ticker.Stop()

	mao.logger.Infof("🔄 API2 워커 시작 - 주기: %v, 노선: %d개",
		mao.config.API2Config.Interval, len(mao.config.API2Config.RouteIDs))

	// API1과 시간차를 두어 시작 (순차 시작으로 부하 분산)
	time.Sleep(3 * time.Second)

	// 첫 번째 즉시 실행
	if mao.config.IsOperatingTime(time.Now()) {
		mao.processAPI2Call()
	}

	for {
		select {
		case <-mao.ctx.Done():
			mao.logger.Info("🔄 API2 워커 종료")
			return
		case <-ticker.C:
			if mao.config.IsOperatingTime(time.Now()) {
				mao.processAPI2Call()
			} else {
				mao.logger.Debugf("API2 호출 건너뛰기 - 운영시간 외")
			}
		}
	}
}

// runCleanupWorker 정리 워커 (Redis 기반)
func (mao *MultiAPIOrchestrator) runCleanupWorker() {
	defer mao.wg.Done()

	ticker := time.NewTicker(mao.config.BusCleanupInterval)
	defer ticker.Stop()

	mao.logger.Infof("🧹 정리 워커 시작 - 주기: %v", mao.config.BusCleanupInterval)

	for {
		select {
		case <-mao.ctx.Done():
			mao.logger.Info("🧹 정리 워커 종료")
			return
		case <-ticker.C:
			mao.processCleanup()
		}
	}
}

// processAPI1Call API1 호출 처리
func (mao *MultiAPIOrchestrator) processAPI1Call() {
	if mao.api1Client == nil {
		return
	}

	start := time.Now()
	busLocations, err := mao.api1Client.FetchAllBusLocations(mao.config.API1Config.RouteIDs)
	duration := time.Since(start)

	if err != nil {
		mao.logger.Errorf("API1 호출 실패 (소요시간: %v): %v", duration, err)
		return
	}

	mao.logger.Infof("API1 호출 완료 - %d건 수신 (소요시간: %v)", len(busLocations), duration)
	mao.dataManager.UpdateAPI1Data(busLocations)
}

// processAPI2Call API2 호출 처리
func (mao *MultiAPIOrchestrator) processAPI2Call() {
	if mao.api2Client == nil {
		return
	}

	start := time.Now()
	busLocations, err := mao.api2Client.FetchAllBusLocations(mao.config.API2Config.RouteIDs)
	duration := time.Since(start)

	if err != nil {
		mao.logger.Errorf("API2 호출 실패 (소요시간: %v): %v", duration, err)
		return
	}

	mao.logger.Infof("API2 호출 완료 - %d건 수신 (소요시간: %v)", len(busLocations), duration)
	mao.dataManager.UpdateAPI2Data(busLocations)
}

// processCleanup 정리 작업 (Redis 기반)
func (mao *MultiAPIOrchestrator) processCleanup() {
	// Redis 기반 통합 데이터 매니저에서 정리 작업 수행
	cleanedCount := mao.dataManager.CleanupOldData(mao.config.DataRetentionPeriod)

	if cleanedCount > 0 {
		mao.logger.Infof("정리 작업 완료 - 제거된 버스: %d대", cleanedCount)
	} else {
		mao.logger.Debugf("정리 작업 완료 - 제거할 데이터 없음")
	}
}

// Stop 오케스트레이터 정지
func (mao *MultiAPIOrchestrator) Stop() {
	mao.mutex.Lock()
	defer mao.mutex.Unlock()

	if !mao.isRunning {
		return
	}

	mao.logger.Info("🔄 멀티 API 오케스트레이터 정지 중...")

	// Redis 기반 주기적 ES 동기화 중지
	mao.dataManager.StopPeriodicESSync()

	// 모든 워커에게 정지 신호 전송
	mao.cancel()

	// 모든 워커 완료 대기
	mao.wg.Wait()

	mao.isRunning = false
	mao.logger.Info("✅ 멀티 API 오케스트레이터 정지 완료")
}

// IsRunning 실행 상태 확인
func (mao *MultiAPIOrchestrator) IsRunning() bool {
	mao.mutex.RLock()
	defer mao.mutex.RUnlock()
	return mao.isRunning
}

// GetStatistics 통계 정보 반환
func (mao *MultiAPIOrchestrator) GetStatistics() (totalBuses, api1Only, api2Only, both int) {
	if mao.dataManager != nil {
		return mao.dataManager.GetStatistics()
	}
	return 0, 0, 0, 0
}

// GetDetailedStatistics 상세 통계 정보 반환 (Redis 기반)
func (mao *MultiAPIOrchestrator) GetDetailedStatistics() map[string]interface{} {
	stats := make(map[string]interface{})

	// 기본 통계
	totalBuses, api1Only, api2Only, both := mao.GetStatistics()
	stats["total_buses"] = totalBuses
	stats["api1_only"] = api1Only
	stats["api2_only"] = api2Only
	stats["both_apis"] = both

	// 오케스트레이터 상태
	stats["is_running"] = mao.IsRunning()
	stats["api1_enabled"] = mao.api1Client != nil && len(mao.config.API1Config.RouteIDs) > 0
	stats["api2_enabled"] = mao.api2Client != nil && len(mao.config.API2Config.RouteIDs) > 0

	// 설정 정보
	stats["api1_interval"] = mao.config.API1Config.Interval.String()
	stats["api2_interval"] = mao.config.API2Config.Interval.String()
	stats["cleanup_interval"] = mao.config.BusCleanupInterval.String()
	stats["data_retention"] = mao.config.DataRetentionPeriod.String()

	// 운영 시간 정보
	stats["operating_schedule"] = mao.config.GetOperatingScheduleString()
	stats["is_operating_time"] = mao.config.IsOperatingTime(time.Now())

	return stats
}
