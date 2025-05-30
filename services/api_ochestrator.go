// services/api_orchestrator.go - 통합 캐시 지원 버전
package services

import (
	"context"
	"sync"
	"time"

	"bus-tracker/config"
	"bus-tracker/utils"
)

// MultiAPIOrchestrator 통합 캐시를 지원하는 다중 API 오케스트레이터
type MultiAPIOrchestrator struct {
	config      *config.Config
	logger      *utils.Logger
	api1Client  *API1Client
	api2Client  *API2Client
	dataManager *UnifiedDataManager

	// 제어 관련
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	isRunning bool
	mutex     sync.RWMutex
}

// NewMultiAPIOrchestrator 새로운 다중 API 오케스트레이터 생성 (통합 캐시 지원)
func NewMultiAPIOrchestrator(cfg *config.Config, logger *utils.Logger,
	api1Client *API1Client, api2Client *API2Client,
	dataManager *UnifiedDataManager) *MultiAPIOrchestrator {

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

	mao.logger.Debug("=== 다중 API 오케스트레이터 시작 (즉시 처리) ===")

	// API1 워커 시작 (활성화된 경우)
	if mao.config.API1Config.Enabled && mao.api1Client != nil {
		mao.wg.Add(1)
		go mao.runAPI1Worker()
		mao.logger.Debugf("API1 워커 시작 - 주기: %v", mao.config.API1Config.Interval)
	}

	// API2 워커 시작 (활성화된 경우)
	if mao.config.API2Config.Enabled && mao.api2Client != nil {
		mao.wg.Add(1)
		go mao.runAPI2Worker()
		mao.logger.Debugf("API2 워커 시작 - 주기: %v", mao.config.API2Config.Interval)
	}

	// 정리 워커 시작
	mao.wg.Add(1)
	go mao.runCleanupWorker()
	mao.logger.Debug("정리 워커 시작")

	mao.isRunning = true
	mao.logger.Debug("=== 모든 워커 시작 완료 ===")

	return nil
}

// runAPI1Worker API1 워커 실행
func (mao *MultiAPIOrchestrator) runAPI1Worker() {
	defer mao.wg.Done()

	ticker := time.NewTicker(mao.config.API1Config.Interval)
	defer ticker.Stop()

	mao.logger.Debug("API1 워커 시작됨")

	// 첫 번째 즉시 실행
	if mao.config.IsOperatingTime(time.Now()) {
		mao.logger.Debug("API1: 첫 호출 시작")
		mao.processAPI1Call()
	}

	for {
		select {
		case <-mao.ctx.Done():
			mao.logger.Debug("API1 워커 정지됨")
			return
		case <-ticker.C:
			if mao.config.IsOperatingTime(time.Now()) {
				mao.processAPI1Call()
			} else {
				nextTime := mao.config.GetNextOperatingTime(time.Now())
				mao.logger.Debugf("API1 운행 중단 시간. 다음 운행 시작: %s",
					nextTime.Format("2006-01-02 15:04:05"))
			}
		}
	}
}

// Stop 오케스트레이터 정지
func (mao *MultiAPIOrchestrator) Stop() {
	mao.mutex.Lock()
	defer mao.mutex.Unlock()

	if !mao.isRunning {
		return
	}

	mao.logger.Info("=== 다중 API 오케스트레이터 정지 시작 ===")

	// 모든 워커에게 정지 신호 전송
	mao.cancel()

	// 모든 워커 완료 대기
	mao.wg.Wait()

	mao.isRunning = false
	mao.logger.Info("=== 다중 API 오케스트레이터 정지 완료 ===")
}

// runAPI2Worker API2 워커 실행 (통합 캐시 기반)
func (mao *MultiAPIOrchestrator) runAPI2Worker() {
	defer mao.wg.Done()

	ticker := time.NewTicker(mao.config.API2Config.Interval)
	defer ticker.Stop()

	mao.logger.Debug("API2 워커 시작됨")

	// API1과 시간차를 두어 시작 (순차 시작으로 부하 분산)
	time.Sleep(3 * time.Second)

	// 첫 번째 즉시 실행
	if mao.config.IsOperatingTime(time.Now()) {
		mao.logger.Debug("API2: 첫 호출 시작")
		mao.processAPI2Call()
	}

	for {
		select {
		case <-mao.ctx.Done():
			mao.logger.Debug("API2 워커 정지됨")
			return
		case <-ticker.C:
			if mao.config.IsOperatingTime(time.Now()) {
				mao.processAPI2Call()
			} else {
				nextTime := mao.config.GetNextOperatingTime(time.Now())
				mao.logger.Debugf("API2 운행 중단 시간. 다음 운행 시작: %s",
					nextTime.Format("2006-01-02 15:04:05"))
			}
		}
	}
}

// runCleanupWorker 정리 워커 실행
func (mao *MultiAPIOrchestrator) runCleanupWorker() {
	defer mao.wg.Done()

	ticker := time.NewTicker(mao.config.BusCleanupInterval)
	defer ticker.Stop()

	mao.logger.Info("정리 워커 시작됨")

	for {
		select {
		case <-mao.ctx.Done():
			mao.logger.Info("정리 워커 정지됨")
			return
		case <-ticker.C:
			mao.processCleanup()
		}
	}
}

// processAPI1Call API1 호출 처리 (통합 캐시 기반)
func (mao *MultiAPIOrchestrator) processAPI1Call() {
	if mao.api1Client == nil {
		return
	}

	mao.logger.Debug("=== API1 호출 시작 ===")

	busLocations, err := mao.api1Client.FetchAllBusLocations(mao.config.API1Config.RouteIDs)
	if err != nil {
		mao.logger.Errorf("API1 호출 실패: %v", err)
		return
	}

	// 통합 데이터 매니저에서 순차 검증 및 즉시 ES 전송 수행
	mao.dataManager.UpdateAPI1Data(busLocations)
	mao.logger.Debugf("API1 처리 완료 - %d대 버스", len(busLocations))
}

// processAPI2Call API2 호출 처리
func (mao *MultiAPIOrchestrator) processAPI2Call() {
	if mao.api2Client == nil {
		return
	}

	mao.logger.Debug("=== API2 호출 시작 ===")

	busLocations, err := mao.api2Client.FetchAllBusLocations(mao.config.API2Config.RouteIDs)
	if err != nil {
		mao.logger.Errorf("API2 호출 실패: %v", err)
		return
	}

	// 통합 데이터 매니저에서 순차 검증 및 즉시 ES 전송 수행
	mao.dataManager.UpdateAPI2Data(busLocations)
	mao.logger.Debugf("API2 처리 완료 - %d대 버스", len(busLocations))
}

// processCleanup 정리 작업 처리
func (mao *MultiAPIOrchestrator) processCleanup() {
	mao.logger.Debug("=== 정리 작업 시작 ===")

	// 통합 데이터 매니저에서 오래된 데이터 정리
	removedUnified := mao.dataManager.CleanupOldData(mao.config.DataRetentionPeriod)

	// BusTracker에서 미목격 버스 정리
	removedBuses := 0
	if mao.dataManager != nil && mao.dataManager.busTracker != nil {
		removedBuses = mao.dataManager.busTracker.CleanupMissingBuses(mao.config.BusTimeoutDuration, mao.logger)
	}

	if removedUnified > 0 || removedBuses > 0 {
		// 현재 상태 요약
		total, api1Only, api2Only, both := mao.dataManager.GetStatistics()
		trackedBuses := 0
		if mao.dataManager.busTracker != nil {
			trackedBuses = mao.dataManager.busTracker.GetTrackedBusCount()
		}

		mao.logger.Debugf("정리 작업 완료 - 통합데이터: %d개 제거, 추적버스: %d대 제거",
			removedUnified, removedBuses)
		mao.logger.Debugf("현재 상태 - 통합데이터: %d대 (API1만:%d, API2만:%d, 둘다:%d), 추적중:%d대",
			total, api1Only, api2Only, both, trackedBuses)
	}

	mao.logger.Debug("=== 정리 작업 완료 ===")
}

// IsRunning 실행 상태 확인
func (mao *MultiAPIOrchestrator) IsRunning() bool {
	mao.mutex.RLock()
	defer mao.mutex.RUnlock()
	return mao.isRunning
}
