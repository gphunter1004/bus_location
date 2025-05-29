// services/simple_orchestrator.go
package services

import (
	"context"
	"sync"
	"time"

	"bus-tracker/config"
	"bus-tracker/utils"
)

// MultiAPIOrchestrator 완전 간소화된 다중 API 오케스트레이터
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

// NewMultiAPIOrchestrator 새로운 다중 API 오케스트레이터 생성
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

	mao.logger.Info("=== 다중 API 오케스트레이터 시작 (즉시 처리 모드) ===")

	// API1 워커 시작 (활성화된 경우)
	if mao.config.API1Config.Enabled && mao.api1Client != nil {
		mao.wg.Add(1)
		go mao.runAPI1Worker()
		mao.logger.Infof("API1 워커 시작 - 주기: %v (데이터 수신시 즉시 통합+ES 전송)", mao.config.API1Config.Interval)
	}

	// API2 워커 시작 (활성화된 경우)
	if mao.config.API2Config.Enabled && mao.api2Client != nil {
		mao.wg.Add(1)
		go mao.runAPI2Worker()
		mao.logger.Infof("API2 워커 시작 - 주기: %v (데이터 수신시 즉시 통합+ES 전송)", mao.config.API2Config.Interval)
	}

	// 정리 워커 시작
	mao.wg.Add(1)
	go mao.runCleanupWorker()
	mao.logger.Info("정리 워커 시작")

	mao.isRunning = true
	mao.logger.Info("=== 모든 워커 시작 완료 (데이터 통합 워커 제거됨) ===")

	return nil
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

// runAPI1Worker API1 워커 실행
func (mao *MultiAPIOrchestrator) runAPI1Worker() {
	defer mao.wg.Done()

	ticker := time.NewTicker(mao.config.API1Config.Interval)
	defer ticker.Stop()

	mao.logger.Info("API1 워커 시작됨 (즉시 통합+ES 전송)")

	// 정류소 캐시 로드 완료 대기 (중복 데이터 방지)
	mao.logger.Info("API1: 정류소 캐시 로드 완료 대기 중...")
	time.Sleep(2 * time.Second) // 캐시 로드 완료를 위한 짧은 지연

	// 첫 번째 즉시 실행
	if mao.config.IsOperatingTime(time.Now()) {
		mao.logger.Info("API1: 정류소 캐시 로드 완료 후 첫 호출 시작")
		mao.processAPI1Call()
	}

	for {
		select {
		case <-mao.ctx.Done():
			mao.logger.Info("API1 워커 정지됨")
			return
		case <-ticker.C:
			if mao.config.IsOperatingTime(time.Now()) {
				mao.processAPI1Call()
			} else {
				nextTime := mao.config.GetNextOperatingTime(time.Now())
				mao.logger.Infof("API1 운행 중단 시간. 다음 운행 시작: %s",
					nextTime.Format("2006-01-02 15:04:05"))
			}
		}
	}
}

// runAPI2Worker API2 워커 실행
func (mao *MultiAPIOrchestrator) runAPI2Worker() {
	defer mao.wg.Done()

	ticker := time.NewTicker(mao.config.API2Config.Interval)
	defer ticker.Stop()

	mao.logger.Info("API2 워커 시작됨 (즉시 통합+ES 전송)")

	// 정류소 캐시 로드 완료 대기 (중복 데이터 방지)
	mao.logger.Info("API2: 정류소 캐시 로드 완료 대기 중...")
	time.Sleep(3 * time.Second) // API1보다 1초 더 지연 (순차 시작)

	// 첫 번째 즉시 실행
	if mao.config.IsOperatingTime(time.Now()) {
		mao.logger.Info("API2: 정류소 캐시 로드 완료 후 첫 호출 시작")
		mao.processAPI2Call()
	}

	for {
		select {
		case <-mao.ctx.Done():
			mao.logger.Info("API2 워커 정지됨")
			return
		case <-ticker.C:
			if mao.config.IsOperatingTime(time.Now()) {
				mao.processAPI2Call()
			} else {
				nextTime := mao.config.GetNextOperatingTime(time.Now())
				mao.logger.Infof("API2 운행 중단 시간. 다음 운행 시작: %s",
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

// processAPI1Call API1 호출 처리 (즉시 통합+ES 전송)
func (mao *MultiAPIOrchestrator) processAPI1Call() {
	if mao.api1Client == nil {
		return
	}

	mao.logger.Info("=== API1 호출 시작 (즉시 통합+ES 전송) ===")

	busLocations, err := mao.api1Client.FetchAllBusLocations(mao.config.API1Config.RouteIDs)
	if err != nil {
		mao.logger.Errorf("API1 호출 실패: %v", err)
		return
	}

	// UpdateAPI1Data에서 즉시 통합 및 변경된 것은 ES 전송까지 수행
	mao.dataManager.UpdateAPI1Data(busLocations)
	mao.logger.Infof("API1 호출 완료 - %d대 버스 (즉시 처리됨)", len(busLocations))
}

// processAPI2Call API2 호출 처리 (즉시 통합+ES 전송)
func (mao *MultiAPIOrchestrator) processAPI2Call() {
	if mao.api2Client == nil {
		return
	}

	mao.logger.Info("=== API2 호출 시작 (즉시 통합+ES 전송) ===")

	busLocations, err := mao.api2Client.FetchAllBusLocations(mao.config.API2Config.RouteIDs)
	if err != nil {
		mao.logger.Errorf("API2 호출 실패: %v", err)
		return
	}

	// UpdateAPI2Data에서 즉시 통합 및 변경된 것은 ES 전송까지 수행
	mao.dataManager.UpdateAPI2Data(busLocations)
	mao.logger.Infof("API2 호출 완료 - %d대 버스 (즉시 처리됨)", len(busLocations))
}

// processCleanup 정리 작업 처리
func (mao *MultiAPIOrchestrator) processCleanup() {
	mao.logger.Info("=== 정리 작업 시작 ===")

	// 통합 데이터 매니저에서 오래된 데이터 정리
	removedUnified := mao.dataManager.CleanupOldData(mao.config.DataRetentionPeriod)

	// BusTracker에서 미목격 버스 정리 (별도로 실행)
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

		mao.logger.Infof("정리 작업 완료 - 통합데이터: %d개 제거, 추적버스: %d대 제거",
			removedUnified, removedBuses)
		mao.logger.Infof("현재 상태 - 통합데이터: %d대 (API1만:%d, API2만:%d, 둘다:%d), 추적중:%d대",
			total, api1Only, api2Only, both, trackedBuses)
	}

	mao.logger.Info("=== 정리 작업 완료 ===")
}

// IsRunning 실행 상태 확인
func (mao *MultiAPIOrchestrator) IsRunning() bool {
	mao.mutex.RLock()
	defer mao.mutex.RUnlock()
	return mao.isRunning
}
