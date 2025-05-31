package services

import (
	"context"
	"sync"
	"time"

	"bus-tracker/config"
	"bus-tracker/internal/services/api"
	"bus-tracker/internal/utils"
)

// MultiAPIOrchestrator 통합 캐시를 지원하는 다중 API 오케스트레이터
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

	// 🔧 새로운 정리 워커 시작 (간소화된 조건)
	mao.wg.Add(1)
	go mao.runCleanupWorker()

	mao.isRunning = true

	return nil
}

// runAPI1Worker API1 워커 실행
func (mao *MultiAPIOrchestrator) runAPI1Worker() {
	defer mao.wg.Done()

	ticker := time.NewTicker(mao.config.API1Config.Interval)
	defer ticker.Stop()

	// 첫 번째 즉시 실행
	if mao.config.IsOperatingTime(time.Now()) {
		mao.processAPI1Call()
	}

	for {
		select {
		case <-mao.ctx.Done():
			return
		case <-ticker.C:
			if mao.config.IsOperatingTime(time.Now()) {
				mao.processAPI1Call()
			}
		}
	}
}

// runAPI2Worker API2 워커 실행
func (mao *MultiAPIOrchestrator) runAPI2Worker() {
	defer mao.wg.Done()

	ticker := time.NewTicker(mao.config.API2Config.Interval)
	defer ticker.Stop()

	// API1과 시간차를 두어 시작 (순차 시작으로 부하 분산)
	time.Sleep(3 * time.Second)

	// 첫 번째 즉시 실행
	if mao.config.IsOperatingTime(time.Now()) {
		mao.processAPI2Call()
	}

	for {
		select {
		case <-mao.ctx.Done():
			return
		case <-ticker.C:
			if mao.config.IsOperatingTime(time.Now()) {
				mao.processAPI2Call()
			}
		}
	}
}

// 🔧 간소화된 정리 워커
func (mao *MultiAPIOrchestrator) runCleanupWorker() {
	defer mao.wg.Done()

	ticker := time.NewTicker(mao.config.BusCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-mao.ctx.Done():
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

	busLocations, err := mao.api1Client.FetchAllBusLocations(mao.config.API1Config.RouteIDs)
	if err != nil {
		mao.logger.Errorf("API1 호출 실패: %v", err)
		return
	}

	mao.dataManager.UpdateAPI1Data(busLocations)
}

// processAPI2Call API2 호출 처리
func (mao *MultiAPIOrchestrator) processAPI2Call() {
	if mao.api2Client == nil {
		return
	}

	busLocations, err := mao.api2Client.FetchAllBusLocations(mao.config.API2Config.RouteIDs)
	if err != nil {
		mao.logger.Errorf("API2 호출 실패: %v", err)
		return
	}

	mao.dataManager.UpdateAPI2Data(busLocations)
}

// 🔧 간소화된 정리 작업
func (mao *MultiAPIOrchestrator) processCleanup() {
	// 1. 통합 데이터 매니저에서 오래된 데이터 정리
	cleanedCount := mao.dataManager.CleanupOldData(mao.config.DataRetentionPeriod)

	if cleanedCount > 0 {
		mao.logger.Infof("메모리 데이터 정리 완료 - 제거된 버스: %d대", cleanedCount)
	}

	// 2. 🔧 새로운 종료 조건에 따른 버스 정리는 dataManager 내부에서 처리됨
	// (BusTracker의 CleanupMissingBuses 메서드가 자동으로 호출됨)
}

// Stop 오케스트레이터 정지
func (mao *MultiAPIOrchestrator) Stop() {
	mao.mutex.Lock()
	defer mao.mutex.Unlock()

	if !mao.isRunning {
		return
	}

	// 모든 워커에게 정지 신호 전송
	mao.cancel()

	// 모든 워커 완료 대기
	mao.wg.Wait()

	mao.isRunning = false
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
