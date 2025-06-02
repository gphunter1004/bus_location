// internal/services/orchestrator.go - 최초 로딩 완료 후 시작 버전
package services

import (
	"context"
	"sync"
	"time"

	"bus-tracker/config"
	"bus-tracker/internal/models"
	"bus-tracker/internal/services/api"
	"bus-tracker/internal/utils"
)

// UnifiedDataManagerInterface 정의
type UnifiedDataManagerInterface interface {
	UpdateAPI1Data(busLocations []models.BusLocation)
	UpdateAPI2Data(busLocations []models.BusLocation)
	CleanupOldData(maxAge time.Duration) int
	IsInitialLoadingDone() bool // 🔧 최초 로딩 완료 여부 확인 메서드 추가
}

// SimplifiedMultiAPIOrchestrator Redis 중심 단순화된 다중 API 오케스트레이터
type SimplifiedMultiAPIOrchestrator struct {
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

// NewSimplifiedMultiAPIOrchestrator 단순화된 다중 API 오케스트레이터 생성
func NewSimplifiedMultiAPIOrchestrator(cfg *config.Config, logger *utils.Logger,
	api1Client *api.API1Client, api2Client *api.API2Client,
	dataManager UnifiedDataManagerInterface) *SimplifiedMultiAPIOrchestrator {

	ctx, cancel := context.WithCancel(context.Background())

	return &SimplifiedMultiAPIOrchestrator{
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

// Start 오케스트레이터 시작 (최초 로딩 완료 확인 후)
func (sao *SimplifiedMultiAPIOrchestrator) Start() error {
	sao.mutex.Lock()
	defer sao.mutex.Unlock()

	if sao.isRunning {
		return nil
	}

	// 🔧 최초 로딩 완료 대기
	sao.logger.Info("⏳ 최초 데이터 로딩 완료 대기 중...")
	for !sao.dataManager.IsInitialLoadingDone() {
		time.Sleep(500 * time.Millisecond)
	}
	sao.logger.Info("✅ 최초 데이터 로딩 완료 확인 - 정상 운영 시작")

	// API1 워커 시작
	if len(sao.config.API1Config.RouteIDs) > 0 && sao.api1Client != nil {
		sao.wg.Add(1)
		go sao.runAPI1Worker()
	}

	// API2 워커 시작
	if len(sao.config.API2Config.RouteIDs) > 0 && sao.api2Client != nil {
		sao.wg.Add(1)
		go sao.runAPI2Worker()
	}

	// 정리 워커 시작
	sao.wg.Add(1)
	go sao.runCleanupWorker()

	sao.isRunning = true
	sao.logger.Info("✅ 오케스트레이터 정상 운영 시작 완료")

	return nil
}

// runAPI1Worker API1 워커 실행 (정상 운영)
func (sao *SimplifiedMultiAPIOrchestrator) runAPI1Worker() {
	defer sao.wg.Done()

	ticker := time.NewTicker(sao.config.API1Config.Interval)
	defer ticker.Stop()

	sao.logger.Infof("🔄 API1 정상 운영 워커 시작 - 주기: %v, 노선: %d개",
		sao.config.API1Config.Interval, len(sao.config.API1Config.RouteIDs))

	// 🔧 첫 번째 호출 전 약간의 지연 (최초 로딩과 겹치지 않도록)
	time.Sleep(2 * time.Second)

	// 첫 번째 즉시 실행
	if sao.config.IsOperatingTime(time.Now()) {
		sao.processAPI1Call()
	}

	for {
		select {
		case <-sao.ctx.Done():
			sao.logger.Info("🔄 API1 워커 종료")
			return
		case <-ticker.C:
			if sao.config.IsOperatingTime(time.Now()) {
				sao.processAPI1Call()
			} else {
				sao.logger.Debugf("API1 호출 건너뛰기 - 운영시간 외")
			}
		}
	}
}

// runAPI2Worker API2 워커 실행 (정상 운영)
func (sao *SimplifiedMultiAPIOrchestrator) runAPI2Worker() {
	defer sao.wg.Done()

	ticker := time.NewTicker(sao.config.API2Config.Interval)
	defer ticker.Stop()

	sao.logger.Infof("🔄 API2 정상 운영 워커 시작 - 주기: %v, 노선: %d개",
		sao.config.API2Config.Interval, len(sao.config.API2Config.RouteIDs))

	// 🔧 API1과 시간차를 두어 시작 (최초 로딩과 겹치지 않도록)
	time.Sleep(5 * time.Second)

	// 첫 번째 즉시 실행
	if sao.config.IsOperatingTime(time.Now()) {
		sao.processAPI2Call()
	}

	for {
		select {
		case <-sao.ctx.Done():
			sao.logger.Info("🔄 API2 워커 종료")
			return
		case <-ticker.C:
			if sao.config.IsOperatingTime(time.Now()) {
				sao.processAPI2Call()
			} else {
				sao.logger.Debugf("API2 호출 건너뛰기 - 운영시간 외")
			}
		}
	}
}

// runCleanupWorker 정리 워커 (정상 운영)
func (sao *SimplifiedMultiAPIOrchestrator) runCleanupWorker() {
	defer sao.wg.Done()

	ticker := time.NewTicker(sao.config.BusCleanupInterval)
	defer ticker.Stop()

	sao.logger.Infof("🧹 정리 워커 시작 - 주기: %v", sao.config.BusCleanupInterval)

	for {
		select {
		case <-sao.ctx.Done():
			sao.logger.Info("🧹 정리 워커 종료")
			return
		case <-ticker.C:
			sao.processCleanup()
		}
	}
}

// processAPI1Call API1 호출 처리 (정상 운영)
func (sao *SimplifiedMultiAPIOrchestrator) processAPI1Call() {
	if sao.api1Client == nil {
		return
	}

	// 🔧 최초 로딩 중에는 건너뛰기
	if !sao.dataManager.IsInitialLoadingDone() {
		sao.logger.Debug("최초 로딩 중 - API1 호출 건너뛰기")
		return
	}

	start := time.Now()
	busLocations, err := sao.api1Client.FetchAllBusLocations(sao.config.API1Config.RouteIDs)
	duration := time.Since(start)

	if err != nil {
		sao.logger.Errorf("API1 호출 실패 (소요시간: %v): %v", duration, err)
		return
	}

	sao.logger.Infof("API1 호출 완료 - %d건 수신 (소요시간: %v)", len(busLocations), duration)

	// 정상 운영 모드로 데이터 매니저에 전달
	sao.dataManager.UpdateAPI1Data(busLocations)
}

// processAPI2Call API2 호출 처리 (정상 운영)
func (sao *SimplifiedMultiAPIOrchestrator) processAPI2Call() {
	if sao.api2Client == nil {
		return
	}

	// 🔧 최초 로딩 중에는 건너뛰기
	if !sao.dataManager.IsInitialLoadingDone() {
		sao.logger.Debug("최초 로딩 중 - API2 호출 건너뛰기")
		return
	}

	start := time.Now()
	busLocations, err := sao.api2Client.FetchAllBusLocations(sao.config.API2Config.RouteIDs)
	duration := time.Since(start)

	if err != nil {
		sao.logger.Errorf("API2 호출 실패 (소요시간: %v): %v", duration, err)
		return
	}

	sao.logger.Infof("API2 호출 완료 - %d건 수신 (소요시간: %v)", len(busLocations), duration)

	// 정상 운영 모드로 데이터 매니저에 전달
	sao.dataManager.UpdateAPI2Data(busLocations)
}

// processCleanup 정리 작업 (정상 운영)
func (sao *SimplifiedMultiAPIOrchestrator) processCleanup() {
	cleanedCount := sao.dataManager.CleanupOldData(sao.config.DataRetentionPeriod)

	if cleanedCount > 0 {
		sao.logger.Infof("정리 작업 완료 - 제거된 버스: %d대", cleanedCount)
	} else {
		sao.logger.Debugf("정리 작업 완료 - 제거할 데이터 없음")
	}
}

// Stop 오케스트레이터 정지
func (sao *SimplifiedMultiAPIOrchestrator) Stop() {
	sao.mutex.Lock()
	defer sao.mutex.Unlock()

	if !sao.isRunning {
		return
	}

	sao.logger.Info("🔄 오케스트레이터 정지 중...")

	// 모든 워커에게 정지 신호 전송
	sao.cancel()

	// 모든 워커 완료 대기
	sao.wg.Wait()

	sao.isRunning = false
	sao.logger.Info("✅ 오케스트레이터 정지 완료")
}

// IsRunning 실행 상태 확인
func (sao *SimplifiedMultiAPIOrchestrator) IsRunning() bool {
	sao.mutex.RLock()
	defer sao.mutex.RUnlock()
	return sao.isRunning
}

// GetDetailedStatistics 상세 통계 정보 반환
func (sao *SimplifiedMultiAPIOrchestrator) GetDetailedStatistics() map[string]interface{} {
	stats := make(map[string]interface{})

	// 기본 정보
	stats["is_running"] = sao.IsRunning()
	stats["api1_enabled"] = sao.api1Client != nil && len(sao.config.API1Config.RouteIDs) > 0
	stats["api2_enabled"] = sao.api2Client != nil && len(sao.config.API2Config.RouteIDs) > 0
	stats["operating_schedule"] = sao.config.GetOperatingScheduleString()
	stats["is_operating_time"] = sao.config.IsOperatingTime(time.Now())
	stats["initial_loading_done"] = sao.dataManager.IsInitialLoadingDone() // 🔧 최초 로딩 상태 추가

	return stats
}

// 타입 별칭 (기존 코드와의 일관성을 위해)
type MultiAPIOrchestrator = SimplifiedMultiAPIOrchestrator

// 생성자
func NewMultiAPIOrchestrator(cfg *config.Config, logger *utils.Logger,
	api1Client *api.API1Client, api2Client *api.API2Client,
	dataManager UnifiedDataManagerInterface) *MultiAPIOrchestrator {

	return NewSimplifiedMultiAPIOrchestrator(cfg, logger, api1Client, api2Client, dataManager)
}
