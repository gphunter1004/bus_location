// services/simple_orchestrator.go
package services

import (
	"context"
	"fmt"
	"sync"
	"time"

	"bus-tracker/config"
	"bus-tracker/utils"
)

// MultiAPIOrchestrator 간단한 다중 API 오케스트레이터
type MultiAPIOrchestrator struct {
	config      *config.Config
	logger      *utils.Logger
	api1Client  *API1Client
	api2Client  *API2Client
	dataManager *UnifiedDataManager
	esService   *ElasticsearchService

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
	dataManager *UnifiedDataManager, esService *ElasticsearchService) *MultiAPIOrchestrator {

	ctx, cancel := context.WithCancel(context.Background())

	return &MultiAPIOrchestrator{
		config:      cfg,
		logger:      logger,
		api1Client:  api1Client,
		api2Client:  api2Client,
		dataManager: dataManager,
		esService:   esService,
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

	mao.logger.Info("=== 다중 API 오케스트레이터 시작 ===")

	// API1 워커 시작 (활성화된 경우)
	if mao.config.API1Config.Enabled && mao.api1Client != nil {
		mao.wg.Add(1)
		go mao.runAPI1Worker()
		mao.logger.Infof("API1 워커 시작 - 주기: %v", mao.config.API1Config.Interval)
	}

	// API2 워커 시작 (활성화된 경우)
	if mao.config.API2Config.Enabled && mao.api2Client != nil {
		mao.wg.Add(1)
		go mao.runAPI2Worker()
		mao.logger.Infof("API2 워커 시작 - 주기: %v", mao.config.API2Config.Interval)
	}

	// 데이터 통합 워커 시작
	mao.wg.Add(1)
	go mao.runDataMergeWorker()
	mao.logger.Infof("데이터 통합 워커 시작 - 주기: %v", mao.config.DataMergeInterval)

	// ES 배치 워커 시작
	mao.wg.Add(1)
	go mao.runESBatchWorker()
	mao.logger.Infof("ES 배치 워커 시작 - 주기: %v", mao.config.ESBatchInterval)

	// 정리 워커 시작
	mao.wg.Add(1)
	go mao.runCleanupWorker()
	mao.logger.Info("정리 워커 시작")

	mao.isRunning = true
	mao.logger.Info("=== 모든 워커 시작 완료 ===")

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

	mao.logger.Info("API1 워커 시작됨")

	// 첫 번째 즉시 실행
	if mao.config.IsOperatingTime(time.Now()) {
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
			}
		}
	}
}

// runAPI2Worker API2 워커 실행
func (mao *MultiAPIOrchestrator) runAPI2Worker() {
	defer mao.wg.Done()

	ticker := time.NewTicker(mao.config.API2Config.Interval)
	defer ticker.Stop()

	mao.logger.Info("API2 워커 시작됨")

	// 첫 번째 즉시 실행
	if mao.config.IsOperatingTime(time.Now()) {
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
			}
		}
	}
}

// runDataMergeWorker 데이터 통합 워커 실행
func (mao *MultiAPIOrchestrator) runDataMergeWorker() {
	defer mao.wg.Done()

	ticker := time.NewTicker(mao.config.DataMergeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-mao.ctx.Done():
			mao.logger.Info("데이터 통합 워커 정지됨")
			return
		case <-ticker.C:
			mao.processDataMerge()
		}
	}
}

// runESBatchWorker ES 배치 워커 실행
func (mao *MultiAPIOrchestrator) runESBatchWorker() {
	defer mao.wg.Done()

	ticker := time.NewTicker(mao.config.ESBatchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-mao.ctx.Done():
			mao.logger.Info("ES 배치 워커 정지됨")
			return
		case <-ticker.C:
			mao.processESBatch()
		}
	}
}

// runCleanupWorker 정리 워커 실행
func (mao *MultiAPIOrchestrator) runCleanupWorker() {
	defer mao.wg.Done()

	ticker := time.NewTicker(mao.config.BusCleanupInterval)
	defer ticker.Stop()

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

// processAPI1Call API1 호출 처리
func (mao *MultiAPIOrchestrator) processAPI1Call() {
	if mao.api1Client == nil {
		return
	}

	mao.logger.Info("=== API1 호출 시작 ===")

	busLocations, err := mao.api1Client.FetchAllBusLocations(mao.config.API1Config.RouteIDs)
	if err != nil {
		mao.logger.Errorf("API1 호출 실패: %v", err)
		return
	}

	mao.dataManager.UpdateAPI1Data(busLocations)
	mao.logger.Infof("API1 호출 완료 - %d대 버스", len(busLocations))
}

// processAPI2Call API2 호출 처리
func (mao *MultiAPIOrchestrator) processAPI2Call() {
	if mao.api2Client == nil {
		return
	}

	mao.logger.Info("=== API2 호출 시작 ===")

	busLocations, err := mao.api2Client.FetchAllBusLocations(mao.config.API2Config.RouteIDs)
	if err != nil {
		mao.logger.Errorf("API2 호출 실패: %v", err)
		return
	}

	mao.dataManager.UpdateAPI2Data(busLocations)
	mao.logger.Infof("API2 호출 완료 - %d대 버스", len(busLocations))
}

// processDataMerge 데이터 통합 처리
func (mao *MultiAPIOrchestrator) processDataMerge() {
	changedBuses := mao.dataManager.MergeAndProcessData()

	if len(changedBuses) > 0 {
		mao.logger.Infof("데이터 통합 완료 - 변경된 버스: %d대", len(changedBuses))
	}
}

// processESBatch ES 배치 처리
func (mao *MultiAPIOrchestrator) processESBatch() {
	mao.logger.Info("=== ES 배치 처리 시작 ===")

	changedBuses := mao.dataManager.GetChangedBuses()

	mao.logger.Infof("ES 배치 체크 - 변경된 버스: %d대", len(changedBuses))

	if len(changedBuses) == 0 {
		mao.logger.Info("변경된 버스가 없어 ES 전송을 생략합니다")
		return
	}

	mao.logger.Infof("=== Elasticsearch 전송 시작 (%d대 버스) ===", len(changedBuses))

	// 전송할 버스 정보를 상세히 로깅
	for i, bus := range changedBuses {
		var locationInfo string
		if bus.NodeNm != "" && bus.NodeId != "" {
			// 정류장 정보가 있는 경우
			locationInfo = fmt.Sprintf("정류장: %s (%s), 순서: %d/%d",
				bus.NodeNm, bus.NodeId, bus.NodeOrd, bus.TotalStations)
		} else {
			// 정류장 정보가 없는 경우
			locationInfo = fmt.Sprintf("정류장ID: %d, 순서: %d/%d",
				bus.StationId, bus.StationSeq, bus.TotalStations)
		}

		var gpsInfo string
		if bus.GpsLati != 0 && bus.GpsLong != 0 {
			gpsInfo = fmt.Sprintf(", GPS: (%.6f, %.6f)", bus.GpsLati, bus.GpsLong)
		}

		mao.logger.Infof("ES 전송 데이터 [%d/%d] - 차량번호: %s, 노선: %d, %s, 혼잡도: %d%s",
			i+1, len(changedBuses), bus.PlateNo, bus.RouteId, locationInfo, bus.Crowded, gpsInfo)
	}

	startTime := time.Now()

	mao.logger.Infof("Elasticsearch 벌크 전송 시작 - 인덱스: %s", mao.config.IndexName)

	if err := mao.esService.BulkSendBusLocations(mao.config.IndexName, changedBuses); err != nil {
		mao.logger.Errorf("ES 배치 전송 실패: %v", err)
		return
	}

	duration := time.Since(startTime)
	mao.logger.Infof("ES 배치 전송 완료 - %d대 버스, 소요시간: %v", len(changedBuses), duration)
	mao.logger.Info("=== Elasticsearch 전송 완료 ===")
}

// processCleanup 정리 작업 처리
func (mao *MultiAPIOrchestrator) processCleanup() {
	removedCount := mao.dataManager.CleanupOldData(mao.config.DataRetentionPeriod)

	if removedCount > 0 {
		mao.logger.Infof("정리 작업 완료 - %d개 데이터 제거", removedCount)
	}
}

// IsRunning 실행 상태 확인
func (mao *MultiAPIOrchestrator) IsRunning() bool {
	mao.mutex.RLock()
	defer mao.mutex.RUnlock()
	return mao.isRunning
}
