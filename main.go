// main.go - 선택 가능한 버스 트래커 (API1/API2/통합 모드)
package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bus-tracker/config"
	"bus-tracker/services"
	"bus-tracker/utils"
)

func main() {
	// 설정 로드
	cfg := config.LoadConfig()

	// 로거 초기화
	logger := utils.NewLogger()

	// 설정 정보 출력
	logger.Info("=== 버스 트래커 시작 ===")
	cfg.PrintConfig()

	// 모드별 실행
	switch cfg.Mode {
	case "api1":
		runAPI1Mode(cfg, logger)
	case "api2":
		runAPI2Mode(cfg, logger)
	case "unified":
		runUnifiedMode(cfg, logger)
	default:
		log.Fatalf("지원하지 않는 모드입니다: %s (api1, api2, unified 중 선택)", cfg.Mode)
	}
}

// runAPI1Mode API1 전용 모드 실행
func runAPI1Mode(cfg *config.Config, logger *utils.Logger) {
	logger.Info("=== API1 모드로 실행 ===")

	// API1용 설정 생성
	api1Config := &config.Config{
		ServiceKey:            cfg.ServiceKey,
		CityCode:              cfg.CityCode,
		APIBaseURL:            cfg.API1Config.BaseURL,
		RouteIDs:              cfg.API1Config.RouteIDs,
		APIType:               "api1",
		IndexName:             cfg.IndexName,
		Interval:              cfg.API1Config.Interval,
		ElasticsearchURL:      cfg.ElasticsearchURL,
		ElasticsearchUsername: cfg.ElasticsearchUsername,
		ElasticsearchPassword: cfg.ElasticsearchPassword,
		OperatingStartHour:    cfg.OperatingStartHour,
		OperatingStartMinute:  cfg.OperatingStartMinute,
		OperatingEndHour:      cfg.OperatingEndHour,
		OperatingEndMinute:    cfg.OperatingEndMinute,
		BusCleanupInterval:    cfg.BusCleanupInterval,
		BusTimeoutDuration:    cfg.BusTimeoutDuration,
	}

	// 서비스 초기화
	busTracker := services.NewBusTracker()
	apiClient := services.NewAPI1Client(api1Config, logger)
	esService := services.NewElasticsearchService(api1Config, logger)

	// 정류소 캐시 로드
	logger.Info("API1 정류소 정보 캐시 로딩 시작")
	if err := apiClient.LoadStationCache(cfg.API1Config.RouteIDs); err != nil {
		logger.Warnf("정류소 캐시 로드 실패: %v", err)
	} else {
		routeCount, stationCount := apiClient.GetCacheStatistics()
		logger.Infof("정류소 캐시 로드 완료 - 노선: %d개, 정류소: %d개", routeCount, stationCount)
	}

	// Elasticsearch 연결 테스트
	if err := esService.TestConnection(); err != nil {
		log.Fatalf("Elasticsearch 연결 실패: %v", err)
	}
	logger.Info("Elasticsearch 연결 성공")

	// API1 전용 실행 루프
	runSingleAPILoop(apiClient, esService, busTracker, api1Config, logger, cfg.API1Config.RouteIDs, cfg.API1Config.Interval)
}

// runAPI2Mode API2 전용 모드 실행
func runAPI2Mode(cfg *config.Config, logger *utils.Logger) {
	logger.Info("=== API2 모드로 실행 ===")

	// API2용 설정 생성
	api2Config := &config.Config{
		ServiceKey:            cfg.ServiceKey,
		CityCode:              cfg.CityCode,
		APIBaseURL:            cfg.API2Config.BaseURL,
		RouteIDs:              cfg.API2Config.RouteIDs,
		APIType:               "api2",
		IndexName:             cfg.IndexName,
		Interval:              cfg.API2Config.Interval,
		ElasticsearchURL:      cfg.ElasticsearchURL,
		ElasticsearchUsername: cfg.ElasticsearchUsername,
		ElasticsearchPassword: cfg.ElasticsearchPassword,
		OperatingStartHour:    cfg.OperatingStartHour,
		OperatingStartMinute:  cfg.OperatingStartMinute,
		OperatingEndHour:      cfg.OperatingEndHour,
		OperatingEndMinute:    cfg.OperatingEndMinute,
		BusCleanupInterval:    cfg.BusCleanupInterval,
		BusTimeoutDuration:    cfg.BusTimeoutDuration,
	}

	// 서비스 초기화
	busTracker := services.NewBusTracker()
	apiClient := services.NewAPI2Client(api2Config, logger)
	esService := services.NewElasticsearchService(api2Config, logger)

	// 정류소 캐시 로드
	logger.Info("API2 정류소 정보 캐시 로딩 시작")
	if err := apiClient.LoadStationCache(cfg.API2Config.RouteIDs); err != nil {
		logger.Warnf("정류소 캐시 로드 실패: %v", err)
	} else {
		routeCount, stationCount := apiClient.GetCacheStatistics()
		logger.Infof("정류소 캐시 로드 완료 - 노선: %d개, 정류소: %d개", routeCount, stationCount)
	}

	// Elasticsearch 연결 테스트
	if err := esService.TestConnection(); err != nil {
		log.Fatalf("Elasticsearch 연결 실패: %v", err)
	}
	logger.Info("Elasticsearch 연결 성공")

	// API2 전용 실행 루프
	runSingleAPILoop(apiClient, esService, busTracker, api2Config, logger, cfg.API2Config.RouteIDs, cfg.API2Config.Interval)
}

// runUnifiedMode 통합 모드 실행 (완전 즉시 처리)
func runUnifiedMode(cfg *config.Config, logger *utils.Logger) {
	logger.Info("=== 통합 모드로 실행 (완전 즉시 처리) ===")

	// 핵심 서비스들 초기화
	logger.Info("=== 서비스 초기화 시작 ===")

	// 1. 버스 트래커 초기화
	busTracker := services.NewBusTracker()
	logger.Info("버스 트래커 초기화 완료")

	// 2. Elasticsearch 서비스 초기화 (먼저)
	esService := services.NewElasticsearchService(cfg, logger)

	// Elasticsearch 연결 테스트
	if err := esService.TestConnection(); err != nil {
		log.Fatalf("Elasticsearch 연결 실패: %v", err)
	}
	logger.Info("Elasticsearch 연결 테스트 성공")

	// 3. API 클라이언트들 초기화 및 정류소 캐시 로드 (ES 초기화 후)
	var api1Client *services.API1Client
	var api2Client *services.API2Client
	var stationCache1 *services.StationCacheService
	var stationCache2 *services.StationCacheService

	// API1 클라이언트 초기화 (활성화된 경우)
	if cfg.API1Config.Enabled {
		logger.Info("=== API1 초기화 시작 ===")

		// API1용 설정 생성
		api1Config := &config.Config{
			ServiceKey: cfg.ServiceKey,
			CityCode:   cfg.CityCode,
			APIBaseURL: cfg.API1Config.BaseURL,
			RouteIDs:   cfg.API1Config.RouteIDs,
			APIType:    "api1",
		}

		api1Client = services.NewAPI1Client(api1Config, logger)
		stationCache1 = services.NewStationCacheService(api1Config, logger, "api1")

		logger.Infof("API1 클라이언트 초기화 완료 - 노선 %d개", len(cfg.API1Config.RouteIDs))

		// API1 정류소 캐시 로드 (중요: API 시작 전에 완료)
		logger.Info("API1 정류소 캐시 로딩 중... (API 호출 전 필수)")
		if err := api1Client.LoadStationCache(cfg.API1Config.RouteIDs); err != nil {
			logger.Errorf("API1 정류소 캐시 로드 실패: %v", err)
			logger.Warn("정류소 캐시 없이 실행하면 nodeNm이 없는 중복 데이터가 발생할 수 있습니다")
		} else {
			routeCount, stationCount := api1Client.GetCacheStatistics()
			logger.Infof("✅ API1 정류소 캐시 로드 완료 - 노선: %d개, 정류소: %d개", routeCount, stationCount)
		}

		logger.Info("=== API1 초기화 완료 ===")
	}

	// API2 클라이언트 초기화 (활성화된 경우)
	if cfg.API2Config.Enabled {
		logger.Info("=== API2 초기화 시작 ===")

		// API2용 설정 생성
		api2Config := &config.Config{
			ServiceKey: cfg.ServiceKey,
			CityCode:   cfg.CityCode,
			APIBaseURL: cfg.API2Config.BaseURL,
			RouteIDs:   cfg.API2Config.RouteIDs,
			APIType:    "api2",
		}

		api2Client = services.NewAPI2Client(api2Config, logger)
		stationCache2 = services.NewStationCacheService(api2Config, logger, "api2")

		logger.Infof("API2 클라이언트 초기화 완료 - 노선 %d개", len(cfg.API2Config.RouteIDs))

		// API2 정류소 캐시 로드 (중요: API 시작 전에 완료)
		logger.Info("API2 정류소 캐시 로딩 중... (API 호출 전 필수)")
		if err := api2Client.LoadStationCache(cfg.API2Config.RouteIDs); err != nil {
			logger.Errorf("API2 정류소 캐시 로드 실패: %v", err)
			logger.Warn("정류소 캐시 없이 실행하면 nodeNm이 없는 중복 데이터가 발생할 수 있습니다")
		} else {
			routeCount, stationCount := api2Client.GetCacheStatistics()
			logger.Infof("✅ API2 정류소 캐시 로드 완료 - 노선: %d개, 정류소: %d개", routeCount, stationCount)
		}

		logger.Info("=== API2 초기화 완료 ===")
	}

	// 4. 통합 데이터 매니저 초기화 (정류소 캐시 로드 후)
	dataManager := services.NewUnifiedDataManager(logger, busTracker, stationCache1, stationCache2, esService, cfg.IndexName)
	logger.Info("통합 데이터 매니저 초기화 완료 (완전 즉시 처리)")

	// 5. 완전 간소화된 다중 API 오케스트레이터 초기화
	orchestrator := services.NewMultiAPIOrchestrator(
		cfg,
		logger,
		api1Client,
		api2Client,
		dataManager,
	)
	logger.Info("다중 API 오케스트레이터 초기화 완료 (완전 즉시 처리 모드)")

	logger.Info("=== 모든 서비스 초기화 완료 ===")
	logger.Info("⚠️  정류소 캐시가 완전히 로드된 후 API 호출이 시작됩니다")

	// 시스템 상태 출력
	printUnifiedSystemStatus(cfg, logger)

	// 오케스트레이터 시작
	if err := orchestrator.Start(); err != nil {
		log.Fatalf("오케스트레이터 시작 실패: %v", err)
	}

	// 우아한 종료를 위한 신호 처리
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	logger.Info("=== 통합 버스 트래커 실행 중 (완전 즉시 처리) ===")
	logger.Info("종료하려면 Ctrl+C를 누르세요")

	// 종료 신호 대기
	<-sigChan

	logger.Info("=== 종료 신호 수신 - 우아한 종료 시작 ===")

	// 오케스트레이터 정지
	orchestrator.Stop()

	logger.Info("=== 통합 버스 트래커 종료 완료 ===")
}

// runSingleAPILoop 단일 API 모드의 공통 실행 루프
func runSingleAPILoop(apiClient services.BusAPIClient, esService *services.ElasticsearchService,
	busTracker *services.BusTracker, cfg *config.Config, logger *utils.Logger,
	routeIDs []string, interval time.Duration) {

	// 타이머 생성
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// 버스 정리 타이머 생성
	cleanupTicker := time.NewTicker(cfg.BusCleanupInterval)
	defer cleanupTicker.Stop()

	logger.Infof("%s 버스 정보 API 모니터링 시작", apiClient.GetAPIType())
	logger.Info("정류장 변경된 버스만 Elasticsearch로 전송됩니다")

	// 첫 번째 호출 (운영 시간 체크 후)
	currentTime := time.Now()
	logger.Infof("현재 시간: %s", currentTime.Format("2006-01-02 15:04:05"))
	logger.Infof("운영 시간 체크 결과: %t", cfg.IsOperatingTime(currentTime))

	if cfg.IsOperatingTime(currentTime) {
		logger.Info("운영 시간 중 - API 호출 시작")
		processSingleAPICall(apiClient, esService, busTracker, logger, routeIDs, cfg.IndexName)
	} else {
		nextOperatingTime := cfg.GetNextOperatingTime(currentTime)
		logger.Infof("현재 운행 중단 시간입니다. 다음 운행 시작: %s",
			nextOperatingTime.Format("2006-01-02 15:04:05"))
	}

	// 우아한 종료를 위한 신호 처리
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	logger.Infof("=== %s 모드 실행 중 ===", apiClient.GetAPIType())
	logger.Info("종료하려면 Ctrl+C를 누르세요")

	// 타이머로 반복 실행
	for {
		select {
		case <-sigChan:
			logger.Infof("=== 종료 신호 수신 - %s 모드 종료 ===", apiClient.GetAPIType())
			return

		case <-ticker.C:
			currentTime := time.Now()
			logger.Infof("시간 체크: %s, 운영 중: %t",
				currentTime.Format("15:04:05"), cfg.IsOperatingTime(currentTime))

			if cfg.IsOperatingTime(currentTime) {
				processSingleAPICall(apiClient, esService, busTracker, logger, routeIDs, cfg.IndexName)
			} else {
				nextOperatingTime := cfg.GetNextOperatingTime(currentTime)
				logger.Infof("운행 중단 시간. 다음 운행 시작: %s",
					nextOperatingTime.Format("2006-01-02 15:04:05"))
			}

		case <-cleanupTicker.C:
			// 구간 운행 버스 정리
			removedCount := busTracker.CleanupMissingBuses(cfg.BusTimeoutDuration, logger)
			if removedCount > 0 {
				logger.Infof("정리 완료 - 현재 추적 중인 버스: %d대", busTracker.GetTrackedBusCount())
			}
		}
	}
}

// processSingleAPICall 단일 API 호출 처리
func processSingleAPICall(apiClient services.BusAPIClient, esService *services.ElasticsearchService,
	busTracker *services.BusTracker, logger *utils.Logger, routeIDs []string, indexName string) {

	logger.Infof("=== 버스 위치 API 호출 시작 (%s, %s) ===",
		time.Now().Format("15:04:05"), apiClient.GetAPIType())

	// 모든 노선에 대해 API 호출
	allBusLocations, err := apiClient.FetchAllBusLocations(routeIDs)
	if err != nil {
		logger.Errorf("API 호출 오류: %v", err)
		return
	}

	// 전체 버스 위치 정보가 비어있는지 확인
	if len(allBusLocations) == 0 {
		logger.Warn("모든 노선에서 버스 위치 정보가 없습니다")
		return
	}

	logger.Infof("전체 수신된 버스 위치 정보: %d대", len(allBusLocations))
	logger.Infof("현재 추적 중인 버스: %d대", busTracker.GetTrackedBusCount())

	// 정류장이 변경된 버스만 필터링
	changedBuses := busTracker.FilterChangedStations(allBusLocations, logger)

	// 정류장이 변경된 버스가 없으면 Elasticsearch 전송 생략
	if len(changedBuses) == 0 {
		logger.Info("정류장 변경된 버스가 없어 ES 전송을 생략합니다")
		logger.Info("=== 처리 완료 ===")
		return
	}

	// 정류장이 변경된 버스 정보만 벌크로 Elasticsearch에 전송
	startTime := time.Now()

	logger.Infof("=== Elasticsearch 정류장 변경 버스 전송 시작 (%d대) ===", len(changedBuses))

	// 전송할 버스 정보를 간소화된 형태로 로깅
	for i, bus := range changedBuses {
		// 기본 위치 정보
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

		// GPS 정보
		var gpsInfo string
		if bus.GpsLati != 0 && bus.GpsLong != 0 {
			gpsInfo = fmt.Sprintf(", GPS: (%.6f, %.6f)", bus.GpsLati, bus.GpsLong)
		}

		// 상세 버스 정보 (요청된 필드들)
		var detailInfo string
		if bus.VehId != 0 {
			detailInfo = fmt.Sprintf(", 차량ID: %d, 잔여석: %d석, 혼잡도: %d",
				bus.VehId, bus.RemainSeatCnt, bus.Crowded)
		} else {
			detailInfo = fmt.Sprintf(", 혼잡도: %d", bus.Crowded)
		}

		// 정류장 변경 전송 로그
		logger.Infof("ES 정류장변경 전송 [%d/%d] - 차량번호: %s, 노선: %d, %s%s%s",
			i+1, len(changedBuses), bus.PlateNo, bus.RouteId, locationInfo, gpsInfo, detailInfo)
	}

	if err := esService.BulkSendBusLocations(indexName, changedBuses); err != nil {
		logger.Errorf("정류장 변경 버스 벌크 전송 중 오류 발생: %v", err)
		return
	}

	duration := time.Since(startTime)
	logger.Infof("정류장 변경 버스 벌크 전송 완료 - 처리 시간: %v", duration)
	logger.Info("=== Elasticsearch 정류장 변경 버스 전송 완료 ===")

	// 전송 완료 요약
	logger.Infof("💾 정류장 변경 데이터: %d건, 인덱스: %s, 소요시간: %v", len(changedBuses), indexName, duration)
	logger.Info("=== 처리 완료 ===")
}

// printUnifiedSystemStatus 통합 모드 시스템 상태 출력
func printUnifiedSystemStatus(cfg *config.Config, logger *utils.Logger) {
	currentTime := time.Now()

	logger.Info("=== 시스템 상태 (완전 즉시 처리) ===")
	logger.Infof("현재 시간: %s", currentTime.Format("2006-01-02 15:04:05"))
	logger.Infof("운영 시간 여부: %t", cfg.IsOperatingTime(currentTime))

	if !cfg.IsOperatingTime(currentTime) {
		nextTime := cfg.GetNextOperatingTime(currentTime)
		logger.Infof("다음 운영 시작: %s", nextTime.Format("2006-01-02 15:04:05"))
	}

	// 활성화된 API 정보
	var activeAPIs []string
	if cfg.API1Config.Enabled {
		activeAPIs = append(activeAPIs, "API1")
	}
	if cfg.API2Config.Enabled {
		activeAPIs = append(activeAPIs, "API2")
	}
	logger.Infof("활성화된 API: %v", activeAPIs)

	// 전체 모니터링 노선 수
	totalRoutes := len(cfg.API1Config.RouteIDs) + len(cfg.API2Config.RouteIDs)
	logger.Infof("총 모니터링 노선: %d개", totalRoutes)

	// 처리 방식 정보
	logger.Info("처리 방식: API 데이터 수신 → 즉시 통합 → 변경 감지 → 즉시 ES 전송")
	logger.Info("배치 처리: 없음 (모든 처리가 즉시 수행됨)")
	logger.Info("데이터 통합 워커: 비활성화")
	logger.Info("ES 배치 워커: 비활성화")

	logger.Info("===============================")
}
