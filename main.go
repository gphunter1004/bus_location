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
	cfg := config.LoadConfig()
	logger := utils.NewLogger()

	logger.Info("=== 버스 트래커 시작 ===")
	cfg.PrintConfig()

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

// main.go의 runAPI1Mode 함수에서 캐시 로딩 부분을 다음과 같이 수정

func runAPI1Mode(cfg *config.Config, logger *utils.Logger) {
	logger.Info("=== API1 모드로 실행 ===")

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

	busTracker := services.NewBusTracker()
	apiClient := services.NewAPI1Client(api1Config, logger)
	esService := services.NewElasticsearchService(api1Config, logger)

	// 🔧 캐시 로딩 디버깅 강화
	logger.Info("🏗️ API1 정류소 정보 캐시 로딩 시작")
	logger.Infof("📋 로딩할 Route IDs: %v", cfg.API1Config.RouteIDs)
	logger.Infof("📊 Route IDs 개수: %d개", len(cfg.API1Config.RouteIDs))

	if len(cfg.API1Config.RouteIDs) == 0 {
		logger.Error("❌ API1Config.RouteIDs가 비어있습니다! .env 파일을 확인하세요")
		logger.Error("💡 .env 파일에 API1_ROUTE_IDS=233000266 설정이 있는지 확인하세요")
	} else {
		logger.Info("✅ Route IDs가 설정되어 있음, 캐시 로딩 시작")

		if err := apiClient.LoadStationCache(cfg.API1Config.RouteIDs); err != nil {
			logger.Errorf("❌ 정류소 캐시 로드 실패: %v", err)
			logger.Warnf("⚠️ 정류소 캐시 없이 계속 실행합니다 (정류소 이름 표시 안됨)")
		} else {
			routeCount, stationCount := apiClient.GetCacheStatistics()
			logger.Infof("✅ 정류소 캐시 로드 완료 - 노선: %d개, 정류소: %d개", routeCount, stationCount)

			// 각 노선별 정류소 개수 확인
			for _, routeID := range cfg.API1Config.RouteIDs {
				count := apiClient.GetRouteStationCount(routeID)
				logger.Infof("   📍 노선 %s: %d개 정류소", routeID, count)
			}
		}
	}

	if err := esService.TestConnection(); err != nil {
		log.Fatalf("Elasticsearch 연결 실패: %v", err)
	}
	logger.Info("Elasticsearch 연결 성공")

	runSingleAPILoop(apiClient, esService, busTracker, api1Config, logger, cfg.API1Config.RouteIDs, cfg.API1Config.Interval)
}

func runAPI2Mode(cfg *config.Config, logger *utils.Logger) {
	logger.Info("=== API2 모드로 실행 ===")

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

	busTracker := services.NewBusTracker()
	apiClient := services.NewAPI2Client(api2Config, logger)
	esService := services.NewElasticsearchService(api2Config, logger)

	logger.Info("API2 정류소 정보 캐시 로딩 시작")
	if err := apiClient.LoadStationCache(cfg.API2Config.RouteIDs); err != nil {
		logger.Warnf("정류소 캐시 로드 실패: %v", err)
	} else {
		routeCount, stationCount := apiClient.GetCacheStatistics()
		logger.Infof("정류소 캐시 로드 완료 - 노선: %d개, 정류소: %d개", routeCount, stationCount)
	}

	if err := esService.TestConnection(); err != nil {
		log.Fatalf("Elasticsearch 연결 실패: %v", err)
	}
	logger.Info("Elasticsearch 연결 성공")

	runSingleAPILoop(apiClient, esService, busTracker, api2Config, logger, cfg.API2Config.RouteIDs, cfg.API2Config.Interval)
}

// main.go 파일에서 runUnifiedMode 함수만 다음과 같이 교체하세요
// (파일의 다른 부분은 그대로 두고 이 함수만 교체)

// main.go의 runUnifiedMode 함수 수정 (통합 캐시 공유)

func runUnifiedMode(cfg *config.Config, logger *utils.Logger) {
	logger.Info("=== 통합 모드로 실행 (완전 즉시 처리 + 통합 캐시) ===")

	logger.Info("=== 서비스 초기화 시작 ===")

	busTracker := services.NewBusTracker()
	logger.Info("버스 트래커 초기화 완료")

	esService := services.NewElasticsearchService(cfg, logger)

	if err := esService.TestConnection(); err != nil {
		log.Fatalf("Elasticsearch 연결 실패: %v", err)
	}
	logger.Info("Elasticsearch 연결 테스트 성공")

	// 🔧 통합 정류소 캐시를 일반 StationCacheService로 변경
	logger.Info("=== 통합 정류소 캐시 초기화 시작 ===")

	// API2를 우선으로 사용하는 통합 캐시 생성
	unifiedStationCache := services.NewStationCacheService(cfg, logger, "api2")

	var api1RouteIDs, api2RouteIDs []string
	if cfg.API1Config.Enabled {
		api1RouteIDs = cfg.API1Config.RouteIDs
	}
	if cfg.API2Config.Enabled {
		api2RouteIDs = cfg.API2Config.RouteIDs
	}

	logger.Info("통합 정류소 캐시 로딩 시작...")
	logger.Infof("- API1 노선: %v", api1RouteIDs)
	logger.Infof("- API2 노선: %v", api2RouteIDs)

	// 🔧 모든 노선 ID를 하나의 배열로 합쳐서 로드
	allRouteIDs := append(api1RouteIDs, api2RouteIDs...)

	if err := unifiedStationCache.LoadStationCache(allRouteIDs); err != nil {
		logger.Errorf("통합 정류소 캐시 로드 실패: %v", err)
		logger.Warn("정류소 캐시 없이 실행하면 중복 데이터가 발생할 수 있습니다")
	} else {
		routeCount, stationCount := unifiedStationCache.GetCacheStatistics()
		logger.Infof("✅ 통합 정류소 캐시 로드 완료 - 노선: %d개, 정류소: %d개", routeCount, stationCount)
	}

	// 🔧 중요: API 클라이언트에 통합 캐시를 전달하도록 수정
	var api1Client *services.API1Client
	var api2Client *services.API2Client

	if cfg.API1Config.Enabled {
		logger.Info("=== API1 클라이언트 초기화 (통합 캐시 공유) ===")
		api1Config := &config.Config{
			ServiceKey: cfg.ServiceKey,
			CityCode:   cfg.CityCode,
			APIBaseURL: cfg.API1Config.BaseURL,
			RouteIDs:   cfg.API1Config.RouteIDs,
			APIType:    "api1",
		}

		// 🔧 API1 클라이언트를 통합 캐시와 함께 생성
		api1Client = services.NewAPI1ClientWithSharedCache(api1Config, logger, unifiedStationCache)
		logger.Infof("API1 클라이언트 초기화 완료 - 노선 %d개 (통합 캐시 공유)", len(cfg.API1Config.RouteIDs))
	}

	if cfg.API2Config.Enabled {
		logger.Info("=== API2 클라이언트 초기화 (통합 캐시 공유) ===")
		api2Config := &config.Config{
			ServiceKey: cfg.ServiceKey,
			CityCode:   cfg.CityCode,
			APIBaseURL: cfg.API2Config.BaseURL,
			RouteIDs:   cfg.API2Config.RouteIDs,
			APIType:    "api2",
		}

		// 🔧 API2 클라이언트를 통합 캐시와 함께 생성
		api2Client = services.NewAPI2ClientWithSharedCache(api2Config, logger, unifiedStationCache)
		logger.Infof("API2 클라이언트 초기화 완료 - 노선 %d개 (통합 캐시 공유)", len(cfg.API2Config.RouteIDs))
	}

	// 🔧 NewUnifiedDataManager 호출 시 StationCacheService 전달
	dataManager := services.NewUnifiedDataManager(logger, busTracker, unifiedStationCache, esService, cfg.IndexName)
	logger.Info("통합 데이터 매니저 초기화 완료 (통합 캐시 기반 + 즉시 처리)")

	orchestrator := services.NewMultiAPIOrchestrator(cfg, logger, api1Client, api2Client, dataManager)
	logger.Info("다중 API 오케스트레이터 초기화 완료")

	logger.Info("=== 모든 서비스 초기화 완료 ===")

	printUnifiedSystemStatus(cfg, logger)

	if err := orchestrator.Start(); err != nil {
		log.Fatalf("오케스트레이터 시작 실패: %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	logger.Info("=== 통합 버스 트래커 실행 중 (통합 캐시 + 완전 즉시 처리) ===")
	logger.Info("특징:")
	logger.Info("- 정류소 캐시: API2 우선 (GPS+상세정보), API1 보조")
	logger.Info("- 데이터 처리: StationSeq/NodeOrd 통합 (같은 정류장은 같은 값)")
	logger.Info("- 순차 검증: 뒤늦은 API의 역순 데이터는 정류장 정보 제외")
	logger.Info("- 즉시 전송: 정류장 변경시 ES 즉시 전송")
	logger.Info("- 캐시 공유: 모든 API 클라이언트가 동일한 통합 캐시 사용")
	logger.Info("종료하려면 Ctrl+C를 누르세요")

	<-sigChan

	logger.Info("=== 종료 신호 수신 - 우아한 종료 시작 ===")

	orchestrator.Stop()

	logger.Info("=== 통합 버스 트래커 종료 완료 ===")
}

func runSingleAPILoop(apiClient services.BusAPIClient, esService *services.ElasticsearchService,
	busTracker *services.BusTracker, cfg *config.Config, logger *utils.Logger,
	routeIDs []string, interval time.Duration) {

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	cleanupTicker := time.NewTicker(cfg.BusCleanupInterval)
	defer cleanupTicker.Stop()

	logger.Infof("%s 버스 정보 API 모니터링 시작", apiClient.GetAPIType())
	logger.Info("정류장 변경된 버스만 Elasticsearch로 전송됩니다")

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

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	logger.Infof("=== %s 모드 실행 중 ===", apiClient.GetAPIType())
	logger.Info("종료하려면 Ctrl+C를 누르세요")

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
			removedCount := busTracker.CleanupMissingBuses(cfg.BusTimeoutDuration, logger)
			if removedCount > 0 {
				logger.Infof("정리 완료 - 현재 추적 중인 버스: %d대", busTracker.GetTrackedBusCount())
			}
		}
	}
}

func processSingleAPICall(apiClient services.BusAPIClient, esService *services.ElasticsearchService,
	busTracker *services.BusTracker, logger *utils.Logger, routeIDs []string, indexName string) {

	logger.Infof("=== 버스 위치 API 호출 시작 (%s, %s) ===",
		time.Now().Format("15:04:05"), apiClient.GetAPIType())

	allBusLocations, err := apiClient.FetchAllBusLocations(routeIDs)
	if err != nil {
		logger.Errorf("API 호출 오류: %v", err)
		return
	}

	if len(allBusLocations) == 0 {
		logger.Warn("모든 노선에서 버스 위치 정보가 없습니다")
		return
	}

	logger.Infof("전체 수신된 버스 위치 정보: %d대", len(allBusLocations))
	logger.Infof("현재 추적 중인 버스: %d대", busTracker.GetTrackedBusCount())

	changedBuses := busTracker.FilterChangedStations(allBusLocations, logger)

	if len(changedBuses) == 0 {
		logger.Info("정류장 변경된 버스가 없어 ES 전송을 생략합니다")
		logger.Info("=== 처리 완료 ===")
		return
	}

	startTime := time.Now()

	logger.Infof("=== Elasticsearch 정류장 변경 버스 전송 시작 (%d대) ===", len(changedBuses))

	for i, bus := range changedBuses {
		var locationInfo string
		if bus.NodeNm != "" && bus.NodeId != "" {
			locationInfo = fmt.Sprintf("정류장: %s (%s), 순서: %d/%d",
				bus.NodeNm, bus.NodeId, bus.NodeOrd, bus.TotalStations)
		} else {
			locationInfo = fmt.Sprintf("정류장ID: %d, 순서: %d/%d",
				bus.StationId, bus.StationSeq, bus.TotalStations)
		}

		var gpsInfo string
		if bus.GpsLati != 0 && bus.GpsLong != 0 {
			gpsInfo = fmt.Sprintf(", GPS: (%.6f, %.6f)", bus.GpsLati, bus.GpsLong)
		}

		var detailInfo string
		if bus.VehId != 0 {
			detailInfo = fmt.Sprintf(", 차량ID: %d, 잔여석: %d석, 혼잡도: %d",
				bus.VehId, bus.RemainSeatCnt, bus.Crowded)
		} else {
			detailInfo = fmt.Sprintf(", 혼잡도: %d", bus.Crowded)
		}

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

	logger.Infof("💾 정류장 변경 데이터: %d건, 인덱스: %s, 소요시간: %v", len(changedBuses), indexName, duration)
	logger.Info("=== 처리 완료 ===")
}

func printUnifiedSystemStatus(cfg *config.Config, logger *utils.Logger) {
	currentTime := time.Now()

	logger.Info("=== 시스템 상태 (완전 즉시 처리 + 통합 캐시) ===")
	logger.Infof("현재 시간: %s", currentTime.Format("2006-01-02 15:04:05"))
	logger.Infof("운영 시간 여부: %t", cfg.IsOperatingTime(currentTime))

	if !cfg.IsOperatingTime(currentTime) {
		nextTime := cfg.GetNextOperatingTime(currentTime)
		logger.Infof("다음 운영 시작: %s", nextTime.Format("2006-01-02 15:04:05"))
	}

	var activeAPIs []string
	if cfg.API1Config.Enabled {
		activeAPIs = append(activeAPIs, "API1")
	}
	if cfg.API2Config.Enabled {
		activeAPIs = append(activeAPIs, "API2")
	}
	logger.Infof("활성화된 API: %v", activeAPIs)

	totalRoutes := len(cfg.API1Config.RouteIDs) + len(cfg.API2Config.RouteIDs)
	logger.Infof("총 모니터링 노선: %d개", totalRoutes)

	logger.Info("처리 방식: API 데이터 수신 → 순차검증 → 즉시 통합 → 변경 감지 → 즉시 ES 전송")
	logger.Info("캐시 전략: API2 우선 (GPS+상세정보), API1 보조")
	logger.Info("순차 검증: 역순 데이터의 정류장 정보 제외, 버스 정보만 업데이트")

	logger.Info("===============================")
}
