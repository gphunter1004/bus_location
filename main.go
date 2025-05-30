// NewMultiAPIOrchestrator 함수 시그니처도 인터페이스를 받도록 수정해야 합니다
// internal/services/orchestrator.go의 NewMultiAPIOrchestrator 함수를 다음과 같이 수정:

//	type MultiAPIOrchestrator struct {
//		config      *config.Config
//		logger      *utils.Logger
//		api1Client  *api.API1Client
//		api2Client  *api.API2Client
//		dataManager UnifiedDataManagerInterface  // 인터페이스로 변경
//
//		// 제어 관련
//		ctx       context.Context
//		cancel    context.CancelFunc
//		wg        sync.WaitGroup
//		isRunning bool
//		mutex     sync.RWMutex
package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bus-tracker/config"
	"bus-tracker/internal/services"
	"bus-tracker/internal/services/api"
	"bus-tracker/internal/services/cache"
	"bus-tracker/internal/services/storage"
	"bus-tracker/internal/services/tracker"
	"bus-tracker/internal/utils"
)

func main() {
	cfg := config.LoadConfig()
	logger := utils.NewLogger()

	logger.Info("버스 트래커 시작")

	switch cfg.Mode {
	case "api1":
		runAPI1Mode(cfg, logger)
	case "api2":
		runAPI2Mode(cfg, logger)
	case "unified":
		runUnifiedMode(cfg, logger)
	default:
		log.Fatalf("지원하지 않는 모드입니다: %s", cfg.Mode)
	}
}

func runAPI1Mode(cfg *config.Config, logger *utils.Logger) {
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

	esService := storage.NewElasticsearchService(api1Config, logger)
	if err := esService.TestConnection(); err != nil {
		log.Fatalf("Elasticsearch 연결 실패: %v", err)
	}

	// 중복 체크 서비스 생성
	duplicateChecker := storage.NewElasticsearchDuplicateChecker(esService, logger, cfg.IndexName)
	busTracker := tracker.NewBusTrackerWithDuplicateCheck(duplicateChecker)

	apiClient := api.NewAPI1Client(api1Config, logger)

	if len(cfg.API1Config.RouteIDs) == 0 {
		logger.Error("API1Config.RouteIDs가 비어있습니다")
	} else {
		if err := apiClient.LoadStationCache(cfg.API1Config.RouteIDs); err != nil {
			logger.Errorf("정류소 캐시 로드 실패: %v", err)
		}
	}

	runSingleAPILoop(apiClient, esService, busTracker, api1Config, logger, cfg.API1Config.RouteIDs, cfg.API1Config.Interval)
}

func runAPI2Mode(cfg *config.Config, logger *utils.Logger) {
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

	esService := storage.NewElasticsearchService(api2Config, logger)
	if err := esService.TestConnection(); err != nil {
		log.Fatalf("Elasticsearch 연결 실패: %v", err)
	}

	// 중복 체크 서비스 생성
	duplicateChecker := storage.NewElasticsearchDuplicateChecker(esService, logger, cfg.IndexName)
	busTracker := tracker.NewBusTrackerWithDuplicateCheck(duplicateChecker)

	apiClient := api.NewAPI2Client(api2Config, logger)

	if err := apiClient.LoadStationCache(cfg.API2Config.RouteIDs); err != nil {
		logger.Errorf("정류소 캐시 로드 실패: %v", err)
	}

	runSingleAPILoop(apiClient, esService, busTracker, api2Config, logger, cfg.API2Config.RouteIDs, cfg.API2Config.Interval)
}

func runUnifiedMode(cfg *config.Config, logger *utils.Logger) {
	logger.Info("통합 모드 시작")

	// 환경변수 디버깅 정보
	logger.Infof("설정 확인 - API1 활성화: %t, API2 활성화: %t", cfg.API1Config.Enabled, cfg.API2Config.Enabled)
	logger.Infof("API1 노선 IDs: %v", cfg.API1Config.RouteIDs)
	logger.Infof("API2 노선 IDs: %v", cfg.API2Config.RouteIDs)
	logger.Infof("API1 BaseURL: %s", cfg.API1Config.BaseURL)
	logger.Infof("API2 BaseURL: %s", cfg.API2Config.BaseURL)

	esService := storage.NewElasticsearchService(cfg, logger)
	if err := esService.TestConnection(); err != nil {
		log.Fatalf("Elasticsearch 연결 실패: %v", err)
	}

	// 중복 체크 서비스 생성
	duplicateChecker := storage.NewElasticsearchDuplicateChecker(esService, logger, cfg.IndexName)
	busTracker := tracker.NewBusTrackerWithDuplicateCheck(duplicateChecker)

	// API2 우선 통합 캐시 생성
	unifiedStationCache := cache.NewStationCacheService(cfg, logger, "api2")

	var api1RouteIDs, api2RouteIDs []string
	if cfg.API1Config.Enabled {
		api1RouteIDs = cfg.API1Config.RouteIDs
	}
	if cfg.API2Config.Enabled {
		api2RouteIDs = cfg.API2Config.RouteIDs
	}

	allRouteIDs := append(api1RouteIDs, api2RouteIDs...)
	logger.Infof("전체 노선 IDs 통합: %v", allRouteIDs)

	if err := unifiedStationCache.LoadStationCache(allRouteIDs); err != nil {
		logger.Errorf("통합 정류소 캐시 로드 실패: %v", err)
	}

	var api1Client *api.API1Client
	var api2Client *api.API2Client

	if cfg.API1Config.Enabled {
		api1Config := &config.Config{
			ServiceKey: cfg.ServiceKey,
			CityCode:   cfg.CityCode,
			APIBaseURL: cfg.API1Config.BaseURL,
			RouteIDs:   cfg.API1Config.RouteIDs,
			APIType:    "api1",
		}
		api1Client = api.NewAPI1ClientWithSharedCache(api1Config, logger, unifiedStationCache)
		logger.Infof("API1 클라이언트 생성 완료")
	}

	if cfg.API2Config.Enabled {
		api2Config := &config.Config{
			ServiceKey: cfg.ServiceKey,
			CityCode:   cfg.CityCode,
			APIBaseURL: cfg.API2Config.BaseURL,
			RouteIDs:   cfg.API2Config.RouteIDs,
			APIType:    "api2",
		}
		api2Client = api.NewAPI2ClientWithSharedCache(api2Config, logger, unifiedStationCache)
		logger.Infof("API2 클라이언트 생성 완료")
	}

	// 중복 체크 기능이 포함된 통합 데이터 매니저 생성
	var dataManager services.UnifiedDataManagerInterface
	dataManager = services.NewUnifiedDataManagerWithDuplicateCheck(
		logger, busTracker, unifiedStationCache, esService, duplicateChecker, cfg.IndexName)

	orchestrator := services.NewMultiAPIOrchestrator(cfg, logger, api1Client, api2Client, dataManager)

	// 운영시간 시작 시 운행 차수 카운터 초기화
	if cfg.IsOperatingTime(time.Now()) {
		busTracker.ResetTripCounters()
	}

	if err := orchestrator.Start(); err != nil {
		log.Fatalf("오케스트레이터 시작 실패: %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	logger.Info("통합 버스 트래커 실행 중 (Ctrl+C로 종료)")

	<-sigChan

	logger.Info("종료 신호 수신 - 우아한 종료 시작")
	orchestrator.Stop()
	logger.Info("통합 버스 트래커 종료 완료")
}

func runSingleAPILoop(apiClient api.BusAPIClient, esService *storage.ElasticsearchService,
	busTracker *tracker.BusTrackerWithDuplicateCheck, cfg *config.Config, logger *utils.Logger,
	routeIDs []string, interval time.Duration) {

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	cleanupTicker := time.NewTicker(cfg.BusCleanupInterval)
	defer cleanupTicker.Stop()

	// 운영시간 시작 시 운행 차수 카운터 초기화
	if cfg.IsOperatingTime(time.Now()) {
		busTracker.ResetTripCounters()
		processSingleAPICall(apiClient, esService, busTracker, logger, routeIDs, cfg.IndexName)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	logger.Infof("%s 모드 실행 중", apiClient.GetAPIType())

	for {
		select {
		case <-sigChan:
			logger.Infof("%s 모드 종료", apiClient.GetAPIType())
			return

		case <-ticker.C:
			if cfg.IsOperatingTime(time.Now()) {
				processSingleAPICall(apiClient, esService, busTracker, logger, routeIDs, cfg.IndexName)
			}

		case <-cleanupTicker.C:
			busTracker.CleanupMissingBuses(cfg.BusTimeoutDuration, logger)
		}
	}
}

func processSingleAPICall(apiClient api.BusAPIClient, esService *storage.ElasticsearchService,
	busTracker *tracker.BusTrackerWithDuplicateCheck, logger *utils.Logger, routeIDs []string, indexName string) {

	allBusLocations, err := apiClient.FetchAllBusLocations(routeIDs)
	if err != nil {
		logger.Errorf("API 호출 오류: %v", err)
		return
	}

	if len(allBusLocations) == 0 {
		return
	}

	// 중복 체크가 포함된 필터링 사용
	changedBuses := busTracker.FilterChangedStationsWithDuplicateCheck(allBusLocations, logger)

	if len(changedBuses) == 0 {
		return
	}

	for i, bus := range changedBuses {
		logger.Infof("ES 전송 [%d/%d] - 차량: %s, 노선: %s, 정류장: %s (%s), %d차수",
			i+1, len(changedBuses), bus.PlateNo, bus.GetRouteIDString(), bus.NodeNm, bus.NodeId, bus.TripNumber)
	}

	if err := esService.BulkSendBusLocations(indexName, changedBuses); err != nil {
		logger.Errorf("ES 전송 실패: %v", err)
		return
	}
}
