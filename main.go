package main

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"bus-tracker/config"
	"bus-tracker/internal/services"
	"bus-tracker/internal/services/api"
	"bus-tracker/internal/services/cache"
	"bus-tracker/internal/services/storage"
	"bus-tracker/internal/services/tracker"
	"bus-tracker/internal/utils"
	"bus-tracker/internal/web"
)

func main() {
	cfg := config.LoadConfig()
	logger := utils.NewLogger()

	logger.Info("버스 트래커 시작 (통합 모드 + 웹 인터페이스)")
	cfg.PrintConfig()

	runUnifiedModeWithWeb(cfg, logger)
}

func runUnifiedModeWithWeb(cfg *config.Config, logger *utils.Logger) {
	logger.Info("통합 모드 + 웹 인터페이스 시작")

	// 환경변수 디버깅 정보
	logger.Infof("설정 확인 - API1 노선수: %d개, API2 노선수: %d개",
		len(cfg.API1Config.RouteIDs), len(cfg.API2Config.RouteIDs))
	logger.Infof("API1 노선 IDs: %v", cfg.API1Config.RouteIDs)
	logger.Infof("API2 노선 IDs: %v", cfg.API2Config.RouteIDs)
	logger.Infof("API1 BaseURL: %s", cfg.API1Config.BaseURL)
	logger.Infof("API2 BaseURL: %s", cfg.API2Config.BaseURL)

	// 🔧 새로운 종료 조건 설정 확인
	logger.Infof("버스 트래킹 종료 조건 - 미목격 시간: %v, 종점 도착 시 종료: %t",
		cfg.BusDisappearanceTimeout, cfg.EnableTerminalStop)

	esService := storage.NewElasticsearchService(cfg, logger)
	if err := esService.TestConnection(); err != nil {
		log.Fatalf("Elasticsearch 연결 실패: %v", err)
	}

	// 중복 체크 서비스 생성
	duplicateChecker := storage.NewElasticsearchDuplicateChecker(esService, logger, cfg.IndexName)
	busTracker := tracker.NewBusTrackerWithDuplicateCheck(cfg, duplicateChecker)

	// API2 우선 통합 캐시 생성
	unifiedStationCache := cache.NewStationCacheService(cfg, logger, "api2")

	var api1RouteIDs, api2RouteIDs []string
	if len(cfg.API1Config.RouteIDs) > 0 {
		api1RouteIDs = cfg.API1Config.RouteIDs
	}
	if len(cfg.API2Config.RouteIDs) > 0 {
		api2RouteIDs = cfg.API2Config.RouteIDs
	}

	allRouteIDs := append(api1RouteIDs, api2RouteIDs...)
	logger.Infof("전체 노선 IDs 통합: %v", allRouteIDs)

	if err := unifiedStationCache.LoadStationCache(allRouteIDs); err != nil {
		logger.Errorf("통합 정류소 캐시 로드 실패: %v", err)
	}

	var api1Client *api.API1Client
	var api2Client *api.API2Client

	if len(cfg.API1Config.RouteIDs) > 0 {
		api1Client = api.NewAPI1ClientWithSharedCache(cfg, logger, unifiedStationCache)
		logger.Infof("API1 클라이언트 생성 완료")
	}

	if len(cfg.API2Config.RouteIDs) > 0 {
		api2Client = api.NewAPI2ClientWithSharedCache(cfg, logger, unifiedStationCache)
		logger.Infof("API2 클라이언트 생성 완료")
	}

	// 중복 체크 기능이 포함된 통합 데이터 매니저 생성
	dataManager := services.NewUnifiedDataManagerWithDuplicateCheck(
		logger, busTracker, unifiedStationCache, esService, duplicateChecker, cfg.IndexName)

	orchestrator := services.NewMultiAPIOrchestrator(cfg, logger, api1Client, api2Client, dataManager)

	// 🌐 웹 서버 생성
	webServer := web.NewWebServer(
		cfg, logger, orchestrator, busTracker, unifiedStationCache,
		api1Client, api2Client, dataManager)

	// 운영시간 시작 시 운행 차수 카운터 초기화
	if cfg.IsOperatingTime(time.Now()) {
		busTracker.ResetTripCounters()
	}

	// WaitGroup으로 서비스 관리
	var wg sync.WaitGroup

	// 오케스트레이터 시작
	if err := orchestrator.Start(); err != nil {
		log.Fatalf("오케스트레이터 시작 실패: %v", err)
	}

	// 웹 서버 시작 (고루틴에서)
	wg.Add(1)
	go func() {
		defer wg.Done()

		// 웹 서버 포트 설정 (환경변수 또는 기본값)
		webPort := getWebPort()

		logger.Infof("웹 서버 시작 중 - 포트: %d", webPort)
		if err := webServer.Start(webPort); err != nil {
			logger.Errorf("웹 서버 시작 실패: %v", err)
		}
	}()

	// 신호 대기
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	logger.Info("🚌 통합 버스 트래커 실행 중")
	logger.Infof("📊 웹 대시보드: http://localhost:%d", getWebPort())
	logger.Info("⏹️  종료하려면 Ctrl+C를 누르세요")

	// 종료 신호 대기
	<-sigChan

	logger.Info("📶 종료 신호 수신 - 우아한 종료 시작")

	// 웹 서버 먼저 정지
	if err := webServer.Stop(); err != nil {
		logger.Errorf("웹 서버 정지 실패: %v", err)
	}

	// 오케스트레이터 정지
	orchestrator.Stop()

	// 모든 고루틴 완료 대기
	wg.Wait()

	logger.Info("✅ 통합 버스 트래커 종료 완료")
}

// getWebPort 웹 서버 포트 가져오기 (환경변수 또는 기본값)
func getWebPort() int {
	if port := os.Getenv("WEB_PORT"); port != "" {
		if p := parseInt(port); p > 0 {
			return p
		}
	}
	return 8080 // 기본 포트
}

// parseInt 문자열을 정수로 변환
func parseInt(s string) int {
	if len(s) == 0 {
		return 0
	}

	result := 0
	for _, char := range s {
		if char < '0' || char > '9' {
			return 0
		}
		result = result*10 + int(char-'0')
	}
	return result
}
