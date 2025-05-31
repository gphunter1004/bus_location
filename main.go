package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
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

	logger.Info("버스 트래커 시작 (통합 모드 + Fiber 웹 인터페이스)")
	cfg.PrintConfig()

	runUnifiedModeWithFiberWeb(cfg, logger)
}

func runUnifiedModeWithFiberWeb(cfg *config.Config, logger *utils.Logger) {
	logger.Info("통합 모드 + Fiber 웹 인터페이스 시작")

	// 환경변수 디버깅 정보
	logger.Infof("설정 확인 - API1 노선수: %d개, API2 노선수: %d개",
		len(cfg.API1Config.RouteIDs), len(cfg.API2Config.RouteIDs))

	// 버스 트래킹 종료 조건 설정 확인
	logger.Infof("버스 트래킹 종료 조건 - 미목격 시간: %v, 종점 도착 시 종료: %t",
		cfg.BusDisappearanceTimeout, cfg.EnableTerminalStop)

	// 운영시간 정보 출력
	logger.Infof("운영시간: %02d:%02d ~ %02d:%02d",
		cfg.OperatingStartHour, cfg.OperatingStartMinute, cfg.OperatingEndHour, cfg.OperatingEndMinute)

	// Elasticsearch 연결 확인 (재시도 로직 추가)
	esService := storage.NewElasticsearchService(cfg, logger)
	if err := connectToElasticsearchWithRetry(esService, logger, 3); err != nil {
		log.Fatalf("Elasticsearch 연결 실패: %v", err)
	}

	// 중복 체크 서비스 생성
	duplicateChecker := storage.NewElasticsearchDuplicateChecker(esService, logger, cfg.IndexName)
	busTracker := tracker.NewBusTrackerWithDuplicateCheck(cfg, duplicateChecker)

	// 현재 운영일자 확인
	currentOperatingDate := busTracker.GetCurrentOperatingDate()
	logger.Infof("현재 운영일자: %s", currentOperatingDate)

	// API2 우선 통합 캐시 생성
	unifiedStationCache := cache.NewStationCacheService(cfg, logger, "unified")

	var api1RouteIDs, api2RouteIDs []string
	if len(cfg.API1Config.RouteIDs) > 0 {
		api1RouteIDs = cfg.API1Config.RouteIDs
	}
	if len(cfg.API2Config.RouteIDs) > 0 {
		api2RouteIDs = cfg.API2Config.RouteIDs
	}

	// 공용 헬퍼 사용으로 중복 제거된 노선 목록 생성
	allRouteIDs := append(api1RouteIDs, api2RouteIDs...)
	allRouteIDs = utils.Slice.RemoveDuplicateStrings(allRouteIDs)
	logger.Infof("전체 노선 IDs 통합 (중복 제거 후): %v", allRouteIDs)

	// 정류소 캐시 로드 (백그라운드에서 진행)
	if len(allRouteIDs) > 0 {
		go func() {
			logger.Info("정류소 캐시 로딩 시작...")
			if err := unifiedStationCache.LoadStationCache(allRouteIDs); err != nil {
				logger.Errorf("통합 정류소 캐시 로드 실패: %v", err)
			} else {
				logger.Info("정류소 캐시 로딩 완료")
			}
		}()
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

	// Fiber 웹 서버 생성
	fiberServer := web.NewFiberServer(
		cfg, logger, orchestrator, busTracker, unifiedStationCache,
		api1Client, api2Client, dataManager)

	// 개발 모드 확인 및 설정
	if isDevelopmentMode() {
		fiberServer.SetupDevelopmentMode()
		logger.Info("개발 모드가 활성화되었습니다")
	}

	// 컨텍스트와 종료 관리
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	webPort := getWebPort()

	// 신호 처리 설정 (개선된 버전)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 웹 서버 시작 (개선된 종료 처리)
	wg.Add(1)
	go func() {
		defer wg.Done()
		startWebServerWithGracefulShutdown(ctx, fiberServer, webPort, logger)
	}()

	// 웹 서버 준비 대기
	if waitForServerReady(webPort, logger, 10*time.Second) {
		logger.Infof("✅ 웹 서버 준비 완료 - 포트: %d", webPort)
	} else {
		logger.Warn("⚠️ 웹 서버 준비 시간 초과, 계속 진행")
	}

	// 📅 일일 운영시간 관리 워커 시작
	wg.Add(1)
	go func() {
		defer wg.Done()
		runDailyOperatingScheduleWorkerWithContext(ctx, cfg, logger, busTracker)
	}()

	// 오케스트레이터 시작 (웹 서버 준비 후)
	logger.Info("오케스트레이터 시작 중...")
	if err := orchestrator.Start(); err != nil {
		log.Fatalf("오케스트레이터 시작 실패: %v", err)
	}
	logger.Info("✅ 오케스트레이터 시작 완료")

	// 시작 완료 메시지
	logger.Info("🚌 통합 버스 트래커 실행 중 (Fiber)")
	logger.Infof("📊 웹 대시보드: http://localhost:%d", webPort)
	logger.Infof("📊 대시보드: http://localhost:%d/dashboard", webPort)
	logger.Infof("📈 모니터링: http://localhost:%d/monitoring", webPort)
	logger.Infof("📋 API 문서: http://localhost:%d/api-doc", webPort)
	logger.Infof("📡 API 엔드포인트: http://localhost:%d/api/v1/", webPort)
	logger.Infof("📅 운영일자: %s", currentOperatingDate)

	if isDevelopmentMode() {
		logger.Infof("🔧 개발 모드 엔드포인트: http://localhost:%d/dev/", webPort)
	}

	logger.Info("⏹️  종료하려면 Ctrl+C를 누르세요")
	logger.Info("🌐 브라우저에서 웹 인터페이스에 접속할 수 있습니다")

	// 종료 신호 대기
	<-sigChan

	logger.Info("📶 종료 신호 수신 - 우아한 종료 시작")

	// 전체 종료 타임아웃 설정 (최대 30초)
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// 종료 순서:
	// 1. 새로운 요청 차단을 위해 컨텍스트 취소
	cancel()

	// 2. 오케스트레이터 정지 (고루틴에서 비동기로)
	go func() {
		logger.Info("🔄 오케스트레이터 정지 중...")
		orchestrator.Stop()
		logger.Info("✅ 오케스트레이터 정지 완료")
	}()

	// 3. WaitGroup 완료 대기 (타임아웃 포함)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("✅ 모든 고루틴 정상 종료")
	case <-shutdownCtx.Done():
		logger.Warn("⚠️ 종료 타임아웃 - 강제 종료")
	}

	logger.Info("✅ 통합 버스 트래커 종료 완료")
}

// startWebServerWithGracefulShutdown 웹 서버를 컨텍스트와 함께 시작
func startWebServerWithGracefulShutdown(ctx context.Context, fiberServer *web.FiberServer, port int, logger *utils.Logger) {
	// 서버 시작 (논블로킹)
	go func() {
		if err := fiberServer.Start(port); err != nil {
			logger.Errorf("Fiber 웹 서버 시작 실패: %v", err)
		}
	}()

	// 컨텍스트 취소 신호 대기
	<-ctx.Done()

	// 웹 서버 종료 (타임아웃 포함)
	logger.Info("🛑 Fiber 웹 서버 정지 중...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- fiberServer.Stop()
	}()

	select {
	case err := <-shutdownDone:
		if err != nil {
			logger.Errorf("Fiber 웹 서버 정지 실패: %v", err)
		} else {
			logger.Info("✅ Fiber 웹 서버 정지 완료")
		}
	case <-shutdownCtx.Done():
		logger.Warn("⚠️ 웹 서버 종료 타임아웃 - 강제 종료")
	}
}

// waitForServerReady 서버 준비 상태 대기
func waitForServerReady(port int, logger *utils.Logger, timeout time.Duration) bool {
	logger.Info("웹 서버 시작 대기 중...")

	client := &http.Client{
		Timeout: 2 * time.Second,
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get(fmt.Sprintf("http://localhost:%d/health", port))
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return true
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(500 * time.Millisecond)
	}

	return false
}

// runDailyOperatingScheduleWorkerWithContext 컨텍스트를 사용하는 일일 운영시간 관리 워커
func runDailyOperatingScheduleWorkerWithContext(ctx context.Context, cfg *config.Config, logger *utils.Logger, busTracker *tracker.BusTrackerWithDuplicateCheck) {
	logger.Info("📅 일일 운영시간 관리 워커 시작")

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	lastCheckDate := ""

	for {
		select {
		case <-ctx.Done():
			logger.Info("📅 일일 운영시간 관리 워커 종료")
			return
		case <-ticker.C:
			now := time.Now()
			currentDate := getDailyOperatingDate(now, cfg)

			if lastCheckDate != "" && lastCheckDate != currentDate {
				logger.Infof("📅 운영일자 변경 감지: %s -> %s", lastCheckDate, currentDate)
				busTracker.ResetDailyTripCounters()
				logger.Infof("🔄 일일 운행 차수 카운터 리셋 완료 (새 운영일: %s)", currentDate)
			}

			lastCheckDate = currentDate

			if now.Minute()%30 == 0 && now.Second() < 5 {
				operatingStatus := "운영시간 외"
				if cfg.IsOperatingTime(now) {
					operatingStatus = "운영시간 내"
				}

				trackedBuses := busTracker.GetTrackedBusCount()
				dailyStats := busTracker.GetDailyTripStatistics()

				logger.Infof("📊 상태 요약 [%s] - 운영일자: %s, 추적버스: %d대, 일일차수기록: %d대",
					operatingStatus, currentDate, trackedBuses, len(dailyStats))
			}
		}
	}
}

// connectToElasticsearchWithRetry Elasticsearch 연결 재시도
func connectToElasticsearchWithRetry(esService *storage.ElasticsearchService, logger *utils.Logger, maxRetries int) error {
	for i := 0; i < maxRetries; i++ {
		if err := esService.TestConnection(); err != nil {
			logger.Warnf("Elasticsearch 연결 실패 (시도 %d/%d): %v", i+1, maxRetries, err)
			if i < maxRetries-1 {
				time.Sleep(time.Duration(i+1) * 2 * time.Second) // 2초, 4초, 6초...
			}
		} else {
			logger.Info("✅ Elasticsearch 연결 성공")
			return nil
		}
	}
	return fmt.Errorf("최대 재시도 횟수 초과")
}

// getDailyOperatingDate 운영일자 계산 (운영시간 기준)
func getDailyOperatingDate(now time.Time, cfg *config.Config) string {
	if !cfg.IsOperatingTime(now) {
		nextOperatingTime := cfg.GetNextOperatingTime(now)
		if nextOperatingTime.Day() != now.Day() {
			return now.AddDate(0, 0, -1).Format("2006-01-02")
		}
	}
	return now.Format("2006-01-02")
}

// getWebPort 웹 서버 포트 가져오기 (환경변수 또는 기본값)
func getWebPort() int {
	return utils.Convert.StringToInt(os.Getenv("WEB_PORT"), 8080)
}

// isDevelopmentMode 개발 모드 여부 확인
func isDevelopmentMode() bool {
	env := os.Getenv("ENVIRONMENT")
	return env == "development" || env == "dev" || env == ""
}
