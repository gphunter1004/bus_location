// main.go - 최초 데이터 로딩 포함 완전 버전
package main

import (
	"context"
	"fmt"
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
	"bus-tracker/internal/services/redis"
	"bus-tracker/internal/services/storage"
	"bus-tracker/internal/utils"
)

func main() {
	cfg := config.LoadConfig()
	logger := utils.NewLogger()

	logger.Info("🚌 최초 데이터 로딩 + 정상 운영 버스 트래커 시작")
	cfg.PrintConfig()

	runWithInitialLoading(cfg, logger)
}

func runWithInitialLoading(cfg *config.Config, logger *utils.Logger) {
	logger.Info("📦 최초 데이터 로딩 모드 시작")

	// Elasticsearch 연결 확인
	esService := storage.NewElasticsearchService(cfg, logger)
	if err := connectToElasticsearchWithRetry(esService, logger, 3); err != nil {
		log.Fatalf("Elasticsearch 연결 실패: %v", err)
	}

	// Redis 버스 데이터 매니저 생성
	logger.Info("📦 Redis 버스 데이터 매니저 초기화 중...")
	redisBusManager := redis.NewRedisBusDataManager(cfg, logger)
	defer redisBusManager.Close()

	// 중복 체크 서비스 생성
	duplicateChecker := storage.NewElasticsearchDuplicateChecker(esService, logger, cfg.IndexName)

	// 정류소 캐시 생성
	logger.Info("📦 Redis 정류소 캐시 시스템 V2 초기화 중...")
	var stationCache cache.StationCacheInterface = cache.NewRedisStationCacheServiceV2(cfg, logger, "unified")
	redisV2Cache := stationCache.(*cache.RedisStationCacheServiceV2)
	defer redisV2Cache.Close()

	// 노선 목록 통합
	allRouteIDs := append(cfg.API1Config.RouteIDs, cfg.API2Config.RouteIDs...)
	allRouteIDs = utils.Slice.RemoveDuplicateStrings(allRouteIDs)

	// 정류소 캐시 로드 (백그라운드)
	if len(allRouteIDs) > 0 {
		go func() {
			logger.Info("📦 정류소 캐시 로딩 시작...")
			loadStart := time.Now()

			if err := stationCache.LoadStationCache(allRouteIDs); err != nil {
				logger.Errorf("정류소 캐시 로드 실패: %v", err)
			} else {
				routes, stations := stationCache.GetCacheStatistics()
				logger.Infof("✅ 정류소 캐시 로딩 완료 - %d노선/%d정류소 (소요시간: %v)",
					routes, stations, time.Since(loadStart))
			}
		}()
	}

	// API 클라이언트 생성
	var api1Client *api.API1Client
	var api2Client *api.API2Client

	if len(cfg.API1Config.RouteIDs) > 0 {
		api1Client = api.NewAPI1ClientWithSharedCache(cfg, logger, stationCache)
		logger.Info("API1 클라이언트 생성 완료")
	}

	if len(cfg.API2Config.RouteIDs) > 0 {
		api2Client = api.NewAPI2ClientWithSharedCache(cfg, logger, stationCache)
		logger.Info("API2 클라이언트 생성 완료")
	}

	// 통합 데이터 매니저 생성
	logger.Info("📦 통합 데이터 매니저 초기화 중...")
	unifiedManager := services.NewUnifiedDataManager(
		logger,
		stationCache,
		esService,
		redisBusManager,
		duplicateChecker,
		cfg.IndexName,
	)
	unifiedManager.InitializeBusTracker(cfg)

	// 🔧 최초 데이터 로딩 수행
	logger.Info("🚀 최초 데이터 로딩 시작...")
	if err := unifiedManager.PerformInitialDataLoading(api1Client, api2Client); err != nil {
		logger.Errorf("최초 데이터 로딩 실패: %v", err)
		// 실패해도 정상 운영은 계속 진행
	}

	// 일일 캐시 새로고침 관리자 생성 및 시작
	dailyCacheRefreshManager := cache.NewDailyCacheRefreshManager(cfg, logger, redisV2Cache, allRouteIDs)
	dailyCacheRefreshManager.Start()
	defer dailyCacheRefreshManager.Stop()

	// 오케스트레이터 생성 및 시작
	logger.Info("📦 오케스트레이터 초기화 중...")
	orchestrator := services.NewMultiAPIOrchestrator(cfg, logger, api1Client, api2Client, unifiedManager)

	// 정상 운영 시작 (최초 로딩 완료 후)
	if err := orchestrator.Start(); err != nil {
		logger.Fatalf("오케스트레이터 시작 실패: %v", err)
	}
	defer orchestrator.Stop()

	// 신호 처리 및 Graceful Shutdown
	handleGracefulShutdown(logger, orchestrator, dailyCacheRefreshManager, redisV2Cache, redisBusManager)

	logger.Info("🏁 버스 트래커 종료 완료")
}

// connectToElasticsearchWithRetry Elasticsearch 연결 재시도
func connectToElasticsearchWithRetry(esService *storage.ElasticsearchService, logger *utils.Logger, maxRetries int) error {
	for i := 1; i <= maxRetries; i++ {
		if err := esService.TestConnection(); err != nil {
			if i == maxRetries {
				return fmt.Errorf("최대 재시도 횟수 초과: %v", err)
			}
			logger.Warnf("Elasticsearch 연결 실패 (시도 %d/%d): %v, 3초 후 재시도...", i, maxRetries, err)
			time.Sleep(3 * time.Second)
			continue
		}
		logger.Info("✅ Elasticsearch 연결 성공")
		return nil
	}
	return fmt.Errorf("연결 재시도 실패")
}

// handleGracefulShutdown Graceful Shutdown 처리
func handleGracefulShutdown(
	logger *utils.Logger,
	orchestrator *services.MultiAPIOrchestrator,
	dailyCacheRefreshManager *cache.DailyCacheRefreshManager,
	redisV2Cache *cache.RedisStationCacheServiceV2,
	redisBusManager *redis.RedisBusDataManager) {

	// 신호 수신 채널 생성
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// 현재 시간 출력 및 상태 체크 고루틴
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	// 주기적 상태 출력 (5분마다)
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				printSystemStatus(logger, orchestrator, dailyCacheRefreshManager, redisBusManager)
			}
		}
	}()

	// 초기 상태 출력 (3초 후)
	time.AfterFunc(3*time.Second, func() {
		printSystemStatus(logger, orchestrator, dailyCacheRefreshManager, redisBusManager)
	})

	// 종료 신호 대기
	<-quit
	logger.Info("🛑 종료 신호 수신, Graceful Shutdown 시작...")

	cancel() // 상태 출력 고루틴 중지

	// 1. 오케스트레이터 중지
	logger.Info("1️⃣ 오케스트레이터 중지 중...")
	orchestrator.Stop()

	// 2. 일일 캐시 새로고침 매니저 중지
	logger.Info("2️⃣ 일일 캐시 새로고침 매니저 중지 중...")
	dailyCacheRefreshManager.Stop()

	// 3. 최종 상태 출력
	logger.Info("3️⃣ 최종 시스템 상태 출력...")
	printFinalStatus(logger, redisBusManager, redisV2Cache)

	// 4. 고루틴 완료 대기
	logger.Info("4️⃣ 백그라운드 작업 완료 대기...")
	wg.Wait()

	logger.Info("✅ Graceful Shutdown 완료")
}

// printSystemStatus 시스템 상태 출력
func printSystemStatus(
	logger *utils.Logger,
	orchestrator *services.MultiAPIOrchestrator,
	dailyCacheRefreshManager *cache.DailyCacheRefreshManager,
	redisBusManager *redis.RedisBusDataManager) {

	logger.Info("📊 === 시스템 상태 리포트 ===")

	// 오케스트레이터 상태
	stats := orchestrator.GetDetailedStatistics()
	logger.Infof("🔄 오케스트레이터 - 실행중: %v, API1: %v, API2: %v",
		stats["is_running"], stats["api1_enabled"], stats["api2_enabled"])
	logger.Infof("⏰ 운영시간: %v, 현재운영중: %v, 최초로딩완료: %v",
		stats["operating_schedule"], stats["is_operating_time"], stats["initial_loading_done"])

	// Redis 버스 데이터 상태
	redisBusManager.PrintBusDataStatistics()

	// 캐시 새로고침 상태
	refreshStatus := dailyCacheRefreshManager.GetRefreshStatus()
	logger.Infof("🔄 캐시 새로고침 - 마지막: %v, 진행중: %v, 노선수: %v",
		refreshStatus["lastRefreshDate"], refreshStatus["isRefreshing"], refreshStatus["totalRoutes"])

	logger.Info("📊 === 상태 리포트 완료 ===")
}

// printFinalStatus 최종 상태 출력
func printFinalStatus(
	logger *utils.Logger,
	redisBusManager *redis.RedisBusDataManager,
	redisV2Cache *cache.RedisStationCacheServiceV2) {

	logger.Info("📊 === 최종 시스템 상태 ===")

	// Redis 버스 데이터 최종 상태
	if stats, err := redisBusManager.GetBusStatistics(); err == nil {
		logger.Infof("🚌 최종 버스 데이터 - 총: %v대, 활성: %v대",
			stats["total_buses"], stats["active_buses"])
	}

	// Redis 정류소 캐시 최종 상태
	redisV2Cache.PrintCacheStatus()

	logger.Info("📊 === 최종 상태 완료 ===")
}
