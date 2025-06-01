// runSimplifiedManagementWorker 단순화된 관// main.go - Redis 중심 단순화 버전
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

	logger.Info("🚌 Redis 중심 단순화 버스 트래커 시작")
	cfg.PrintConfig()

	runSimplifiedMode(cfg, logger)
}

func runSimplifiedMode(cfg *config.Config, logger *utils.Logger) {
	logger.Info("📦 Redis 중심 단순화 모드 시작")

	// 환경변수 디버깅 정보
	logger.Infof("설정 확인 - API1 노선수: %d개, API2 노선수: %d개",
		len(cfg.API1Config.RouteIDs), len(cfg.API2Config.RouteIDs))

	// 운영시간 정보 출력
	logger.Infof("운영시간: %02d:%02d ~ %02d:%02d",
		cfg.OperatingStartHour, cfg.OperatingStartMinute, cfg.OperatingEndHour, cfg.OperatingEndMinute)

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

	// 정류소 캐시 생성 (Redis V2 - L1 제거됨)
	logger.Info("📦 Redis 정류소 캐시 시스템 V2 초기화 중...")
	var stationCache cache.StationCacheInterface = cache.NewRedisStationCacheServiceV2(cfg, logger, "unified")
	redisV2Cache := stationCache.(*cache.RedisStationCacheServiceV2)
	defer redisV2Cache.Close()

	// 노선 목록 통합
	allRouteIDs := append(cfg.API1Config.RouteIDs, cfg.API2Config.RouteIDs...)
	allRouteIDs = utils.Slice.RemoveDuplicateStrings(allRouteIDs)
	logger.Infof("전체 노선 IDs 통합: %v", allRouteIDs)

	// 정류소 캐시 로드
	if len(allRouteIDs) > 0 {
		go func() {
			logger.Info("📦 정류소 캐시 로딩 시작...")
			loadStart := time.Now()

			if err := stationCache.LoadStationCache(allRouteIDs); err != nil {
				logger.Errorf("정류소 캐시 로드 실패: %v", err)
			} else {
				loadDuration := time.Since(loadStart)
				routes, stations := stationCache.GetCacheStatistics()
				logger.Infof("✅ 정류소 캐시 로딩 완료 - %d노선/%d정류소 (소요시간: %v)",
					routes, stations, loadDuration)
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

	// 단순화된 통합 데이터 매니저 생성
	dataManager := services.NewSimplifiedUnifiedDataManager(
		logger, stationCache, esService, redisBusManager, duplicateChecker, cfg.IndexName)

	// 오케스트레이터 생성
	orchestrator := services.NewMultiAPIOrchestrator(cfg, logger, api1Client, api2Client, dataManager)

	// 컨텍스트와 종료 관리
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	// 신호 처리 설정
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 단순화된 관리 워커 시작
	wg.Add(1)
	go func() {
		defer wg.Done()
		runSimplifiedManagementWorker(ctx, cfg, logger, redisBusManager, redisV2Cache)
	}()

	// 오케스트레이터 시작
	logger.Info("오케스트레이터 시작 중...")
	if err := orchestrator.Start(); err != nil {
		log.Fatalf("오케스트레이터 시작 실패: %v", err)
	}
	logger.Info("✅ 오케스트레이터 시작 완료")

	// 시작 완료 메시지
	logger.Info("🚌 Redis 중심 단순화 버스 트래커 실행 중")
	logger.Info("🔄 데이터 플로우: API → Redis → ES")
	logger.Info("⏹️  종료하려면 Ctrl+C를 누르세요")

	// 정기적인 상태 출력 (단순화)
	go func() {
		ticker := time.NewTicker(10 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				printSimplifiedStatus(logger, dataManager, redisV2Cache, orchestrator)
			}
		}
	}()

	// 종료 신호 대기
	<-sigChan

	logger.Info("📶 종료 신호 수신 - 우아한 종료 시작")

	// 전체 종료 타임아웃 설정
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// 종료 순서
	cancel()

	go func() {
		logger.Info("🔄 오케스트레이터 정지 중...")
		orchestrator.Stop()
		logger.Info("✅ 오케스트레이터 정지 완료")
	}()

	// WaitGroup 완료 대기
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

	// Redis 연결 종료
	if err := redisV2Cache.Close(); err != nil {
		logger.Errorf("Redis 정류소 캐시 연결 종료 실패: %v", err)
	} else {
		logger.Info("📦 Redis 정류소 캐시 연결 정상 종료")
	}

	if err := redisBusManager.Close(); err != nil {
		logger.Errorf("Redis 버스 데이터 매니저 연결 종료 실패: %v", err)
	} else {
		logger.Info("📦 Redis 버스 데이터 매니저 연결 정상 종료")
	}

	logger.Info("✅ Redis 중심 단순화 버스 트래커 종료 완료")
}

// runSimplifiedManagementWorker 단순화된 관리 워커
func runSimplifiedManagementWorker(ctx context.Context, cfg *config.Config, logger *utils.Logger,
	redisBusManager *redis.RedisBusDataManager, redisCache *cache.RedisStationCacheServiceV2) {

	logger.Info("📅 단순화된 관리 워커 시작")

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	lastCheckDate := ""

	for {
		select {
		case <-ctx.Done():
			logger.Info("📅 단순화된 관리 워커 종료")
			return
		case <-ticker.C:
			now := time.Now()
			currentDate := getDailyOperatingDate(now, cfg)

			if lastCheckDate != "" && lastCheckDate != currentDate {
				logger.Infof("📅 운영일자 변경 감지: %s -> %s", lastCheckDate, currentDate)

				// 새로운 운영일 시작 시 Redis 정리
				cleanedCount, err := redisBusManager.CleanupInactiveBuses(24 * time.Hour)
				if err != nil {
					logger.Errorf("일일 Redis 정리 실패: %v", err)
				} else if cleanedCount > 0 {
					logger.Infof("🧹 일일 Redis 정리 완료 - 제거된 버스: %d대", cleanedCount)
				}
			}

			lastCheckDate = currentDate

			// 30분마다 상태 요약 (단순화)
			if now.Minute()%30 == 0 && now.Second() < 5 {
				operatingStatus := "운영시간 외"
				if cfg.IsOperatingTime(now) {
					operatingStatus = "운영시간 내"
				}

				logger.Infof("📊 상태 요약 [%s] - 운영일자: %s", operatingStatus, currentDate)

				// 정류소 캐시 상태
				routes, stations := redisCache.GetCacheStatistics()
				logger.Infof("📦 정류소 캐시 현황 - %d노선/%d정류소", routes, stations)
			}
		}
	}
}

// printSimplifiedStatus 단순화된 상태 출력
func printSimplifiedStatus(logger *utils.Logger, dataManager *services.SimplifiedUnifiedDataManager,
	redisCache *cache.RedisStationCacheServiceV2, orchestrator *services.MultiAPIOrchestrator) {

	// 정류소 캐시 상태
	routes, stations := redisCache.GetCacheStatistics()
	logger.Infof("📦 정류소 캐시 현황 - %d노선/%d정류소", routes, stations)

	// 오케스트레이터 기본 상태
	orchStats := orchestrator.GetDetailedStatistics()
	logger.Infof("🎯 오케스트레이터 상태 - 실행중: %v, API1: %v, API2: %v",
		orchStats["is_running"], orchStats["api1_enabled"], orchStats["api2_enabled"])
}

// connectToElasticsearchWithRetry Elasticsearch 연결 재시도
func connectToElasticsearchWithRetry(esService *storage.ElasticsearchService, logger *utils.Logger, maxRetries int) error {
	for i := 0; i < maxRetries; i++ {
		if err := esService.TestConnection(); err != nil {
			logger.Warnf("Elasticsearch 연결 실패 (시도 %d/%d): %v", i+1, maxRetries, err)
			if i < maxRetries-1 {
				time.Sleep(time.Duration(i+1) * 2 * time.Second)
			}
		} else {
			logger.Info("✅ Elasticsearch 연결 성공")
			return nil
		}
	}
	return fmt.Errorf("최대 재시도 횟수 초과")
}

// getDailyOperatingDate 운영일자 계산
func getDailyOperatingDate(now time.Time, cfg *config.Config) string {
	if !cfg.IsOperatingTime(now) {
		nextOperatingTime := cfg.GetNextOperatingTime(now)
		if nextOperatingTime.Day() != now.Day() {
			return now.AddDate(0, 0, -1).Format("2006-01-02")
		}
	}
	return now.Format("2006-01-02")
}
