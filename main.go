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
	"bus-tracker/internal/services/tracker"
	"bus-tracker/internal/utils"
)

func main() {
	cfg := config.LoadConfig()
	logger := utils.NewLogger()

	logger.Info("🚌 버스 트래커 시작 (Redis 중심 데이터 플로우)")
	cfg.PrintConfig()

	runUnifiedModeWithRedis(cfg, logger)
}

func runUnifiedModeWithRedis(cfg *config.Config, logger *utils.Logger) {
	logger.Info("통합 모드 (Redis 중심) 시작")

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

	// Redis 버스 데이터 매니저 생성
	logger.Info("📦 Redis 버스 데이터 매니저 초기화 중...")
	redisBusManager := redis.NewRedisBusDataManager(cfg, logger)
	defer redisBusManager.Close()

	// 중복 체크 서비스 생성
	duplicateChecker := storage.NewElasticsearchDuplicateChecker(esService, logger, cfg.IndexName)

	// 현재 운영일자 확인
	currentOperatingDate := time.Now().Format("2006-01-02")
	logger.Infof("현재 운영일자: %s", currentOperatingDate)

	// Redis + L1 2단계 캐시 V2 생성 (정류소 캐시)
	logger.Info("📦 Redis + L1 2단계 정류소 캐시 시스템 V2 초기화 중...")
	logger.Info("   🔧 캐시 구조: RouteID -> StationOrder -> StationData")

	// V2 캐시 생성 - StationCacheInterface로 사용
	var stationCache cache.StationCacheInterface = cache.NewRedisStationCacheServiceV2(cfg, logger, "unified")

	// Redis 연결 종료를 위해 타입 캐스팅 필요
	redisV2Cache := stationCache.(*cache.RedisStationCacheServiceV2)
	defer redisV2Cache.Close() // 프로그램 종료 시 Redis 연결 닫기

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

	// 일일 캐시 새로고침 관리자 V2 생성
	cacheRefreshManager := cache.NewDailyCacheRefreshManager(cfg, logger, redisV2Cache, allRouteIDs)

	// 정류소 캐시 로드 (V2 2단계 캐시로 빠른 로딩)
	if len(allRouteIDs) > 0 {
		go func() {
			logger.Info("📦 V2 2단계 정류소 캐시 로딩 시작...")
			loadStart := time.Now()

			if err := stationCache.LoadStationCache(allRouteIDs); err != nil {
				logger.Errorf("정류소 캐시 V2 로드 실패: %v", err)
			} else {
				loadDuration := time.Since(loadStart)
				redisV2Cache.PrintCacheStatus()
				logger.Infof("✅ 정류소 캐시 V2 로딩 완료 (소요시간: %v)", loadDuration)
			}
		}()
	}

	var api1Client *api.API1Client
	var api2Client *api.API2Client

	// 인터페이스를 통해 API 클라이언트에 캐시 전달
	if len(cfg.API1Config.RouteIDs) > 0 {
		api1Client = api.NewAPI1ClientWithSharedCache(cfg, logger, stationCache)
		logger.Infof("API1 클라이언트 생성 완료 (Redis V2 캐시 사용)")
	}

	if len(cfg.API2Config.RouteIDs) > 0 {
		api2Client = api.NewAPI2ClientWithSharedCache(cfg, logger, stationCache)
		logger.Infof("API2 클라이언트 생성 완료 (Redis V2 캐시 사용)")
	}

	// Redis 기반 통합 데이터 매니저 생성 (단순화)
	dataManager := services.NewUnifiedDataManagerWithRedis(
		logger, stationCache, esService, redisBusManager, duplicateChecker, cfg.IndexName)

	orchestrator := services.NewMultiAPIOrchestrator(cfg, logger, api1Client, api2Client, dataManager)

	// 컨텍스트와 종료 관리
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	// 신호 처리 설정
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 일일 운영시간 관리 워커 시작 (단순화)
	wg.Add(1)
	go func() {
		defer wg.Done()
		runDailyOperatingScheduleWorkerWithRedis(ctx, cfg, logger, redisV2Cache, cacheRefreshManager, redisBusManager)
	}()

	// 일일 캐시 새로고침 워커 V2 시작
	wg.Add(1)
	go func() {
		defer wg.Done()
		cacheRefreshManager.Start()
		<-ctx.Done()
		cacheRefreshManager.Stop()
	}()

	// 오케스트레이터 시작
	logger.Info("오케스트레이터 시작 중...")
	if err := orchestrator.Start(); err != nil {
		log.Fatalf("오케스트레이터 시작 실패: %v", err)
	}
	logger.Info("✅ 오케스트레이터 시작 완료")

	// 시작 완료 메시지
	logger.Info("🚌 Redis 기반 통합 버스 트래커 실행 중")
	logger.Infof("📅 운영일자: %s", currentOperatingDate)
	logger.Info("🔄 Redis 중심 데이터 플로우: API → Redis → ES")
	logger.Info("🔧 정류소 캐시: RouteID -> StationOrder -> StationData")
	logger.Info("⏹️  종료하려면 Ctrl+C를 누르세요")

	// 정기적인 상태 출력
	go func() {
		ticker := time.NewTicker(10 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// 정류소 캐시 상태
				redisV2Cache.PrintCacheStatus()

				// Redis 버스 데이터 상태
				if stats, err := dataManager.GetRedisStatistics(); err == nil {
					logger.Infof("📊 Redis 버스 데이터 현황 - 총: %v대, 활성: %v대, 마지막 업데이트: %v",
						stats["total_buses"], stats["active_buses"], stats["last_update"])
				}

				// 새로고침 상태
				refreshStatus := cacheRefreshManager.GetRefreshStatus()
				if lastRefresh, ok := refreshStatus["lastRefreshDate"].(string); ok && lastRefresh != "" {
					logger.Infof("🔄 캐시 새로고침 상태 V2 - 마지막: %s, 진행중: %v",
						lastRefresh, refreshStatus["isRefreshing"])
				}

				// 오케스트레이터 통계
				orchStats := orchestrator.GetDetailedStatistics()
				logger.Infof("🎯 오케스트레이터 상태 - 실행중: %v, 총버스: %v대",
					orchStats["is_running"], orchStats["total_buses"])
			}
		}
	}()

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

	// 4. Redis 연결 종료
	if err := redisV2Cache.Close(); err != nil {
		logger.Errorf("Redis V2 정류소 캐시 연결 종료 실패: %v", err)
	} else {
		logger.Info("📦 Redis V2 정류소 캐시 연결 정상 종료")
	}

	if err := redisBusManager.Close(); err != nil {
		logger.Errorf("Redis 버스 데이터 매니저 연결 종료 실패: %v", err)
	} else {
		logger.Info("📦 Redis 버스 데이터 매니저 연결 정상 종료")
	}

	logger.Info("✅ Redis 기반 통합 버스 트래커 종료 완료")
}

// runDailyOperatingScheduleWorkerWithRedis Redis 기반 일일 운영시간 관리
func runDailyOperatingScheduleWorkerWithRedis(ctx context.Context, cfg *config.Config, logger *utils.Logger,
	busTracker *tracker.BusTrackerWithDuplicateCheck, redisCache *cache.RedisStationCacheServiceV2,
	refreshManager *cache.DailyCacheRefreshManager, redisBusManager *redis.RedisBusDataManager) {
	logger.Info("📅 Redis 기반 일일 운영시간 관리 워커 시작")

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	lastCheckDate := ""

	for {
		select {
		case <-ctx.Done():
			logger.Info("📅 Redis 기반 일일 운영시간 관리 워커 종료")
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

			// 30분마다 상태 요약
			if now.Minute()%30 == 0 && now.Second() < 5 {
				operatingStatus := "운영시간 외"
				if cfg.IsOperatingTime(now) {
					operatingStatus = "운영시간 내"
				}

				trackedBuses := busTracker.GetTrackedBusCount()
				dailyStats := busTracker.GetDailyTripStatistics()

				logger.Infof("📊 상태 요약 (Redis 기반) [%s] - 운영일자: %s, 추적버스: %d대, 일일차수기록: %d대",
					operatingStatus, currentDate, trackedBuses, len(dailyStats))

				// Redis 정류소 캐시 상태
				redisCache.PrintCacheStatus()

				// Redis 버스 데이터 상태
				if busStats, err := redisBusManager.GetBusStatistics(); err == nil {
					logger.Infof("📦 Redis 버스 데이터 - 총: %v대, 활성: %v대",
						busStats["total_buses"], busStats["active_buses"])
				}

				// 새로고침 상태
				refreshStatus := refreshManager.GetRefreshStatus()
				logger.Infof("🔄 캐시 새로고침 V2 - 마지막: %s, 다음예상: %s",
					refreshStatus["lastRefreshDate"], refreshStatus["nextRefreshTime"])

				// 캐시 히트율 정보
				if cacheStats, ok := refreshStatus["cacheStats"].(map[string]interface{}); ok {
					logger.Infof("📈 캐시 통계 V2 - 구조: %s, Redis: %t",
						cacheStats["cache_type"], cacheStats["redis_enabled"])
				}
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