// main.go - 최초 데이터 로딩 포함 수정 버전
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