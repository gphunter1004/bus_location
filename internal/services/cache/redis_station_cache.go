// internal/services/cache/redis_station_cache.go - L1 제거된 수정 버전
package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"bus-tracker/config"
	"bus-tracker/internal/models"
	"bus-tracker/internal/utils"
)

// RedisStationCacheServiceV2 Redis 전용 정류소 캐시 서비스 (L1 제거)
type RedisStationCacheServiceV2 struct {
	config         *config.Config
	logger         *utils.Logger
	redisClient    *redis.Client
	ctx            context.Context
	keyPrefix      string
	cacheTTL       time.Duration
	routeConverter *RouteIDConverter
	apiService     *StationCacheServiceV2 // API 호출용
}

// NewRedisStationCacheServiceV2 Redis 전용 정류소 캐시 서비스 생성
func NewRedisStationCacheServiceV2(cfg *config.Config, logger *utils.Logger, apiType string) *RedisStationCacheServiceV2 {
	// API 호출용 서비스 생성 (Redis 없이)
	apiService := NewStationCacheServiceV2(cfg, logger, apiType)

	// Redis 클라이언트 생성
	redisClient := redis.NewClient(&redis.Options{
		Addr:            cfg.Redis.Addr,
		Password:        cfg.Redis.Password,
		DB:              cfg.Redis.DB,
		MaxRetries:      cfg.Redis.MaxRetries,
		PoolSize:        cfg.Redis.PoolSize,
		ConnMaxIdleTime: time.Duration(cfg.Redis.IdleTimeout) * time.Second,
		ReadTimeout:     10 * time.Second,
		WriteTimeout:    10 * time.Second,
	})

	ctx := context.Background()

	// Redis 연결 테스트
	if err := redisClient.Ping(ctx).Err(); err != nil {
		logger.Errorf("Redis 연결 실패, API 서비스만 사용: %v", err)
		return &RedisStationCacheServiceV2{
			config:         cfg,
			logger:         logger,
			redisClient:    nil, // Redis 비활성화
			ctx:            ctx,
			keyPrefix:      "bus:station:v2:",
			cacheTTL:       24 * time.Hour,
			routeConverter: NewRouteIDConverter(),
			apiService:     apiService,
		}
	}

	logger.Info("✅ Redis 전용 정류소 캐시 시스템 V2 초기화 완료")

	return &RedisStationCacheServiceV2{
		config:         cfg,
		logger:         logger,
		redisClient:    redisClient,
		ctx:            ctx,
		keyPrefix:      "bus:station:v2:",
		cacheTTL:       24 * time.Hour,
		routeConverter: NewRouteIDConverter(),
		apiService:     apiService,
	}
}

// GetStationByRouteAndOrder Redis에서 노선ID와 정류장 순서로 조회
func (rscs *RedisStationCacheServiceV2) GetStationByRouteAndOrder(routeID string, stationOrder int) (models.StationCache, bool) {
	if rscs.redisClient == nil {
		// Redis 없으면 API 서비스 사용
		return rscs.apiService.GetStationByRouteAndOrder(routeID, stationOrder)
	}

	unifiedKey := rscs.routeConverter.ToUnifiedKey(routeID)
	redisKey := rscs.keyPrefix + unifiedKey

	// Redis에서 조회
	data, err := rscs.redisClient.Get(rscs.ctx, redisKey).Result()
	if err == redis.Nil {
		rscs.logger.Debugf("Redis 캐시 미스: 노선=%s", routeID)
		// 캐시 미스 시 API에서 로드 후 Redis에 저장
		if rscs.loadRouteFromAPIToRedis(routeID) {
			// 재시도
			return rscs.GetStationByRouteAndOrder(routeID, stationOrder)
		}
		return models.StationCache{}, false
	} else if err != nil {
		rscs.logger.Errorf("Redis 조회 실패 (노선: %s): %v", routeID, err)
		return models.StationCache{}, false
	}

	// JSON 언마샬링
	var stationMap map[int]models.StationCache
	if err := json.Unmarshal([]byte(data), &stationMap); err != nil {
		rscs.logger.Errorf("Redis 데이터 파싱 실패 (노선: %s): %v", routeID, err)
		return models.StationCache{}, false
	}

	if station, found := stationMap[stationOrder]; found {
		rscs.logger.Debugf("Redis 캐시 히트: 노선=%s, 순서=%d -> %s", routeID, stationOrder, station.NodeNm)
		return station, true
	}

	return models.StationCache{}, false
}

// LoadStationCache Redis 전용 정류소 정보 로드
func (rscs *RedisStationCacheServiceV2) LoadStationCache(routeIDs []string) error {
	if len(routeIDs) == 0 {
		return fmt.Errorf("routeIDs가 비어있습니다")
	}

	successCount := 0
	for _, routeID := range routeIDs {
		if rscs.loadRouteFromAPIToRedis(routeID) {
			successCount++
		}
	}

	if successCount == 0 {
		return fmt.Errorf("모든 노선의 정류소 정보 로드가 실패했습니다")
	}

	rscs.logger.Infof("Redis 정류소 캐시 로드 완료: %d/%d 노선 성공", successCount, len(routeIDs))
	return nil
}

// loadRouteFromAPIToRedis API에서 정류소 정보를 가져와 Redis에 저장
func (rscs *RedisStationCacheServiceV2) loadRouteFromAPIToRedis(routeID string) bool {
	// API 서비스로 정류소 정보 로드
	if err := rscs.apiService.LoadStationCache([]string{routeID}); err != nil {
		rscs.logger.Errorf("API 정류소 로드 실패 (노선: %s): %v", routeID, err)
		return false
	}

	// API 서비스에서 정류소 정보 가져오기
	stations, found := rscs.apiService.GetRouteStations(routeID)
	if !found || len(stations) == 0 {
		rscs.logger.Warnf("API에서 정류소 정보 없음 (노선: %s)", routeID)
		return false
	}

	// Redis에 저장할 map 생성
	stationMap := make(map[int]models.StationCache)
	for _, station := range stations {
		if station.NodeOrd > 0 {
			stationMap[station.NodeOrd] = station
		}
	}

	if len(stationMap) == 0 {
		rscs.logger.Warnf("변환된 정류소 맵이 비어있음 (노선: %s)", routeID)
		return false
	}

	// Redis에 저장 (Redis가 활성화된 경우만)
	if rscs.redisClient != nil {
		unifiedKey := rscs.routeConverter.ToUnifiedKey(routeID)
		redisKey := rscs.keyPrefix + unifiedKey

		data, err := json.Marshal(stationMap)
		if err != nil {
			rscs.logger.Errorf("정류소 데이터 마샬링 실패 (노선: %s): %v", routeID, err)
			return false
		}

		if err := rscs.redisClient.Set(rscs.ctx, redisKey, data, rscs.cacheTTL).Err(); err != nil {
			rscs.logger.Errorf("Redis 저장 실패 (노선: %s): %v", routeID, err)
			return false
		}

		rscs.logger.Infof("✅ Redis 정류소 저장 완료 - 노선: %s, 정류소: %d개", routeID, len(stationMap))
	}

	return true
}

// GetRouteStationCount 노선의 전체 정류소 개수 반환
func (rscs *RedisStationCacheServiceV2) GetRouteStationCount(routeID string) int {
	if rscs.redisClient == nil {
		return rscs.apiService.GetRouteStationCount(routeID)
	}

	unifiedKey := rscs.routeConverter.ToUnifiedKey(routeID)
	redisKey := rscs.keyPrefix + unifiedKey

	data, err := rscs.redisClient.Get(rscs.ctx, redisKey).Result()
	if err != nil {
		// 캐시 미스 시 API에서 로드 후 재시도
		if rscs.loadRouteFromAPIToRedis(routeID) {
			return rscs.GetRouteStationCount(routeID)
		}
		return 0
	}

	var stationMap map[int]models.StationCache
	if err := json.Unmarshal([]byte(data), &stationMap); err != nil {
		return 0
	}

	return len(stationMap)
}

// EnrichBusLocationWithStationInfo 버스 위치 정보에 정류소 정보 보강
func (rscs *RedisStationCacheServiceV2) EnrichBusLocationWithStationInfo(busLocation *models.BusLocation, routeID string) {
	var stationOrder int
	if busLocation.NodeOrd > 0 {
		stationOrder = busLocation.NodeOrd
	} else if busLocation.StationSeq > 0 {
		stationOrder = busLocation.StationSeq
	} else {
		busLocation.TotalStations = rscs.GetRouteStationCount(routeID)
		return
	}

	if station, found := rscs.GetStationByRouteAndOrder(routeID, stationOrder); found {
		if busLocation.NodeNm == "" {
			busLocation.NodeNm = station.NodeNm
		}
		if busLocation.NodeId == "" {
			busLocation.NodeId = station.NodeId
		}
		if busLocation.NodeNo == 0 {
			busLocation.NodeNo = station.NodeNo
		}
		if busLocation.GpsLati == 0 && station.GPSLat != 0 {
			busLocation.GpsLati = station.GPSLat
		}
		if busLocation.GpsLong == 0 && station.GPSLong != 0 {
			busLocation.GpsLong = station.GPSLong
		}
		if busLocation.StationId == 0 && station.StationId > 0 {
			busLocation.StationId = station.StationId
		}

		busLocation.TotalStations = rscs.GetRouteStationCount(routeID)
	} else {
		busLocation.TotalStations = rscs.GetRouteStationCount(routeID)
	}
}

// GetCacheStatistics 캐시 통계 반환
func (rscs *RedisStationCacheServiceV2) GetCacheStatistics() (int, int) {
	if rscs.redisClient == nil {
		return rscs.apiService.GetCacheStatistics()
	}

	pattern := rscs.keyPrefix + "*"
	keys, err := rscs.redisClient.Keys(rscs.ctx, pattern).Result()
	if err != nil {
		return 0, 0
	}

	routeCount := len(keys)
	stationCount := 0

	for _, key := range keys {
		data, err := rscs.redisClient.Get(rscs.ctx, key).Result()
		if err != nil {
			continue
		}

		var stationMap map[int]models.StationCache
		if err := json.Unmarshal([]byte(data), &stationMap); err != nil {
			continue
		}

		stationCount += len(stationMap)
	}

	return routeCount, stationCount
}

// Close Redis 연결 종료
func (rscs *RedisStationCacheServiceV2) Close() error {
	if rscs.redisClient != nil {
		return rscs.redisClient.Close()
	}
	return nil
}

// PrintCacheStatus 캐시 상태 출력
func (rscs *RedisStationCacheServiceV2) PrintCacheStatus() {
	routes, stations := rscs.GetCacheStatistics()
	rscs.logger.Infof("📦 Redis 정류소 캐시 현황 - %d노선/%d정류소", routes, stations)

	if rscs.redisClient == nil {
		rscs.logger.Warn("⚠️ Redis 비활성화 - API 서비스만 사용 중")
	} else {
		if err := rscs.redisClient.Ping(rscs.ctx).Err(); err != nil {
			rscs.logger.Errorf("❌ Redis 연결 상태 이상: %v", err)
		} else {
			rscs.logger.Debug("✅ Redis 연결 정상")
		}
	}
}

// ClearCache 캐시 비우기 (Redis 전용)
func (rscs *RedisStationCacheServiceV2) ClearCache() error {
	if rscs.redisClient == nil {
		rscs.logger.Warn("Redis 연결 없음 - 캐시 초기화 건너뛰기")
		return nil
	}

	// Redis 캐시 비우기
	pattern := rscs.keyPrefix + "*"
	keys, err := rscs.redisClient.Keys(rscs.ctx, pattern).Result()
	if err != nil {
		return fmt.Errorf("Redis 키 조회 실패: %v", err)
	}

	if len(keys) > 0 {
		if err := rscs.redisClient.Del(rscs.ctx, keys...).Err(); err != nil {
			return fmt.Errorf("Redis 캐시 삭제 실패: %v", err)
		}
		rscs.logger.Infof("🗑️ Redis 캐시 삭제 완료: %d개 키", len(keys))
	}

	rscs.logger.Info("🧹 Redis 정류소 캐시 초기화 완료")
	return nil
}

// RefreshRouteCache 특정 노선의 캐시 갱신
func (rscs *RedisStationCacheServiceV2) RefreshRouteCache(routeID string) error {
	if rscs.redisClient == nil {
		rscs.logger.Warnf("Redis 연결 없음 - 노선 캐시 갱신 건너뛰기: %s", routeID)
		return nil
	}

	// Redis에서 해당 노선 제거
	unifiedKey := rscs.routeConverter.ToUnifiedKey(routeID)
	redisKey := rscs.keyPrefix + unifiedKey

	rscs.redisClient.Del(rscs.ctx, redisKey)

	// 새로 로드
	if rscs.loadRouteFromAPIToRedis(routeID) {
		rscs.logger.Infof("✅ 노선 캐시 갱신 완료: %s", routeID)
		return nil
	} else {
		return fmt.Errorf("노선 캐시 갱신 실패: %s", routeID)
	}
}
