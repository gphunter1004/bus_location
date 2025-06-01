// internal/services/cache/redis_station_cache_v2.go - 인터페이스 호환성 수정
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

// RedisStationCacheServiceV2 개선된 Redis + L1 2단계 정류소 캐시 서비스
// 구조: RouteID -> StationOrder -> StationData
type RedisStationCacheServiceV2 struct {
	*StationCacheServiceV2 // L1 캐시 (개선된 구조) 임베드
	redisClient            *redis.Client
	ctx                    context.Context
	keyPrefix              string
	cacheTTL               time.Duration
}

// NewRedisStationCacheServiceV2 새로운 개선된 Redis + L1 2단계 캐시 서비스 생성
func NewRedisStationCacheServiceV2(cfg *config.Config, logger *utils.Logger, apiType string) *RedisStationCacheServiceV2 {
	// L1 캐시 (개선된 구조) 먼저 생성
	l1Cache := NewStationCacheServiceV2(cfg, logger, apiType)

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
		logger.Errorf("Redis 연결 실패, L1 캐시만 사용: %v", err)
		return &RedisStationCacheServiceV2{
			StationCacheServiceV2: l1Cache,
			redisClient:           nil, // Redis 비활성화
			ctx:                   ctx,
			keyPrefix:             "bus:station:v2:",
			cacheTTL:              24 * time.Hour,
		}
	}

	logger.Info("✅ Redis + L1 2단계 캐시 시스템 V2 초기화 완료")

	return &RedisStationCacheServiceV2{
		StationCacheServiceV2: l1Cache,
		redisClient:           redisClient,
		ctx:                   ctx,
		keyPrefix:             "bus:station:v2:",
		cacheTTL:              24 * time.Hour,
	}
}

// GetStationByRouteAndOrder 2단계 캐시에서 노선ID와 정류장 순서로 조회
func (rscs *RedisStationCacheServiceV2) GetStationByRouteAndOrder(routeID string, stationOrder int) (models.StationCache, bool) {
	// 1. L1 캐시에서 먼저 조회
	station, found := rscs.StationCacheServiceV2.GetStationByRouteAndOrder(routeID, stationOrder)
	if found {
		rscs.logger.Debugf("L1 캐시 히트: 노선=%s, 순서=%d", routeID, stationOrder)
		return station, true
	}

	// 2. L1에 없으면 Redis에서 로드 시도
	if rscs.redisClient != nil {
		if rscs.loadRouteFromRedis(routeID) {
			// Redis에서 로드 후 다시 L1에서 조회
			station, found := rscs.StationCacheServiceV2.GetStationByRouteAndOrder(routeID, stationOrder)
			if found {
				rscs.logger.Debugf("Redis 캐시 히트: 노선=%s, 순서=%d", routeID, stationOrder)
				return station, true
			}
		}
	}

	rscs.logger.Debugf("캐시 미스: 노선=%s, 순서=%d", routeID, stationOrder)
	return models.StationCache{}, false
}

// LoadStationCache 2단계 캐시를 사용한 정류소 정보 로드
func (rscs *RedisStationCacheServiceV2) LoadStationCache(routeIDs []string) error {
	// 1. L1 캐시에서 먼저 확인
	rscs.mutex.RLock()
	missingRoutes := []string{}
	for _, routeID := range routeIDs {
		unifiedKey := rscs.routeConverter.ToUnifiedKey(routeID)
		if _, exists := rscs.stationCache[unifiedKey]; !exists {
			missingRoutes = append(missingRoutes, routeID)
		}
	}
	rscs.mutex.RUnlock()

	if len(missingRoutes) == 0 {
		rscs.logger.Info("🚀 모든 정류소 정보가 L1 캐시에 있음")
		return nil
	}

	// 2. Redis에서 확인 (Redis가 활성화된 경우)
	if rscs.redisClient != nil {
		redisHits := 0
		for _, routeID := range missingRoutes {
			if rscs.loadRouteFromRedis(routeID) {
				redisHits++
			}
		}
		if redisHits > 0 {
			rscs.logger.Infof("📦 Redis에서 %d개 노선 정보 로드", redisHits)
		}
	}

	// 3. 여전히 없는 데이터는 API에서 로드
	rscs.mutex.RLock()
	finalMissingRoutes := []string{}
	for _, routeID := range missingRoutes {
		unifiedKey := rscs.routeConverter.ToUnifiedKey(routeID)
		if _, exists := rscs.stationCache[unifiedKey]; !exists {
			finalMissingRoutes = append(finalMissingRoutes, routeID)
		}
	}
	rscs.mutex.RUnlock()

	if len(finalMissingRoutes) > 0 {
		rscs.logger.Infof("🌐 API에서 %d개 노선 정보 로드 중...", len(finalMissingRoutes))

		// 기존 L1 캐시의 LoadStationCache 호출
		if err := rscs.StationCacheServiceV2.LoadStationCache(finalMissingRoutes); err != nil {
			return err
		}

		// 새로 로드된 데이터를 Redis에 저장
		if rscs.redisClient != nil {
			go rscs.saveNewDataToRedis(finalMissingRoutes)
		}
	}

	return nil
}

// loadRouteFromRedis Redis에서 특정 노선 정보 로드
func (rscs *RedisStationCacheServiceV2) loadRouteFromRedis(routeID string) bool {
	if rscs.redisClient == nil {
		return false
	}

	unifiedKey := rscs.routeConverter.ToUnifiedKey(routeID)
	redisKey := rscs.keyPrefix + unifiedKey

	// Redis에서 데이터 조회
	data, err := rscs.redisClient.Get(rscs.ctx, redisKey).Result()
	if err == redis.Nil {
		return false // 키가 없음
	} else if err != nil {
		rscs.logger.Errorf("Redis 조회 실패 (노선: %s): %v", routeID, err)
		return false
	}

	// JSON 언마샬링: map[int]models.StationCache (StationOrder -> StationData)
	var stationMap map[int]models.StationCache
	if err := json.Unmarshal([]byte(data), &stationMap); err != nil {
		rscs.logger.Errorf("Redis 데이터 파싱 실패 (노선: %s): %v", routeID, err)
		return false
	}

	// L1 캐시에 저장
	rscs.mutex.Lock()
	if rscs.stationCache[unifiedKey] == nil {
		rscs.stationCache[unifiedKey] = make(map[int]models.StationCache)
	}
	for stationOrder, station := range stationMap {
		rscs.stationCache[unifiedKey][stationOrder] = station
	}
	rscs.mutex.Unlock()

	rscs.logger.Debugf("Redis에서 노선 로드: %s (%d개 정류소)", routeID, len(stationMap))
	return true
}

// saveNewDataToRedis 새로 로드된 데이터를 Redis에 저장 (비동기)
func (rscs *RedisStationCacheServiceV2) saveNewDataToRedis(routeIDs []string) {
	if rscs.redisClient == nil {
		return
	}

	rscs.mutex.RLock()
	defer rscs.mutex.RUnlock()

	for _, routeID := range routeIDs {
		unifiedKey := rscs.routeConverter.ToUnifiedKey(routeID)
		stationMap, exists := rscs.stationCache[unifiedKey]
		if !exists || len(stationMap) == 0 {
			continue
		}

		// JSON 마샬링: map[int]models.StationCache
		data, err := json.Marshal(stationMap)
		if err != nil {
			rscs.logger.Errorf("Redis 저장용 데이터 마샬링 실패 (노선: %s): %v", routeID, err)
			continue
		}

		// Redis에 저장
		redisKey := rscs.keyPrefix + unifiedKey
		if err := rscs.redisClient.Set(rscs.ctx, redisKey, data, rscs.cacheTTL).Err(); err != nil {
			rscs.logger.Errorf("Redis 저장 실패 (노선: %s): %v", routeID, err)
		} else {
			rscs.logger.Debugf("✅ Redis 저장 완료 - 노선: %s, 정류소: %d개", routeID, len(stationMap))
		}
	}
}

// ClearCache 캐시 비우기 (L1 + Redis)
func (rscs *RedisStationCacheServiceV2) ClearCache() error {
	// L1 캐시 비우기
	rscs.mutex.Lock()
	rscs.stationCache = make(map[string]map[int]models.StationCache)
	rscs.mutex.Unlock()

	// Redis 캐시 비우기
	if rscs.redisClient != nil {
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
	}

	rscs.logger.Info("🧹 L1 + Redis 캐시 V2 초기화 완료")
	return nil
}

// RefreshRouteCache 특정 노선의 캐시 갱신
func (rscs *RedisStationCacheServiceV2) RefreshRouteCache(routeID string) error {
	unifiedKey := rscs.routeConverter.ToUnifiedKey(routeID)

	// L1에서 해당 노선 제거
	rscs.mutex.Lock()
	delete(rscs.stationCache, unifiedKey)
	rscs.mutex.Unlock()

	// Redis에서도 제거
	if rscs.redisClient != nil {
		redisKey := rscs.keyPrefix + unifiedKey
		rscs.redisClient.Del(rscs.ctx, redisKey)
	}

	// 새로 로드
	return rscs.LoadStationCache([]string{routeID})
}

// Close Redis 연결 종료
func (rscs *RedisStationCacheServiceV2) Close() error {
	if rscs.redisClient != nil {
		return rscs.redisClient.Close()
	}
	return nil
}

// EnrichBusLocationWithStationInfo 개선된 버스 위치 정보 보강
func (rscs *RedisStationCacheServiceV2) EnrichBusLocationWithStationInfo(busLocation *models.BusLocation, routeID string) {
	// 우선순위: NodeOrd -> StationSeq
	var stationOrder int
	if busLocation.NodeOrd > 0 {
		stationOrder = busLocation.NodeOrd
	} else if busLocation.StationSeq > 0 {
		stationOrder = busLocation.StationSeq
	} else {
		// 순서 정보가 없으면 전체 정류소 개수만 설정
		busLocation.TotalStations = rscs.GetRouteStationCount(routeID)
		return
	}

	// 2단계 캐시에서 정류소 정보 조회
	if station, found := rscs.GetStationByRouteAndOrder(routeID, stationOrder); found {
		// 정류소 정보 보강
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

		// StationId 보강
		if busLocation.StationId == 0 && station.StationId > 0 {
			busLocation.StationId = station.StationId
		}

		busLocation.TotalStations = rscs.GetRouteStationCount(routeID)
	} else {
		// 캐시 미스 - 전체 정류소 개수만 설정
		busLocation.TotalStations = rscs.GetRouteStationCount(routeID)
	}
}

// GetRouteStations 특정 노선의 모든 정류소 조회 (2단계 캐시 지원)
func (rscs *RedisStationCacheServiceV2) GetRouteStations(routeID string) ([]models.StationCache, bool) {
	// L1에서 먼저 조회
	stations, found := rscs.StationCacheServiceV2.GetRouteStations(routeID)
	if found {
		return stations, true
	}

	// L1에 없으면 Redis에서 로드 시도
	if rscs.redisClient != nil && rscs.loadRouteFromRedis(routeID) {
		return rscs.StationCacheServiceV2.GetRouteStations(routeID)
	}

	return nil, false
}
