// internal/services/cache/implementations.go - L1 제거된 수정 버전
package cache

import "bus-tracker/internal/models"

// V2 캐시를 위한 GetStationInfo 구현 (인터페이스 호환)
func (scs *StationCacheServiceV2) GetStationInfo(routeNmOrId string, nodeOrdOrSeq int) (models.StationCache, bool) {
	return scs.GetStationByRouteAndOrder(routeNmOrId, nodeOrdOrSeq)
}

// V2 Redis 캐시를 위한 GetStationInfo 구현 (인터페이스 호환)
func (rscs *RedisStationCacheServiceV2) GetStationInfo(routeNmOrId string, nodeOrdOrSeq int) (models.StationCache, bool) {
	return rscs.GetStationByRouteAndOrder(routeNmOrId, nodeOrdOrSeq)
}

// GetCacheStatisticsV2 상세 통계 (Redis 전용)
func (rscs *RedisStationCacheServiceV2) GetCacheStatisticsV2() (routes, stations, redisKeys int) {
	routes, stations = rscs.GetCacheStatistics()

	// Redis 키 개수
	if rscs.redisClient != nil {
		pattern := rscs.keyPrefix + "*"
		keys, err := rscs.redisClient.Keys(rscs.ctx, pattern).Result()
		if err == nil {
			redisKeys = len(keys)
		}
	}

	return routes, stations, redisKeys
}

// GetCacheHitRate Redis 캐시 히트율 정보
func (rscs *RedisStationCacheServiceV2) GetCacheHitRate() map[string]interface{} {
	routes, stations, redisKeys := rscs.GetCacheStatisticsV2()

	return map[string]interface{}{
		"routes":        routes,
		"stations":      stations,
		"redis_keys":    redisKeys,
		"cache_type":    "Redis Only (L1 Removed)",
		"redis_enabled": rscs.redisClient != nil,
	}
}
