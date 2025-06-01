// internal/services/cache/implementations.go - V1과 V2 인터페이스 구현 확인
package cache

import "bus-tracker/internal/models"

// V2 캐시를 위한 GetStationInfo 구현 (V2 -> V1 호환)
func (scs *StationCacheServiceV2) GetStationInfo(routeNmOrId string, nodeOrdOrSeq int) (models.StationCache, bool) {
	return scs.GetStationByRouteAndOrder(routeNmOrId, nodeOrdOrSeq)
}

// V2 Redis 캐시를 위한 GetStationInfo 구현 (V2 -> V1 호환)
func (rscs *RedisStationCacheServiceV2) GetStationInfo(routeNmOrId string, nodeOrdOrSeq int) (models.StationCache, bool) {
	return rscs.GetStationByRouteAndOrder(routeNmOrId, nodeOrdOrSeq)
}

// V2 Redis 캐시를 위한 GetCacheStatistics 구현 (인터페이스 호환)
func (rscs *RedisStationCacheServiceV2) GetCacheStatistics() (int, int) {
	l1Routes, l1Stations, _ := rscs.GetCacheStatisticsV2()
	return l1Routes, l1Stations
}

// GetCacheStatisticsV2 V2용 상세 통계 (public 메서드)
func (rscs *RedisStationCacheServiceV2) GetCacheStatisticsV2() (l1Routes, l1Stations, redisKeys int) {
	// L1 통계
	l1Routes, l1Stations = rscs.StationCacheServiceV2.GetCacheStatistics()

	// Redis 통계
	if rscs.redisClient != nil {
		pattern := rscs.keyPrefix + "*"
		keys, err := rscs.redisClient.Keys(rscs.ctx, pattern).Result()
		if err == nil {
			redisKeys = len(keys)
		}
	}

	return l1Routes, l1Stations, redisKeys
}

// PrintCacheStatus V2 캐시 상태 출력 (수정된 버전)
func (rscs *RedisStationCacheServiceV2) PrintCacheStatus() {
	l1Routes, l1Stations, redisKeys := rscs.GetCacheStatisticsV2()

	rscs.logger.Infof("📊 캐시 상태 V2 - L1: %d노선/%d정류소, Redis: %d키",
		l1Routes, l1Stations, redisKeys)

	if rscs.redisClient == nil {
		rscs.logger.Warn("⚠️ Redis 비활성화 - L1 캐시만 사용 중")
	} else {
		// Redis 연결 상태 확인
		if err := rscs.redisClient.Ping(rscs.ctx).Err(); err != nil {
			rscs.logger.Errorf("❌ Redis 연결 상태 이상: %v", err)
		} else {
			rscs.logger.Debug("✅ Redis 연결 정상")
		}
	}

	// 상세 캐시 상태 (DEBUG 레벨)
	rscs.StationCacheServiceV2.PrintCacheStatus()
}

// GetCacheHitRate V2 캐시 히트율 계산 (수정된 버전)
func (rscs *RedisStationCacheServiceV2) GetCacheHitRate() map[string]interface{} {
	l1Routes, l1Stations, redisKeys := rscs.GetCacheStatisticsV2()

	return map[string]interface{}{
		"l1_routes":     l1Routes,
		"l1_stations":   l1Stations,
		"redis_keys":    redisKeys,
		"cache_type":    "RouteID -> StationOrder -> StationData",
		"redis_enabled": rscs.redisClient != nil,
	}
}
