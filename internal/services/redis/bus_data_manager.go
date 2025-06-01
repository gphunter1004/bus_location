// internal/services/redis/bus_data_manager.go
package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"bus-tracker/config"
	"bus-tracker/internal/models"
	"bus-tracker/internal/utils"
)

// RedisBusData Redis에 저장할 버스 데이터 (ES 전송 데이터와 동일)
type RedisBusData struct {
	// ES 전송용 데이터와 동일한 구조
	models.BusLocation

	// Redis 전용 메타데이터
	LastRedisUpdate time.Time `json:"lastRedisUpdate"`
	LastESSync      time.Time `json:"lastESSync"`
	ChangeCount     int       `json:"changeCount"`
	DataSources     []string  `json:"dataSources"`
	IsActive        bool      `json:"isActive"`
}

// BusStatus 버스 상태 추적 (별도 키)
type BusStatus struct {
	PlateNo      string    `json:"plateNo"`
	IsActive     bool      `json:"isActive"`
	LastSeenTime time.Time `json:"lastSeenTime"`
	TripNumber   int       `json:"tripNumber"`
	IsTerminated bool      `json:"isTerminated"`
	RouteId      int64     `json:"routeId"`
	RouteNm      string    `json:"routeNm"`
}

// RedisBusDataManager Redis 기반 버스 데이터 매니저
type RedisBusDataManager struct {
	redisClient *redis.Client
	logger      *utils.Logger
	keyPrefix   string
	dataTTL     time.Duration
	statusTTL   time.Duration
	ctx         context.Context
}

// NewRedisBusDataManager Redis 버스 데이터 매니저 생성
func NewRedisBusDataManager(cfg *config.Config, logger *utils.Logger) *RedisBusDataManager {
	// Redis 클라이언트 설정 (정류소 캐시와 다른 DB 사용)
	redisClient := redis.NewClient(&redis.Options{
		Addr:            cfg.Redis.Addr,
		Password:        cfg.Redis.Password,
		DB:              cfg.Redis.DB + 1, // 정류소 캐시와 분리
		MaxRetries:      cfg.Redis.MaxRetries,
		PoolSize:        cfg.Redis.PoolSize,
		ConnMaxIdleTime: time.Duration(cfg.Redis.IdleTimeout) * time.Second,
		ReadTimeout:     10 * time.Second,
		WriteTimeout:    10 * time.Second,
	})

	ctx := context.Background()

	// Redis 연결 테스트
	if err := redisClient.Ping(ctx).Err(); err != nil {
		logger.Errorf("Redis 버스 데이터 매니저 연결 실패, L1 메모리만 사용: %v", err)
		// Redis 실패 시에도 매니저는 생성하되 Redis 없이 동작
		return &RedisBusDataManager{
			redisClient: nil, // Redis 비활성화
			logger:      logger,
			keyPrefix:   "bus:",
			dataTTL:     2 * time.Hour,
			statusTTL:   24 * time.Hour,
			ctx:         ctx,
		}
	}

	logger.Infof("✅ Redis 버스 데이터 매니저 초기화 완료 (DB: %d)", cfg.Redis.DB+1)

	return &RedisBusDataManager{
		redisClient: redisClient,
		logger:      logger,
		keyPrefix:   "bus:",
		dataTTL:     2 * time.Hour,  // 버스 데이터 TTL
		statusTTL:   24 * time.Hour, // 상태 데이터 TTL
		ctx:         ctx,
	}
}

// UpdateBusData 버스 데이터 저장/업데이트
func (rbm *RedisBusDataManager) UpdateBusData(busLocation models.BusLocation, dataSources []string) (*RedisBusData, bool, error) {
	if rbm.redisClient == nil {
		rbm.logger.Warnf("Redis 연결 없음 - 메모리 모드로 동작 중")
		return nil, false, fmt.Errorf("Redis 연결 없음")
	}

	plateNo := busLocation.PlateNo
	locationKey := rbm.keyPrefix + "location:" + plateNo
	statusKey := rbm.keyPrefix + "status:" + plateNo

	now := time.Now()

	rbm.logger.Debugf("Redis 저장 시작 - 차량: %s, 키: %s", plateNo, locationKey)

	// 기존 데이터 조회
	existingData, err := rbm.getBusData(locationKey)
	if err != nil && err != redis.Nil {
		rbm.logger.Errorf("기존 데이터 조회 실패 (차량: %s): %v", plateNo, err)
		return nil, false, fmt.Errorf("기존 데이터 조회 실패: %v", err)
	}

	// 변경 여부 검사
	hasChanged := rbm.hasLocationChanged(existingData, busLocation)

	// 새로운 데이터 생성
	newData := &RedisBusData{
		BusLocation:     busLocation,
		LastRedisUpdate: now,
		DataSources:     rbm.mergeDataSources(existingData, dataSources),
		IsActive:        true,
	}

	if existingData != nil {
		newData.LastESSync = existingData.LastESSync
		newData.ChangeCount = existingData.ChangeCount
		if hasChanged {
			newData.ChangeCount++
		}
	} else {
		newData.ChangeCount = 1
	}

	// 위치 변경이 없는 경우 어떤 데이터가 달라졌는지 로깅
	if !hasChanged && existingData != nil {
		rbm.logDataDifferences(existingData, newData)
	}

	// Pipeline으로 원자적 업데이트
	pipe := rbm.redisClient.Pipeline()

	// 1. 버스 위치 데이터 저장 (위치 변경이 있거나 첫 등록인 경우에만)
	if hasChanged || existingData == nil {
		locationData, err := json.Marshal(newData)
		if err != nil {
			rbm.logger.Errorf("데이터 마샬링 실패 (차량: %s): %v", plateNo, err)
			return nil, false, fmt.Errorf("데이터 마샬링 실패: %v", err)
		}
		pipe.Set(rbm.ctx, locationKey, locationData, rbm.dataTTL)
		rbm.logger.Debugf("Redis 위치 데이터 업데이트 - 차량: %s", plateNo)
	} else {
		// 위치 변경이 없으면 TTL만 연장
		pipe.Expire(rbm.ctx, locationKey, rbm.dataTTL)
		rbm.logger.Debugf("Redis 위치 데이터 TTL 연장만 - 차량: %s", plateNo)
	}

	// 2. 버스 상태는 항상 업데이트 (LastSeenTime 때문에)
	status := &BusStatus{
		PlateNo:      plateNo,
		IsActive:     true,
		LastSeenTime: now,
		TripNumber:   busLocation.TripNumber,
		IsTerminated: false,
		RouteId:      busLocation.RouteId,
		RouteNm:      busLocation.RouteNm,
	}
	statusData, err := json.Marshal(status)
	if err != nil {
		rbm.logger.Errorf("상태 데이터 마샬링 실패 (차량: %s): %v", plateNo, err)
		return nil, false, fmt.Errorf("상태 데이터 마샬링 실패: %v", err)
	}
	pipe.Set(rbm.ctx, statusKey, statusData, rbm.statusTTL)

	// 3. 노선별 활성 버스 목록 업데이트 (첫 등록이거나 노선 변경 시에만)
	shouldUpdateRoute := existingData == nil || existingData.RouteId != busLocation.RouteId
	if shouldUpdateRoute {
		routeKey := fmt.Sprintf("%sroute:%d:active", rbm.keyPrefix, busLocation.RouteId)
		pipe.SAdd(rbm.ctx, routeKey, plateNo)
		pipe.Expire(rbm.ctx, routeKey, rbm.statusTTL)
		rbm.logger.Debugf("Redis 노선 목록 업데이트 - 차량: %s, 노선: %d", plateNo, busLocation.RouteId)
	}

	// 4. 마지막 업데이트 시간 저장 (위치 변경이 있었을 때만)
	if hasChanged {
		pipe.Set(rbm.ctx, rbm.keyPrefix+"lastupdate", now.Unix(), time.Hour)
	}

	// Pipeline 실행
	results, err := pipe.Exec(rbm.ctx)
	if err != nil {
		rbm.logger.Errorf("Redis Pipeline 실행 실패 (차량: %s): %v", plateNo, err)
		return nil, false, fmt.Errorf("Redis 업데이트 실패: %v", err)
	}

	rbm.logger.Debugf("Redis 저장 완료 - 차량: %s, Pipeline 결과: %d개 명령", plateNo, len(results))

	if hasChanged {
		rbm.logger.Infof("Redis 버스 데이터 업데이트 - 차량: %s (변경: %t, 차수: %d, 위치: %s)",
			plateNo, hasChanged, busLocation.TripNumber, busLocation.NodeNm)
	}

	return newData, hasChanged, nil
}

// GetChangedBusesForES 변경된 버스 데이터 조회 (ES 전송용)
func (rbm *RedisBusDataManager) GetChangedBusesForES() ([]models.BusLocation, error) {
	if rbm.redisClient == nil {
		return nil, fmt.Errorf("Redis 연결 없음")
	}

	pattern := rbm.keyPrefix + "location:*"
	keys, err := rbm.redisClient.Keys(rbm.ctx, pattern).Result()
	if err != nil {
		return nil, fmt.Errorf("키 조회 실패: %v", err)
	}

	var changedBuses []models.BusLocation
	now := time.Now()

	for _, key := range keys {
		data, err := rbm.getBusData(key)
		if err != nil {
			continue
		}

		// ES 동기화가 필요한 데이터인지 확인
		if rbm.needsESSync(data, now) {
			changedBuses = append(changedBuses, data.BusLocation)
		}
	}

	return changedBuses, nil
}

// MarkAsSynced ES 동기화 완료 마킹
func (rbm *RedisBusDataManager) MarkAsSynced(plateNos []string) error {
	if rbm.redisClient == nil || len(plateNos) == 0 {
		return nil
	}

	now := time.Now()
	pipe := rbm.redisClient.Pipeline()

	for _, plateNo := range plateNos {
		key := rbm.keyPrefix + "location:" + plateNo

		// 기존 데이터 업데이트
		data, err := rbm.getBusData(key)
		if err != nil {
			continue
		}

		data.LastESSync = now
		updatedData, _ := json.Marshal(data)
		pipe.Set(rbm.ctx, key, updatedData, rbm.dataTTL)
	}

	_, err := pipe.Exec(rbm.ctx)
	if err != nil {
		rbm.logger.Errorf("ES 동기화 마킹 실패: %v", err)
	}

	return err
}

// CleanupInactiveBuses 비활성 버스 정리
func (rbm *RedisBusDataManager) CleanupInactiveBuses(inactiveThreshold time.Duration) (int, error) {
	if rbm.redisClient == nil {
		return 0, nil
	}

	pattern := rbm.keyPrefix + "status:*"
	keys, err := rbm.redisClient.Keys(rbm.ctx, pattern).Result()
	if err != nil {
		return 0, err
	}

	now := time.Now()
	var inactiveKeys []string
	var inactivePlates []string

	for _, statusKey := range keys {
		data, err := rbm.redisClient.Get(rbm.ctx, statusKey).Result()
		if err != nil {
			continue
		}

		var status BusStatus
		if err := json.Unmarshal([]byte(data), &status); err != nil {
			continue
		}

		if now.Sub(status.LastSeenTime) > inactiveThreshold {
			inactiveKeys = append(inactiveKeys, statusKey)

			// 위치 데이터 키도 추가
			plateNo := status.PlateNo
			locationKey := rbm.keyPrefix + "location:" + plateNo
			inactiveKeys = append(inactiveKeys, locationKey)
			inactivePlates = append(inactivePlates, plateNo)

			// 노선별 활성 목록에서도 제거
			routeKey := fmt.Sprintf("%sroute:%d:active", rbm.keyPrefix, status.RouteId)
			rbm.redisClient.SRem(rbm.ctx, routeKey, plateNo)
		}
	}

	if len(inactiveKeys) > 0 {
		rbm.redisClient.Del(rbm.ctx, inactiveKeys...)
		rbm.logger.Infof("Redis 비활성 버스 정리: %d대 (%v)", len(inactivePlates), inactivePlates[:min(5, len(inactivePlates))])
	}

	return len(inactivePlates), nil
}

// GetActiveBusesByRoute 노선별 활성 버스 목록 조회
func (rbm *RedisBusDataManager) GetActiveBusesByRoute(routeId int64) ([]string, error) {
	if rbm.redisClient == nil {
		return nil, fmt.Errorf("Redis 연결 없음")
	}

	routeKey := fmt.Sprintf("%sroute:%d:active", rbm.keyPrefix, routeId)
	members, err := rbm.redisClient.SMembers(rbm.ctx, routeKey).Result()
	if err != nil {
		return nil, err
	}

	return members, nil
}

// GetBusStatistics Redis 버스 통계 반환
func (rbm *RedisBusDataManager) GetBusStatistics() (map[string]interface{}, error) {
	if rbm.redisClient == nil {
		return map[string]interface{}{
			"redis_enabled": false,
			"total_buses":   0,
			"active_buses":  0,
			"error":         "Redis 연결 없음",
		}, nil
	}

	// 전체 버스 수
	locationPattern := rbm.keyPrefix + "location:*"
	locationKeys, err := rbm.redisClient.Keys(rbm.ctx, locationPattern).Result()
	if err != nil {
		rbm.logger.Errorf("Redis 위치 키 조회 실패: %v", err)
		return nil, err
	}

	// 활성 버스 수
	statusPattern := rbm.keyPrefix + "status:*"
	statusKeys, err := rbm.redisClient.Keys(rbm.ctx, statusPattern).Result()
	if err != nil {
		rbm.logger.Errorf("Redis 상태 키 조회 실패: %v", err)
		return nil, err
	}

	// 마지막 업데이트 시간
	lastUpdateStr, err := rbm.redisClient.Get(rbm.ctx, rbm.keyPrefix+"lastupdate").Result()
	var lastUpdate time.Time
	if err == nil {
		if timestamp, err := strconv.ParseInt(lastUpdateStr, 10, 64); err == nil {
			lastUpdate = time.Unix(timestamp, 0)
		}
	} else if err != redis.Nil {
		rbm.logger.Errorf("Redis 마지막 업데이트 시간 조회 실패: %v", err)
	}

	// 노선 수 계산
	routePattern := rbm.keyPrefix + "route:*:active"
	routeKeys, err := rbm.redisClient.Keys(rbm.ctx, routePattern).Result()
	routeCount := 0
	if err == nil {
		routeCount = len(routeKeys)
	}

	stats := map[string]interface{}{
		"redis_enabled":    true,
		"total_buses":      len(locationKeys),
		"active_buses":     len(statusKeys),
		"active_routes":    routeCount,
		"last_update":      lastUpdate.Format("2006-01-02 15:04:05"),
		"data_ttl_hours":   rbm.dataTTL.Hours(),
		"status_ttl_hours": rbm.statusTTL.Hours(),
		"key_prefix":       rbm.keyPrefix,
	}

	// 상세 정보 (DEBUG 레벨)
	rbm.logger.Debugf("Redis 통계 - 위치키: %d개, 상태키: %d개, 노선키: %d개",
		len(locationKeys), len(statusKeys), routeCount)

	return stats, nil
}

// GetLastESSyncTime 마지막 ES 동기화 시간 조회
func (rbm *RedisBusDataManager) GetLastESSyncTime(plateNo string) (time.Time, error) {
	if rbm.redisClient == nil {
		return time.Time{}, fmt.Errorf("Redis 연결 없음")
	}

	locationKey := rbm.keyPrefix + "location:" + plateNo
	data, err := rbm.getBusData(locationKey)
	if err != nil {
		return time.Time{}, err
	}

	return data.LastESSync, nil
}

// GetLastESData 마지막 ES 전송 데이터 조회
func (rbm *RedisBusDataManager) GetLastESData(plateNo string) (*models.BusLocation, error) {
	if rbm.redisClient == nil {
		return nil, fmt.Errorf("Redis 연결 없음")
	}

	locationKey := rbm.keyPrefix + "location:" + plateNo
	data, err := rbm.getBusData(locationKey)
	if err != nil {
		return nil, err
	}

	// ES에 마지막으로 전송된 데이터가 있는 경우에만 반환
	if data.LastESSync.IsZero() {
		return nil, fmt.Errorf("ES 전송 이력 없음")
	}

	return &data.BusLocation, nil
}

// 내부 헬퍼 메서드들

// getBusData Redis에서 버스 데이터 조회
func (rbm *RedisBusDataManager) getBusData(key string) (*RedisBusData, error) {
	data, err := rbm.redisClient.Get(rbm.ctx, key).Result()
	if err != nil {
		return nil, err
	}

	var busData RedisBusData
	if err := json.Unmarshal([]byte(data), &busData); err != nil {
		return nil, err
	}

	return &busData, nil
}

// hasLocationChanged 위치 변경 여부 확인 (정류장 순서 기준만)
func (rbm *RedisBusDataManager) hasLocationChanged(existing *RedisBusData, new models.BusLocation) bool {
	if existing == nil {
		rbm.logger.Debugf("새 버스 등록 - 차량: %s", new.PlateNo)
		return true
	}

	rbm.logger.Debugf("정류장 위치 변경 없음 - 차량: %s", new.PlateNo)
	return false
}

// needsESSync ES 동기화 필요 여부 확인
func (rbm *RedisBusDataManager) needsESSync(data *RedisBusData, now time.Time) bool {
	// 아직 ES에 동기화되지 않은 경우
	if data.LastESSync.IsZero() {
		return true
	}

	// Redis 업데이트가 ES 동기화보다 최신인 경우
	if data.LastRedisUpdate.After(data.LastESSync) {
		return true
	}

	// 일정 시간(5분) 이상 동기화되지 않은 경우
	if now.Sub(data.LastESSync) > 5*time.Minute {
		return true
	}

	return false
}

// mergeDataSources 데이터 소스 병합
func (rbm *RedisBusDataManager) mergeDataSources(existing *RedisBusData, newSources []string) []string {
	if existing == nil {
		return newSources
	}

	merged := make(map[string]bool)
	for _, source := range existing.DataSources {
		merged[source] = true
	}
	for _, source := range newSources {
		merged[source] = true
	}

	var result []string
	for source := range merged {
		result = append(result, source)
	}

	return result
}

// Close Redis 연결 종료
func (rbm *RedisBusDataManager) Close() error {
	if rbm.redisClient != nil {
		return rbm.redisClient.Close()
	}
	return nil
}

// min 함수
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// logDataDifferences 위치 변경 없을 때 어떤 데이터가 달라졌는지 로깅
func (rbm *RedisBusDataManager) logDataDifferences(existing, new *RedisBusData) {
	rbm.logger.Debugf("=== 위치 변경 없는 Redis 업데이트 분석 - 차량: %s ===", new.PlateNo)

	// 시간 정보 비교
	if !existing.LastRedisUpdate.Equal(new.LastRedisUpdate) {
		rbm.logger.Debugf("  LastRedisUpdate: %s -> %s",
			existing.LastRedisUpdate.Format("15:04:05"), new.LastRedisUpdate.Format("15:04:05"))
	}

	// 데이터 소스 비교
	if len(existing.DataSources) != len(new.DataSources) {
		rbm.logger.Debugf("  DataSources 개수: %d -> %d", len(existing.DataSources), len(new.DataSources))
	}

	// 기타 메타데이터 비교
	if existing.IsActive != new.IsActive {
		rbm.logger.Debugf("  IsActive: %t -> %t", existing.IsActive, new.IsActive)
	}

	// 버스 정보 비교 (위치 외)
	if existing.TripNumber != new.TripNumber {
		rbm.logger.Debugf("  TripNumber: %d -> %d", existing.TripNumber, new.TripNumber)
	}

	if existing.RouteNm != new.RouteNm {
		rbm.logger.Debugf("  RouteNm: %s -> %s", existing.RouteNm, new.RouteNm)
	}

	if existing.Crowded != new.Crowded {
		rbm.logger.Debugf("  Crowded: %d -> %d", existing.Crowded, new.Crowded)
	}

	if existing.RemainSeatCnt != new.RemainSeatCnt {
		rbm.logger.Debugf("  RemainSeatCnt: %d -> %d", existing.RemainSeatCnt, new.RemainSeatCnt)
	}

	if existing.StateCd != new.StateCd {
		rbm.logger.Debugf("  StateCd: %d -> %d", existing.StateCd, new.StateCd)
	}

	rbm.logger.Debugf("=== Redis 업데이트 이유: 메타데이터 또는 부가 정보 변경 ===")
}
