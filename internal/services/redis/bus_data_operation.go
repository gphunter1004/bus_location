// internal/services/redis/bus_data_operation.go - Part 2 (tripNumber 로깅 강화)
package redis

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"bus-tracker/internal/models"
)

// API2 전용: 위치정보만 안전 업데이트 (tripNumber 로깅 강화)
func (rbm *RedisBusDataManager) UpdateBusLocation(busLocation models.BusLocation, dataSources []string) (bool, error) {
	if rbm.redisClient == nil {
		return false, fmt.Errorf("Redis 연결 없음")
	}

	plateNo := busLocation.PlateNo
	locationKey := rbm.keyPrefix + "location:" + plateNo
	statusKey := rbm.keyPrefix + "status:" + plateNo
	now := time.Now()

	// 기존 데이터 조회
	existingData, err := rbm.getBusData(locationKey)
	if err != nil && err != redis.Nil {
		rbm.logger.Errorf("기존 데이터 조회 실패 (차량: %s): %v", plateNo, err)
		return false, err
	}

	// 위치 변경만 체크
	locationChanged := rbm.hasLocationChanged(existingData, busLocation)

	// 안전한 필드별 업데이트
	var newData *RedisBusData
	if existingData != nil {
		// 🔢 tripNumber 변경 확인 및 로깅
		oldTripNumber := existingData.TripNumber
		newTripNumber := busLocation.TripNumber

		if oldTripNumber != newTripNumber {
			rbm.logger.Infof("🔢 Redis API2 TripNumber 변경 - 차량: %s, T%d → T%d",
				plateNo, oldTripNumber, newTripNumber)
		} else if newTripNumber > 0 {
			rbm.logger.Debugf("🔒 Redis API2 TripNumber 유지 - 차량: %s, T%d",
				plateNo, newTripNumber)
		}

		// 기존 데이터 복사 후 API2 필드만 업데이트
		newData = &RedisBusData{
			BusLocation: models.BusLocation{
				// 기본 정보
				PlateNo:    busLocation.PlateNo,
				RouteId:    busLocation.RouteId,
				RouteNm:    busLocation.RouteNm,
				TripNumber: newTripNumber, // 🔢 tripNumber 업데이트
				Timestamp:  busLocation.Timestamp,

				// API2 전용 위치 필드만 안전 업데이트
				StationId:     rbm.safeUpdateInt64(existingData.StationId, busLocation.StationId),
				StationSeq:    rbm.safeUpdateInt(existingData.StationSeq, busLocation.StationSeq),
				NodeId:        rbm.safeUpdateString(existingData.NodeId, busLocation.NodeId),
				NodeNm:        rbm.safeUpdateString(existingData.NodeNm, busLocation.NodeNm),
				NodeOrd:       rbm.safeUpdateInt(existingData.NodeOrd, busLocation.NodeOrd),
				NodeNo:        rbm.safeUpdateInt(existingData.NodeNo, busLocation.NodeNo),
				GpsLati:       rbm.safeUpdateFloat64(existingData.GpsLati, busLocation.GpsLati),
				GpsLong:       rbm.safeUpdateFloat64(existingData.GpsLong, busLocation.GpsLong),
				TotalStations: rbm.safeUpdateInt(existingData.TotalStations, busLocation.TotalStations),

				// API1 상태 정보는 완전히 보존
				Crowded:       existingData.Crowded,
				LowPlate:      existingData.LowPlate,
				RemainSeatCnt: existingData.RemainSeatCnt,
				RouteTypeCd:   existingData.RouteTypeCd,
				StateCd:       existingData.StateCd,
				TaglessCd:     existingData.TaglessCd,
				VehId:         existingData.VehId,
			},
			LastRedisUpdate: now,
			LastESSync:      existingData.LastESSync,
			ChangeCount:     existingData.ChangeCount,
			DataSources:     rbm.mergeDataSources(existingData, dataSources),
			IsActive:        true,
			LastAPI1Update:  existingData.LastAPI1Update,
			LastAPI2Update:  now,
		}

		if locationChanged {
			newData.ChangeCount++
		}
	} else {
		// 🆕 새로운 버스인 경우
		rbm.logger.Infof("🆕 Redis API2 신규 버스 - 차량: %s, TripNumber: T%d",
			plateNo, busLocation.TripNumber)

		newData = &RedisBusData{
			BusLocation:     busLocation,
			LastRedisUpdate: now,
			ChangeCount:     1,
			DataSources:     dataSources,
			IsActive:        true,
			LastAPI2Update:  now,
		}
		locationChanged = true
	}

	// Pipeline으로 업데이트
	pipe := rbm.redisClient.Pipeline()

	if locationChanged {
		locationData, err := json.Marshal(newData)
		if err != nil {
			return false, fmt.Errorf("데이터 마샬링 실패: %v", err)
		}
		pipe.Set(rbm.ctx, locationKey, locationData, rbm.dataTTL)
		pipe.Set(rbm.ctx, rbm.keyPrefix+"lastupdate", now.Unix(), time.Hour)

		// 🔢 Redis 저장 완료 로깅
		rbm.logger.Debugf("💾 Redis API2 저장 - 차량: %s, TripNumber: T%d",
			plateNo, newData.BusLocation.TripNumber)
	} else {
		pipe.Expire(rbm.ctx, locationKey, rbm.dataTTL)
	}

	// 버스 상태 업데이트
	status := &BusStatus{
		PlateNo:      plateNo,
		IsActive:     true,
		LastSeenTime: now,
		TripNumber:   busLocation.TripNumber,
		IsTerminated: false,
		RouteId:      busLocation.RouteId,
		RouteNm:      busLocation.RouteNm,
	}
	statusData, _ := json.Marshal(status)
	pipe.Set(rbm.ctx, statusKey, statusData, rbm.statusTTL)

	// 노선별 활성 버스 목록
	routeKey := fmt.Sprintf("%sroute:%d:active", rbm.keyPrefix, busLocation.RouteId)
	pipe.SAdd(rbm.ctx, routeKey, plateNo)
	pipe.Expire(rbm.ctx, routeKey, rbm.statusTTL)

	// Pipeline 실행
	_, err = pipe.Exec(rbm.ctx)
	if err != nil {
		return false, fmt.Errorf("Redis 업데이트 실패: %v", err)
	}

	if locationChanged {
		rbm.logger.Infof("📍 API2 위치 업데이트 - 차량: %s (위치: %s, 순서: %d)",
			plateNo, newData.NodeNm, rbm.getStationOrder(newData.BusLocation))
	} else {
		rbm.logger.Debugf("⏸️ API2 위치 변경 없음 - 차량: %s", plateNo)
	}

	return locationChanged, nil
}

// 안전 업데이트 헬퍼 함수들
func (rbm *RedisBusDataManager) safeUpdateInt(existing, new int) int {
	if new != 0 {
		return new
	}
	return existing
}

func (rbm *RedisBusDataManager) safeUpdateInt64(existing, new int64) int64 {
	if new != 0 {
		return new
	}
	return existing
}

func (rbm *RedisBusDataManager) safeUpdateString(existing, new string) string {
	if new != "" {
		return new
	}
	return existing
}

func (rbm *RedisBusDataManager) safeUpdateFloat64(existing, new float64) float64 {
	if new != 0 {
		return new
	}
	return existing
}

// 정류장 순서 반환 헬퍼 함수
func (rbm *RedisBusDataManager) getStationOrder(bus models.BusLocation) int {
	if bus.NodeOrd > 0 {
		return bus.NodeOrd
	} else if bus.StationSeq > 0 {
		return bus.StationSeq
	}
	return 0
}

// 위치 변경 체크
func (rbm *RedisBusDataManager) hasLocationChanged(existing *RedisBusData, new models.BusLocation) bool {
	if existing == nil {
		rbm.logger.Debugf("🆕 새 버스 등록 - 차량: %s", new.PlateNo)
		return true
	}

	// 정류장 순서 비교
	var existingOrder, newOrder int

	if existing.NodeOrd > 0 {
		existingOrder = existing.NodeOrd
	} else if existing.StationSeq > 0 {
		existingOrder = existing.StationSeq
	}

	if new.NodeOrd > 0 {
		newOrder = new.NodeOrd
	} else if new.StationSeq > 0 {
		newOrder = new.StationSeq
	}

	// 정류장 순서 변경
	if existingOrder != newOrder && newOrder > 0 {
		rbm.logger.Debugf("📍 정류장 순서 변경 - 차량: %s, %d -> %d", new.PlateNo, existingOrder, newOrder)
		return true
	}

	// StationId 변경
	if existing.StationId != new.StationId && new.StationId > 0 && existing.StationId > 0 {
		rbm.logger.Debugf("🏪 StationId 변경 - 차량: %s, %d -> %d", new.PlateNo, existing.StationId, new.StationId)
		return true
	}

	// NodeId 변경
	if existing.NodeId != new.NodeId && new.NodeId != "" && existing.NodeId != "" {
		rbm.logger.Debugf("🚏 NodeId 변경 - 차량: %s, %s -> %s", new.PlateNo, existing.NodeId, new.NodeId)
		return true
	}

	return false
}

// 상태 변경 체크
func (rbm *RedisBusDataManager) hasBusStatusChanged(existing *RedisBusData, new models.BusLocation) bool {
	if existing == nil {
		return true
	}

	// 의미있는 변경만 체크
	if new.RemainSeatCnt > 0 && existing.RemainSeatCnt != new.RemainSeatCnt {
		rbm.logger.Debugf("💺 좌석정보 변경 - 차량: %s, %d -> %d", new.PlateNo, existing.RemainSeatCnt, new.RemainSeatCnt)
		return true
	}

	if new.Crowded > 0 && existing.Crowded != new.Crowded {
		rbm.logger.Debugf("👥 혼잡도 변경 - 차량: %s, %d -> %d", new.PlateNo, existing.Crowded, new.Crowded)
		return true
	}

	if new.StateCd > 0 && existing.StateCd != new.StateCd {
		rbm.logger.Debugf("📍 상태코드 변경 - 차량: %s, %d -> %d", new.PlateNo, existing.StateCd, new.StateCd)
		return true
	}

	if existing.LowPlate != new.LowPlate {
		rbm.logger.Debugf("♿ 저상버스 변경 - 차량: %s, %d -> %d", new.PlateNo, existing.LowPlate, new.LowPlate)
		return true
	}

	if existing.TripNumber != new.TripNumber {
		rbm.logger.Debugf("🔄 운행차수 변경 - 차량: %s, T%d -> T%d", new.PlateNo, existing.TripNumber, new.TripNumber)
		return true
	}

	return false
}

// GetChangedBusesForES 변경된 버스 데이터 조회
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

		if rbm.needsESSync(data, now) {
			changedBuses = append(changedBuses, data.BusLocation)
		}
	}

	if len(changedBuses) > 0 {
		rbm.logger.Infof("📤 ES 동기화 대상 - %d건 (전체 Redis 키: %d개)", len(changedBuses), len(keys))
	}

	return changedBuses, nil
}

// ES 동기화 필요 여부 확인
func (rbm *RedisBusDataManager) needsESSync(data *RedisBusData, now time.Time) bool {
	if data.LastESSync.IsZero() {
		return true
	}

	if data.LastRedisUpdate.After(data.LastESSync) {
		return true
	}

	if now.Sub(data.LastESSync) > 20*time.Minute {
		return true
	}

	return false
}

// MarkAsSynced ES 동기화 완료 마킹
func (rbm *RedisBusDataManager) MarkAsSynced(plateNos []string) error {
	if rbm.redisClient == nil || len(plateNos) == 0 {
		return nil
	}

	now := time.Now()
	pipe := rbm.redisClient.Pipeline()
	successCount := 0

	for _, plateNo := range plateNos {
		key := rbm.keyPrefix + "location:" + plateNo

		data, err := rbm.getBusData(key)
		if err != nil {
			continue
		}

		data.LastESSync = now
		updatedData, _ := json.Marshal(data)
		pipe.Set(rbm.ctx, key, updatedData, rbm.dataTTL)
		successCount++
	}

	_, err := pipe.Exec(rbm.ctx)
	if err != nil {
		rbm.logger.Errorf("ES 동기화 마킹 실패: %v", err)
	} else {
		rbm.logger.Debugf("✅ ES 동기화 마킹 완료: %d/%d건", successCount, len(plateNos))
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

			plateNo := status.PlateNo
			locationKey := rbm.keyPrefix + "location:" + plateNo
			inactiveKeys = append(inactiveKeys, locationKey)
			inactivePlates = append(inactivePlates, plateNo)

			routeKey := fmt.Sprintf("%sroute:%d:active", rbm.keyPrefix, status.RouteId)
			rbm.redisClient.SRem(rbm.ctx, routeKey, plateNo)
		}
	}

	if len(inactiveKeys) > 0 {
		rbm.redisClient.Del(rbm.ctx, inactiveKeys...)
		rbm.logger.Infof("🧹 Redis 비활성 버스 정리: %d대", len(inactivePlates))
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

	locationPattern := rbm.keyPrefix + "location:*"
	statusPattern := rbm.keyPrefix + "status:*"

	locationKeys, err := rbm.redisClient.Keys(rbm.ctx, locationPattern).Result()
	if err != nil {
		return nil, err
	}

	statusKeys, err := rbm.redisClient.Keys(rbm.ctx, statusPattern).Result()
	if err != nil {
		return nil, err
	}

	// 활성 버스 계산 (최근 30분 이내에 업데이트된 버스)
	now := time.Now()
	activeBuses := 0
	for _, key := range statusKeys {
		data, err := rbm.redisClient.Get(rbm.ctx, key).Result()
		if err != nil {
			continue
		}

		var status BusStatus
		if err := json.Unmarshal([]byte(data), &status); err != nil {
			continue
		}

		if now.Sub(status.LastSeenTime) <= 30*time.Minute {
			activeBuses++
		}
	}

	return map[string]interface{}{
		"redis_enabled": true,
		"total_buses":   len(locationKeys),
		"active_buses":  activeBuses,
		"status_keys":   len(statusKeys),
		"last_update":   rbm.getLastUpdateTime(),
	}, nil
}

// getLastUpdateTime 마지막 업데이트 시간 조회
func (rbm *RedisBusDataManager) getLastUpdateTime() string {
	if rbm.redisClient == nil {
		return "정보 없음"
	}

	lastUpdateKey := rbm.keyPrefix + "lastupdate"
	timestamp, err := rbm.redisClient.Get(rbm.ctx, lastUpdateKey).Result()
	if err != nil {
		return "정보 없음"
	}

	if unixTime, err := time.Parse("1136214245", timestamp); err == nil {
		return unixTime.Format("2006-01-02 15:04:05")
	}

	return timestamp
}

// Close Redis 연결 종료
func (rbm *RedisBusDataManager) Close() error {
	if rbm.redisClient != nil {
		return rbm.redisClient.Close()
	}
	return nil
}

// GetBusLocationData 특정 버스의 위치 데이터 조회
func (rbm *RedisBusDataManager) GetBusLocationData(plateNo string) (*RedisBusData, error) {
	if rbm.redisClient == nil {
		return nil, fmt.Errorf("Redis 연결 없음")
	}

	locationKey := rbm.keyPrefix + "location:" + plateNo
	return rbm.getBusData(locationKey)
}

// GetBusStatus 특정 버스의 상태 조회
func (rbm *RedisBusDataManager) GetBusStatus(plateNo string) (*BusStatus, error) {
	if rbm.redisClient == nil {
		return nil, fmt.Errorf("Redis 연결 없음")
	}

	statusKey := rbm.keyPrefix + "status:" + plateNo
	data, err := rbm.redisClient.Get(rbm.ctx, statusKey).Result()
	if err != nil {
		return nil, err
	}

	var status BusStatus
	if err := json.Unmarshal([]byte(data), &status); err != nil {
		return nil, err
	}

	return &status, nil
}

// GetAllActiveBuses 모든 활성 버스 목록 조회
func (rbm *RedisBusDataManager) GetAllActiveBuses() ([]string, error) {
	if rbm.redisClient == nil {
		return nil, fmt.Errorf("Redis 연결 없음")
	}

	pattern := rbm.keyPrefix + "status:*"
	keys, err := rbm.redisClient.Keys(rbm.ctx, pattern).Result()
	if err != nil {
		return nil, err
	}

	now := time.Now()
	var activeBuses []string

	for _, key := range keys {
		data, err := rbm.redisClient.Get(rbm.ctx, key).Result()
		if err != nil {
			continue
		}

		var status BusStatus
		if err := json.Unmarshal([]byte(data), &status); err != nil {
			continue
		}

		// 최근 30분 이내에 업데이트된 버스만
		if now.Sub(status.LastSeenTime) <= 30*time.Minute {
			activeBuses = append(activeBuses, status.PlateNo)
		}
	}

	return activeBuses, nil
}

// FlushAllBusData 모든 버스 데이터 삭제 (테스트용)
func (rbm *RedisBusDataManager) FlushAllBusData() error {
	if rbm.redisClient == nil {
		return fmt.Errorf("Redis 연결 없음")
	}

	patterns := []string{
		rbm.keyPrefix + "location:*",
		rbm.keyPrefix + "status:*",
		rbm.keyPrefix + "route:*",
		rbm.keyPrefix + "lastupdate",
	}

	for _, pattern := range patterns {
		keys, err := rbm.redisClient.Keys(rbm.ctx, pattern).Result()
		if err != nil {
			continue
		}

		if len(keys) > 0 {
			rbm.redisClient.Del(rbm.ctx, keys...)
		}
	}

	rbm.logger.Info("🗑️ 모든 Redis 버스 데이터 삭제 완료")
	return nil
}

// PrintBusDataStatistics Redis 버스 데이터 통계 출력
func (rbm *RedisBusDataManager) PrintBusDataStatistics() {
	stats, err := rbm.GetBusStatistics()
	if err != nil {
		rbm.logger.Errorf("Redis 통계 조회 실패: %v", err)
		return
	}

	rbm.logger.Infof("📊 Redis 버스 데이터 통계:")
	rbm.logger.Infof("   💾 총 버스: %v대", stats["total_buses"])
	rbm.logger.Infof("   🚌 활성 버스: %v대", stats["active_buses"])
	rbm.logger.Infof("   📋 상태 키: %v개", stats["status_keys"])
	rbm.logger.Infof("   ⏰ 마지막 업데이트: %v", stats["last_update"])

	if enabled, ok := stats["redis_enabled"].(bool); ok && !enabled {
		rbm.logger.Warn("   ⚠️ Redis 연결 없음")
	}
}

// SetBusTerminated 버스 종료 상태 설정
func (rbm *RedisBusDataManager) SetBusTerminated(plateNo string, reason string) error {
	if rbm.redisClient == nil {
		return fmt.Errorf("Redis 연결 없음")
	}

	statusKey := rbm.keyPrefix + "status:" + plateNo
	status, err := rbm.GetBusStatus(plateNo)
	if err != nil {
		return fmt.Errorf("버스 상태 조회 실패: %v", err)
	}

	status.IsTerminated = true
	status.IsActive = false

	statusData, err := json.Marshal(status)
	if err != nil {
		return fmt.Errorf("상태 데이터 마샬링 실패: %v", err)
	}

	err = rbm.redisClient.Set(rbm.ctx, statusKey, statusData, rbm.statusTTL).Err()
	if err != nil {
		return fmt.Errorf("Redis 상태 업데이트 실패: %v", err)
	}

	// 노선별 활성 목록에서 제거
	routeKey := fmt.Sprintf("%sroute:%d:active", rbm.keyPrefix, status.RouteId)
	rbm.redisClient.SRem(rbm.ctx, routeKey, plateNo)

	rbm.logger.Infof("🔚 버스 종료 처리 완료 - 차량: %s, 이유: %s", plateNo, reason)
	return nil
}
