// internal/services/redis/bus_data_manager.go - Part 1
package redis

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

// RedisBusData Redis에 저장할 버스 데이터
type RedisBusData struct {
	models.BusLocation

	// Redis 전용 메타데이터
	LastRedisUpdate time.Time `json:"lastRedisUpdate"`
	LastESSync      time.Time `json:"lastESSync"`
	ChangeCount     int       `json:"changeCount"`
	DataSources     []string  `json:"dataSources"`
	IsActive        bool      `json:"isActive"`

	// 필드별 업데이트 추적
	LastAPI1Update time.Time `json:"lastAPI1Update"`
	LastAPI2Update time.Time `json:"lastAPI2Update"`
}

// BusStatus 버스 상태 추적
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
	redisClient := redis.NewClient(&redis.Options{
		Addr:            cfg.Redis.Addr,
		Password:        cfg.Redis.Password,
		DB:              cfg.Redis.DB + 1,
		MaxRetries:      cfg.Redis.MaxRetries,
		PoolSize:        cfg.Redis.PoolSize,
		ConnMaxIdleTime: time.Duration(cfg.Redis.IdleTimeout) * time.Second,
		ReadTimeout:     10 * time.Second,
		WriteTimeout:    10 * time.Second,
	})

	ctx := context.Background()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		logger.Errorf("Redis 버스 데이터 매니저 연결 실패: %v", err)
		return &RedisBusDataManager{
			redisClient: nil,
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
		dataTTL:     2 * time.Hour,
		statusTTL:   24 * time.Hour,
		ctx:         ctx,
	}
}

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

// mergeDataSources 데이터 소스 병합
func (rbm *RedisBusDataManager) mergeDataSources(existing *RedisBusData, newSources []string) []string {
	if existing == nil {
		return newSources
	}

	sourceMap := make(map[string]bool)

	// 기존 소스 추가
	for _, source := range existing.DataSources {
		sourceMap[source] = true
	}

	// 새 소스 추가
	for _, source := range newSources {
		sourceMap[source] = true
	}

	// map을 slice로 변환
	var result []string
	for source := range sourceMap {
		result = append(result, source)
	}

	return result
}

// API1 전용: 상태정보만 안전 업데이트
func (rbm *RedisBusDataManager) UpdateBusStatusOnly(busLocation models.BusLocation, dataSources []string) (bool, error) {
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

	// 상태 변경만 체크
	statusChanged := rbm.hasBusStatusChanged(existingData, busLocation)

	// 안전한 필드별 업데이트
	var newData *RedisBusData
	if existingData != nil {
		// 기존 데이터 복사 후 API1 필드만 업데이트
		newData = &RedisBusData{
			BusLocation: models.BusLocation{
				// 위치 정보는 기존 데이터 완전 유지
				RouteId:       existingData.RouteId,
				RouteNm:       existingData.RouteNm,
				PlateNo:       existingData.PlateNo,
				StationId:     existingData.StationId,
				StationSeq:    existingData.StationSeq,
				NodeId:        existingData.NodeId,
				NodeNm:        existingData.NodeNm,
				NodeOrd:       existingData.NodeOrd,
				NodeNo:        existingData.NodeNo,
				GpsLati:       existingData.GpsLati,
				GpsLong:       existingData.GpsLong,
				TotalStations: existingData.TotalStations,
				TripNumber:    busLocation.TripNumber,

				// API1 전용 필드만 안전 업데이트
				Crowded:       rbm.safeUpdateInt(existingData.Crowded, busLocation.Crowded),
				LowPlate:      rbm.safeUpdateInt(existingData.LowPlate, busLocation.LowPlate),
				RemainSeatCnt: rbm.safeUpdateInt(existingData.RemainSeatCnt, busLocation.RemainSeatCnt),
				RouteTypeCd:   rbm.safeUpdateInt(existingData.RouteTypeCd, busLocation.RouteTypeCd),
				StateCd:       rbm.safeUpdateInt(existingData.StateCd, busLocation.StateCd),
				TaglessCd:     rbm.safeUpdateInt(existingData.TaglessCd, busLocation.TaglessCd),
				VehId:         rbm.safeUpdateInt64(existingData.VehId, busLocation.VehId),
				Timestamp:     busLocation.Timestamp,
			},
			LastRedisUpdate: now,
			LastESSync:      existingData.LastESSync,
			ChangeCount:     existingData.ChangeCount,
			DataSources:     rbm.mergeDataSources(existingData, dataSources),
			IsActive:        true,
			LastAPI1Update:  now,
			LastAPI2Update:  existingData.LastAPI2Update,
		}

		if statusChanged {
			newData.ChangeCount++
		}
	} else {
		// 새로운 버스인 경우
		newData = &RedisBusData{
			BusLocation:     busLocation,
			LastRedisUpdate: now,
			ChangeCount:     1,
			DataSources:     dataSources,
			IsActive:        true,
			LastAPI1Update:  now,
		}
		statusChanged = true
	}

	// Pipeline으로 업데이트
	pipe := rbm.redisClient.Pipeline()

	if statusChanged {
		locationData, err := json.Marshal(newData)
		if err != nil {
			return false, fmt.Errorf("데이터 마샬링 실패: %v", err)
		}
		pipe.Set(rbm.ctx, locationKey, locationData, rbm.dataTTL)
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

	if statusChanged {
		rbm.logger.Infof("🔧 API1 상태 업데이트 - 차량: %s (좌석:%d, 혼잡:%d, 상태:%d)",
			plateNo, newData.RemainSeatCnt, newData.Crowded, newData.StateCd)
	} else {
		rbm.logger.Debugf("⏸️ API1 상태 변경 없음 - 차량: %s", plateNo)
	}

	return statusChanged, nil
}
