// services/unified_data_manager.go
package services

import (
	"strings"
	"sync"
	"time"

	"bus-tracker/models"
	"bus-tracker/utils"
)

// UnifiedBusData 통합 버스 데이터 구조체
type UnifiedBusData struct {
	// 기본 식별 정보
	PlateNo    string    `json:"plateNo"`    // 차량번호 (Primary Key)
	RouteId    int64     `json:"routeId"`    // 노선ID
	LastUpdate time.Time `json:"lastUpdate"` // 마지막 업데이트 시간

	// API1 데이터 (경기도 버스위치정보 v2)
	API1Data *API1BusInfo `json:"api1Data,omitempty"`

	// API2 데이터 (공공데이터포털 버스위치정보)
	API2Data *API2BusInfo `json:"api2Data,omitempty"`

	// 통합된 최종 데이터
	FinalData *models.BusLocation `json:"finalData"`

	// 메타데이터
	DataSources    []string  `json:"dataSources"` // ["api1", "api2"]
	LastAPI1Update time.Time `json:"lastAPI1Update,omitempty"`
	LastAPI2Update time.Time `json:"lastAPI2Update,omitempty"`
	IsChanged      bool      `json:"isChanged"` // 변경 여부
}

// API1BusInfo API1 전용 정보
type API1BusInfo struct {
	VehId         int64     `json:"vehId"`
	StationId     int64     `json:"stationId"`
	StationSeq    int       `json:"stationSeq"`
	Crowded       int       `json:"crowded"`
	RemainSeatCnt int       `json:"remainSeatCnt"`
	StateCd       int       `json:"stateCd"`
	LowPlate      int       `json:"lowPlate"`
	RouteTypeCd   int       `json:"routeTypeCd"`
	TaglessCd     int       `json:"taglessCd"`
	UpdateTime    time.Time `json:"updateTime"`
}

// API2BusInfo API2 전용 정보
type API2BusInfo struct {
	NodeId     string    `json:"nodeId"`
	NodeNm     string    `json:"nodeNm"`
	NodeOrd    int       `json:"nodeOrd"`
	GpsLati    float64   `json:"gpsLati"`
	GpsLong    float64   `json:"gpsLong"`
	UpdateTime time.Time `json:"updateTime"`
}

// UnifiedDataManager 통합 데이터 관리자
type UnifiedDataManager struct {
	dataStore     map[string]*UnifiedBusData // plateNo -> UnifiedBusData
	mutex         sync.RWMutex
	logger        *utils.Logger
	busTracker    *BusTracker
	stationCache1 *StationCacheService // API1용
	stationCache2 *StationCacheService // API2용
}

// NewUnifiedDataManager 새로운 통합 데이터 관리자 생성
func NewUnifiedDataManager(logger *utils.Logger, busTracker *BusTracker,
	stationCache1, stationCache2 *StationCacheService) *UnifiedDataManager {
	return &UnifiedDataManager{
		dataStore:     make(map[string]*UnifiedBusData),
		logger:        logger,
		busTracker:    busTracker,
		stationCache1: stationCache1,
		stationCache2: stationCache2,
	}
}

// UpdateAPI1Data API1 데이터 업데이트
func (udm *UnifiedDataManager) UpdateAPI1Data(busLocations []models.BusLocation) {
	udm.mutex.Lock()
	defer udm.mutex.Unlock()

	now := time.Now()
	updatedCount := 0

	udm.logger.Infof("API1 데이터 업데이트 시작 - 수신된 버스: %d대", len(busLocations))

	for _, bus := range busLocations {
		plateNo := bus.PlateNo
		if plateNo == "" {
			udm.logger.Warnf("차량번호가 없는 버스 데이터 무시: %+v", bus)
			continue
		}

		// 기존 데이터 조회 또는 새로 생성
		unified, exists := udm.dataStore[plateNo]
		if !exists {
			unified = &UnifiedBusData{
				PlateNo:     plateNo,
				RouteId:     bus.RouteId,
				LastUpdate:  now,
				DataSources: []string{},
			}
			udm.dataStore[plateNo] = unified
			udm.logger.Infof("새 버스 추가 - 차량번호: %s", plateNo)
		}

		// API1 데이터 업데이트
		unified.API1Data = &API1BusInfo{
			VehId:         bus.VehId,
			StationId:     bus.StationId,
			StationSeq:    bus.StationSeq,
			Crowded:       bus.Crowded,
			RemainSeatCnt: bus.RemainSeatCnt,
			StateCd:       bus.StateCd,
			LowPlate:      bus.LowPlate,
			RouteTypeCd:   bus.RouteTypeCd,
			TaglessCd:     bus.TaglessCd,
			UpdateTime:    now,
		}

		unified.LastAPI1Update = now
		unified.LastUpdate = now

		// 데이터 소스 추가
		if !containsString(unified.DataSources, "api1") {
			unified.DataSources = append(unified.DataSources, "api1")
		}

		updatedCount++
		udm.logger.Infof("API1 데이터 업데이트 - 차량: %s, StationSeq: %d, 정류장ID: %d",
			plateNo, bus.StationSeq, bus.StationId)
	}

	udm.logger.Infof("API1 데이터 업데이트 완료: %d대 버스, 총 저장된 버스: %d대", updatedCount, len(udm.dataStore))
}

// UpdateAPI2Data API2 데이터 업데이트
func (udm *UnifiedDataManager) UpdateAPI2Data(busLocations []models.BusLocation) {
	udm.mutex.Lock()
	defer udm.mutex.Unlock()

	now := time.Now()
	updatedCount := 0

	for _, bus := range busLocations {
		plateNo := bus.PlateNo
		if plateNo == "" {
			continue
		}

		// 기존 데이터 조회 또는 새로 생성
		unified, exists := udm.dataStore[plateNo]
		if !exists {
			unified = &UnifiedBusData{
				PlateNo:     plateNo,
				RouteId:     bus.RouteId,
				LastUpdate:  now,
				DataSources: []string{},
			}
			udm.dataStore[plateNo] = unified
		}

		// API2 데이터 업데이트
		unified.API2Data = &API2BusInfo{
			NodeId:     bus.NodeId,
			NodeNm:     bus.NodeNm,
			NodeOrd:    bus.NodeOrd,
			GpsLati:    bus.GpsLati,
			GpsLong:    bus.GpsLong,
			UpdateTime: now,
		}

		unified.LastAPI2Update = now
		unified.LastUpdate = now

		// 데이터 소스 추가
		if !containsString(unified.DataSources, "api2") {
			unified.DataSources = append(unified.DataSources, "api2")
		}

		updatedCount++
	}

	udm.logger.Infof("API2 데이터 업데이트 완료: %d대 버스", updatedCount)
}

// MergeAndProcessData 데이터 통합 및 처리
func (udm *UnifiedDataManager) MergeAndProcessData() []models.BusLocation {
	udm.mutex.Lock()
	defer udm.mutex.Unlock()

	var processedBuses []models.BusLocation
	mergedCount := 0

	for plateNo, unified := range udm.dataStore {
		// 데이터 병합
		finalData := udm.mergeData(unified)
		if finalData == nil {
			continue
		}

		unified.FinalData = finalData
		mergedCount++

		// 정류장 변경 확인
		var trackingKey string
		var currentPosition int64

		if unified.API1Data != nil {
			trackingKey = plateNo
			currentPosition = unified.API1Data.StationId
		} else if unified.API2Data != nil {
			trackingKey = plateNo
			currentPosition = int64(unified.API2Data.NodeOrd)
		}

		// 변경 감지
		if trackingKey != "" && udm.busTracker.IsStationChanged(trackingKey, currentPosition) {
			unified.IsChanged = true
			processedBuses = append(processedBuses, *finalData)

			udm.logger.Infof("통합 데이터 변경 감지 - 차량번호: %s, 소스: %v, 최종위치: %d",
				plateNo, unified.DataSources, currentPosition)
		} else {
			unified.IsChanged = false
		}
	}

	udm.logger.Infof("데이터 통합 완료 - 처리: %d대, 변경: %d대", mergedCount, len(processedBuses))
	return processedBuses
}

// mergeData 데이터 병합 로직
func (udm *UnifiedDataManager) mergeData(unified *UnifiedBusData) *models.BusLocation {
	if unified.API1Data == nil && unified.API2Data == nil {
		return nil
	}

	final := &models.BusLocation{
		PlateNo:   unified.PlateNo,
		RouteId:   unified.RouteId,
		Timestamp: time.Now().Format(time.RFC3339),
	}

	// 두 API 데이터가 모두 있는 경우
	if unified.API1Data != nil && unified.API2Data != nil {
		// API1 우선: 상세 버스 정보
		final.VehId = unified.API1Data.VehId
		final.StationId = unified.API1Data.StationId
		final.StationSeq = unified.API1Data.StationSeq
		final.Crowded = unified.API1Data.Crowded
		final.RemainSeatCnt = unified.API1Data.RemainSeatCnt
		final.StateCd = unified.API1Data.StateCd
		final.LowPlate = unified.API1Data.LowPlate
		final.RouteTypeCd = unified.API1Data.RouteTypeCd
		final.TaglessCd = unified.API1Data.TaglessCd

		// API2 우선: GPS 정보와 정류장 상세
		final.NodeId = unified.API2Data.NodeId
		final.NodeNm = unified.API2Data.NodeNm
		final.NodeOrd = unified.API2Data.NodeOrd
		final.GpsLati = unified.API2Data.GpsLati
		final.GpsLong = unified.API2Data.GpsLong

		udm.logger.Infof("데이터 병합 (API1+API2): %s - 정류장: %s (%d), GPS: (%.6f, %.6f)",
			unified.PlateNo, final.NodeNm, final.StationSeq, final.GpsLati, final.GpsLong)

	} else if unified.API1Data != nil {
		// API1 데이터만 있는 경우
		final.VehId = unified.API1Data.VehId
		final.StationId = unified.API1Data.StationId
		final.StationSeq = unified.API1Data.StationSeq
		final.Crowded = unified.API1Data.Crowded
		final.RemainSeatCnt = unified.API1Data.RemainSeatCnt
		final.StateCd = unified.API1Data.StateCd
		final.LowPlate = unified.API1Data.LowPlate
		final.RouteTypeCd = unified.API1Data.RouteTypeCd
		final.TaglessCd = unified.API1Data.TaglessCd

		// 정류소 정보 보강 (API1 캐시 사용)
		if udm.stationCache1 != nil {
			// 여기서 실제로 정류소 정보 보강이 필요하다면 구현
			// udm.stationCache1.EnrichBusLocationWithStationInfo(final, final.GetRouteIDString())
		}

	} else if unified.API2Data != nil {
		// API2 데이터만 있는 경우
		final.NodeId = unified.API2Data.NodeId
		final.NodeNm = unified.API2Data.NodeNm
		final.NodeOrd = unified.API2Data.NodeOrd
		final.StationSeq = unified.API2Data.NodeOrd // NodeOrd를 StationSeq로 사용
		final.GpsLati = unified.API2Data.GpsLati
		final.GpsLong = unified.API2Data.GpsLong

		// 정류소 정보 보강 (API2 캐시 사용)
		if udm.stationCache2 != nil {
			// 여기서 실제로 정류소 정보 보강이 필요하다면 구현
			// udm.stationCache2.EnrichBusLocationWithStationInfo(final, final.GetRouteIDString())
		}
	}

	return final
}

// GetChangedBuses 변경된 버스 데이터만 반환
func (udm *UnifiedDataManager) GetChangedBuses() []models.BusLocation {
	udm.mutex.RLock()
	defer udm.mutex.RUnlock()

	var changedBuses []models.BusLocation

	for plateNo, unified := range udm.dataStore {
		if unified.IsChanged && unified.FinalData != nil {
			changedBuses = append(changedBuses, *unified.FinalData)
			udm.logger.Infof("변경된 버스 조회 - 차량: %s, 소스: [%s], 정류장: %s",
				plateNo, strings.Join(unified.DataSources, "+"), unified.FinalData.NodeNm)
		}
	}

	udm.logger.Infof("GetChangedBuses 결과 - 총 %d대 변경된 버스", len(changedBuses))

	// 변경 플래그 리셋 (읽은 후에 리셋)
	udm.mutex.RUnlock()
	udm.mutex.Lock()
	for _, unified := range udm.dataStore {
		if unified.IsChanged {
			unified.IsChanged = false
		}
	}
	udm.mutex.Unlock()
	udm.mutex.RLock()

	return changedBuses
}

// CleanupOldData 오래된 데이터 정리
func (udm *UnifiedDataManager) CleanupOldData(maxAge time.Duration) int {
	udm.mutex.Lock()
	defer udm.mutex.Unlock()

	now := time.Now()
	var removedPlates []string

	for plateNo, data := range udm.dataStore {
		if now.Sub(data.LastUpdate) > maxAge {
			removedPlates = append(removedPlates, plateNo)
		}
	}

	// 정리 실행
	for _, plateNo := range removedPlates {
		delete(udm.dataStore, plateNo)
		// BusTracker에서도 제거
		udm.busTracker.RemoveFromTracking(plateNo)
	}

	if len(removedPlates) > 0 {
		udm.logger.Infof("통합 데이터 정리 완료: %d개 버스 데이터 제거 (%.1f분 미목격)",
			len(removedPlates), maxAge.Minutes())
	}

	return len(removedPlates)
}

// GetStatistics 통계 정보 반환
func (udm *UnifiedDataManager) GetStatistics() (int, int, int, int) {
	udm.mutex.RLock()
	defer udm.mutex.RUnlock()

	totalBuses := len(udm.dataStore)
	api1Only := 0
	api2Only := 0
	both := 0

	for _, data := range udm.dataStore {
		hasAPI1 := data.API1Data != nil
		hasAPI2 := data.API2Data != nil

		if hasAPI1 && hasAPI2 {
			both++
		} else if hasAPI1 {
			api1Only++
		} else if hasAPI2 {
			api2Only++
		}
	}

	return totalBuses, api1Only, api2Only, both
}

// containsString 슬라이스에 문자열이 포함되어 있는지 확인
func containsString(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
