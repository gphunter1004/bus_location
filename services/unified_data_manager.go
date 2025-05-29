// services/unified_data_manager.go
package services

import (
	"fmt"
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
	stationCache1 *StationCacheService  // API1용
	stationCache2 *StationCacheService  // API2용
	esService     *ElasticsearchService // ES 서비스 직접 참조
	indexName     string                // ES 인덱스명
}

// NewUnifiedDataManager 새로운 통합 데이터 관리자 생성
func NewUnifiedDataManager(logger *utils.Logger, busTracker *BusTracker,
	stationCache1, stationCache2 *StationCacheService,
	esService *ElasticsearchService, indexName string) *UnifiedDataManager {
	return &UnifiedDataManager{
		dataStore:     make(map[string]*UnifiedBusData),
		logger:        logger,
		busTracker:    busTracker,
		stationCache1: stationCache1,
		stationCache2: stationCache2,
		esService:     esService,
		indexName:     indexName,
	}
}

// UpdateAPI1Data API1 데이터 업데이트 + 즉시 통합 처리
func (udm *UnifiedDataManager) UpdateAPI1Data(busLocations []models.BusLocation) {
	udm.mutex.Lock()
	defer udm.mutex.Unlock()

	now := time.Now()
	updatedCount := 0
	ignoredCount := 0

	udm.logger.Infof("API1 데이터 업데이트 시작 - 수신된 버스: %d대", len(busLocations))

	// 변경된 버스들을 추적하기 위한 슬라이스
	var changedBuses []models.BusLocation

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
		} else {
			// 기존 데이터가 있는 경우 sequence 체크
			if unified.API1Data != nil {
				existingSeq := unified.API1Data.StationSeq
				newSeq := bus.StationSeq

				// 새로운 sequence가 기존보다 작으면 무시
				if newSeq < existingSeq {
					udm.logger.Warnf("API1 데이터 무시 - 차량번호: %s, 기존 seq: %d > 새 seq: %d (역순 이동 감지)",
						plateNo, existingSeq, newSeq)
					ignoredCount++
					continue
				}

				// sequence가 동일하면 무시 (중복 데이터)
				if newSeq == existingSeq {
					udm.logger.Infof("API1 데이터 무시 - 차량번호: %s, seq: %d (동일한 위치, 중복 데이터)",
						plateNo, newSeq)
					ignoredCount++
					continue
				}

				udm.logger.Infof("API1 데이터 진행 확인 - 차량번호: %s, 기존 seq: %d → 새 seq: %d (정상 진행)",
					plateNo, existingSeq, newSeq)
			}
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

		// 즉시 데이터 통합 및 변경 감지
		finalData := udm.mergeDataForBus(unified)
		if finalData != nil {
			unified.FinalData = finalData

			// 정류장 변경 확인 (핵심: 정류장이 변경된 경우만 ES 전송)
			currentPosition := unified.API1Data.StationId
			if udm.busTracker.IsStationChanged(plateNo, currentPosition) {
				changedBuses = append(changedBuses, *finalData)

				udm.logger.Infof("API1 정류장 변경 감지 - 차량번호: %s, 소스: [%s], 정류장위치: %d → ES 전송 예정",
					plateNo, strings.Join(unified.DataSources, "+"), currentPosition)
			} else {
				// 정류장은 변경되지 않았지만 마지막 목격 시간은 업데이트
				udm.busTracker.UpdateLastSeenTime(plateNo)
				udm.logger.Infof("API1 정류장 유지 - 차량번호: %s, 정류장위치: %d → ES 전송 생략",
					plateNo, currentPosition)
			}
		}

		updatedCount++
		udm.logger.Infof("API1 데이터 업데이트 - 차량: %s, StationSeq: %d, 정류장ID: %d",
			plateNo, bus.StationSeq, bus.StationId)
	}

	udm.logger.Infof("API1 데이터 업데이트 완료: 처리=%d대, 정류장변경=%d대, 무시=%d대 (역순/중복)",
		updatedCount, len(changedBuses), ignoredCount)

	// 정류장이 변경된 버스가 있으면 즉시 ES로 전송
	if len(changedBuses) > 0 {
		udm.sendChangedBusesToElasticsearch(changedBuses, "API1")
	}
}

// UpdateAPI2Data API2 데이터 업데이트 + 즉시 통합 처리
func (udm *UnifiedDataManager) UpdateAPI2Data(busLocations []models.BusLocation) {
	udm.mutex.Lock()
	defer udm.mutex.Unlock()

	now := time.Now()
	updatedCount := 0
	ignoredCount := 0

	udm.logger.Infof("API2 데이터 업데이트 시작 - 수신된 버스: %d대", len(busLocations))

	// 변경된 버스들을 추적하기 위한 슬라이스
	var changedBuses []models.BusLocation

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
		} else {
			// 기존 데이터가 있는 경우 sequence 체크
			if unified.API2Data != nil {
				existingOrd := unified.API2Data.NodeOrd
				newOrd := bus.NodeOrd

				// 새로운 NodeOrd가 기존보다 작으면 무시
				if newOrd < existingOrd {
					udm.logger.Warnf("API2 데이터 무시 - 차량번호: %s, 기존 ord: %d > 새 ord: %d (역순 이동 감지)",
						plateNo, existingOrd, newOrd)
					ignoredCount++
					continue
				}

				// NodeOrd가 동일하면 무시 (중복 데이터)
				if newOrd == existingOrd {
					udm.logger.Infof("API2 데이터 무시 - 차량번호: %s, ord: %d (동일한 위치, 중복 데이터)",
						plateNo, newOrd)
					ignoredCount++
					continue
				}

				udm.logger.Infof("API2 데이터 진행 확인 - 차량번호: %s, 기존 ord: %d → 새 ord: %d (정상 진행)",
					plateNo, existingOrd, newOrd)
			}
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

		// 즉시 데이터 통합 및 변경 감지
		finalData := udm.mergeDataForBus(unified)
		if finalData != nil {
			unified.FinalData = finalData

			// 정류장 변경 확인 (핵심: 정류장이 변경된 경우만 ES 전송)
			currentPosition := int64(unified.API2Data.NodeOrd)
			if udm.busTracker.IsStationChanged(plateNo, currentPosition) {
				changedBuses = append(changedBuses, *finalData)

				udm.logger.Infof("API2 정류장 변경 감지 - 차량번호: %s, 소스: [%s], 정류장위치: %d → ES 전송 예정",
					plateNo, strings.Join(unified.DataSources, "+"), currentPosition)
			} else {
				// 정류장은 변경되지 않았지만 마지막 목격 시간은 업데이트
				udm.busTracker.UpdateLastSeenTime(plateNo)
				udm.logger.Infof("API2 정류장 유지 - 차량번호: %s, 정류장위치: %d → ES 전송 생략",
					plateNo, currentPosition)
			}
		}

		updatedCount++
		udm.logger.Infof("API2 데이터 업데이트 - 차량: %s, NodeOrd: %d, 정류장: %s (%s)",
			plateNo, bus.NodeOrd, bus.NodeNm, bus.NodeId)
	}

	udm.logger.Infof("API2 데이터 업데이트 완료: 처리=%d대, 정류장변경=%d대, 무시=%d대 (역순/중복)",
		updatedCount, len(changedBuses), ignoredCount)

	// 정류장이 변경된 버스가 있으면 즉시 ES로 전송
	if len(changedBuses) > 0 {
		udm.sendChangedBusesToElasticsearch(changedBuses, "API2")
	}
}

// sendChangedBusesToElasticsearch 정류장이 변경된 버스만 ES로 전송
func (udm *UnifiedDataManager) sendChangedBusesToElasticsearch(changedBuses []models.BusLocation, source string) {
	if udm.esService == nil {
		udm.logger.Warn("ES 서비스가 설정되지 않아 전송을 건너뜁니다")
		return
	}

	udm.logger.Infof("=== Elasticsearch 정류장 변경 버스 전송 시작 (%s: %d대) ===", source, len(changedBuses))

	// 정류장이 변경된 버스 정보를 로깅
	for i, bus := range changedBuses {
		// 기본 위치 정보
		var locationInfo string
		if bus.NodeNm != "" && bus.NodeId != "" {
			locationInfo = fmt.Sprintf("정류장: %s (%s), 순서: %d/%d",
				bus.NodeNm, bus.NodeId, bus.NodeOrd, bus.TotalStations)
		} else {
			locationInfo = fmt.Sprintf("정류장ID: %d, 순서: %d/%d",
				bus.StationId, bus.StationSeq, bus.TotalStations)
		}

		// GPS 정보
		var gpsInfo string
		if bus.GpsLati != 0 && bus.GpsLong != 0 {
			gpsInfo = fmt.Sprintf(", GPS: (%.6f, %.6f)", bus.GpsLati, bus.GpsLong)
		}

		// 상세 버스 정보 (요청된 필드들)
		var detailInfo string
		if source == "API1" || (source == "API2" && bus.VehId != 0) {
			// API1 또는 통합 데이터에서 VehId가 있는 경우
			detailInfo = fmt.Sprintf(", 차량ID: %d, 잔여석: %d석, 혼잡도: %d",
				bus.VehId, bus.RemainSeatCnt, bus.Crowded)
		} else {
			// API2만 있거나 VehId가 없는 경우
			detailInfo = fmt.Sprintf(", 혼잡도: %d", bus.Crowded)
		}

		// 정류장 변경 전송 로그
		udm.logger.Infof("ES 정류장변경 전송 [%d/%d] - 차량번호: %s, 노선: %d, %s%s%s",
			i+1, len(changedBuses), bus.PlateNo, bus.RouteId, locationInfo, gpsInfo, detailInfo)
	}

	startTime := time.Now()

	if err := udm.esService.BulkSendBusLocations(udm.indexName, changedBuses); err != nil {
		udm.logger.Errorf("ES 정류장 변경 버스 전송 실패 (%s): %v", source, err)
		return
	}

	duration := time.Since(startTime)
	udm.logger.Infof("ES 정류장 변경 버스 전송 완료 (%s) - %d대 버스, 소요시간: %v", source, len(changedBuses), duration)

	// 전송 완료 요약
	udm.logger.Infof("=== Elasticsearch 정류장 변경 버스 전송 완료 (%s) ===", source)
	udm.logger.Infof("💾 정류장 변경 데이터: %d건, 인덱스: %s, 소요시간: %v", len(changedBuses), udm.indexName, duration)
}

// mergeDataForBus 특정 버스의 데이터 병합
func (udm *UnifiedDataManager) mergeDataForBus(unified *UnifiedBusData) *models.BusLocation {
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

		// 전체 정류소 개수 설정 (캐시에서)
		if udm.stationCache1 != nil {
			final.TotalStations = udm.stationCache1.GetRouteStationCount(final.GetRouteIDString())
		}
		if udm.stationCache2 != nil && final.TotalStations == 0 {
			final.TotalStations = udm.stationCache2.GetRouteStationCount(final.GetRouteIDString())
		}

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
			udm.stationCache1.EnrichBusLocationWithStationInfo(final, final.GetRouteIDString())
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
			udm.stationCache2.EnrichBusLocationWithStationInfo(final, final.GetRouteIDString())
		}
	}

	return final
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
