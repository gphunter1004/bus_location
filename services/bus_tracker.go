package services

import (
	"sync"

	"bus-tracker/models"
	"bus-tracker/utils"
)

// BusTracker 버스별 마지막 정류장 정보를 추적하는 서비스
type BusTracker struct {
	lastStationMap map[string]int64 // key: plateNo, value: stationId
	mutex          sync.RWMutex
}

// NewBusTracker 새로운 BusTracker 생성
func NewBusTracker() *BusTracker {
	return &BusTracker{
		lastStationMap: make(map[string]int64),
	}
}

// IsStationChanged 정류장 변경 여부 확인 및 상태 업데이트
func (bt *BusTracker) IsStationChanged(plateNo string, currentStationId int64) bool {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()
	
	lastStationId, exists := bt.lastStationMap[plateNo]
	
	// 처음 보는 버스이거나 정류장이 변경된 경우
	if !exists || lastStationId != currentStationId {
		bt.lastStationMap[plateNo] = currentStationId
		return true
	}
	
	return false
}

// GetTrackedBusCount 현재 추적 중인 버스 개수 반환
func (bt *BusTracker) GetTrackedBusCount() int {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()
	return len(bt.lastStationMap)
}

// GetLastStation 특정 버스의 마지막 정류장 정보 조회
func (bt *BusTracker) GetLastStation(plateNo string) (int64, bool) {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()
	stationId, exists := bt.lastStationMap[plateNo]
	return stationId, exists
}

// FilterChangedStations 정류장 변경된 버스만 필터링
func (bt *BusTracker) FilterChangedStations(busLocations []models.BusLocation, logger *utils.Logger) []models.BusLocation {
	var changedBuses []models.BusLocation
	
	for _, bus := range busLocations {
		// 차량번호를 추적 키로 사용 (API1, API2 모두 차량번호 존재)
		trackingKey := bus.PlateNo
		if trackingKey == "" {
			// 차량번호가 없으면 NodeId 사용 (예외 상황)
			trackingKey = bus.NodeId
		}
		
		if bt.IsStationChanged(trackingKey, bus.StationId) {
			changedBuses = append(changedBuses, bus)
			if bus.NodeNm != "" {
				logger.Infof("정류장 변경 감지 - 차량번호: %s, 정류장: %s (%d), 순서: %d", 
					bus.PlateNo, bus.NodeNm, bus.StationId, bus.NodeOrd)
			} else {
				logger.Infof("정류장 변경 감지 - 차량번호: %s, 정류장ID: %d, 순서: %d", 
					bus.PlateNo, bus.StationId, bus.NodeOrd)
			}
		}
	}
	
	if len(changedBuses) > 0 {
		logger.Infof("정류장 변경된 버스: %d대 / 전체: %d대", len(changedBuses), len(busLocations))
		
		// 첫 번째 변경된 버스 정보 샘플 로깅
		firstChangedBus := changedBuses[0]
		trackingKey := firstChangedBus.PlateNo
		if trackingKey == "" {
			trackingKey = firstChangedBus.NodeId
		}
		
		lastStation, hasHistory := bt.GetLastStation(trackingKey)
		if hasHistory {
			logger.Infof("첫 번째 변경 버스 - 차량번호: %s, 이전 정류장: %d → 현재 정류장: %d", 
				firstChangedBus.PlateNo, lastStation, firstChangedBus.StationId)
		} else {
			logger.Infof("첫 번째 변경 버스 - 차량번호: %s, 현재 정류장: %d (신규)", 
				firstChangedBus.PlateNo, firstChangedBus.StationId)
		}
	} else {
		logger.Infof("정류장 변경된 버스 없음 (전체: %d대)", len(busLocations))
	}
	
	return changedBuses
}