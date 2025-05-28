package services

import (
	"sync"

	"bus-tracker/models"
	"bus-tracker/utils"
)

// BusTracker 버스별 마지막 정류장 정보를 추적하는 서비스
type BusTracker struct {
	lastStationMap map[string]int64 // key: plateNo, value: stationId (API1) 또는 nodeOrd (API2)
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

		// API2의 경우 NodeOrd를 기준으로, API1의 경우 StationId를 기준으로 변경 감지
		var currentPosition int64
		if bus.NodeOrd > 0 {
			// API2 - NodeOrd 기준 (정류장 순서)
			currentPosition = int64(bus.NodeOrd)
		} else {
			// API1 - StationId 기준
			currentPosition = bus.StationId
		}

		if bt.IsStationChanged(trackingKey, currentPosition) {
			changedBuses = append(changedBuses, bus)

			// 로깅 메시지 개선 (현재순서/전체순서 형식)
			if bus.NodeNm != "" && bus.NodeId != "" {
				// 정류소명과 ID가 모두 있는 경우 (API2 또는 캐시 보강된 API1)
				if bus.NodeOrd > 0 {
					// API2 또는 NodeOrd가 설정된 API1
					if bus.TotalStations > 0 {
						logger.Infof("정류장 변경 감지 - 차량번호: %s, 정류장: %s (%s), 순서: %d/%d",
							bus.PlateNo, bus.NodeNm, bus.NodeId, bus.NodeOrd, bus.TotalStations)
					} else {
						logger.Infof("정류장 변경 감지 - 차량번호: %s, 정류장: %s (%s), 순서: %d",
							bus.PlateNo, bus.NodeNm, bus.NodeId, bus.NodeOrd)
					}
				} else {
					// API1에서 NodeOrd가 0인 경우 StationSeq 표시
					if bus.TotalStations > 0 {
						logger.Infof("정류장 변경 감지 - 차량번호: %s, 정류장: %s (%s), Seq: %d/%d",
							bus.PlateNo, bus.NodeNm, bus.NodeId, bus.StationSeq, bus.TotalStations)
					} else {
						logger.Infof("정류장 변경 감지 - 차량번호: %s, 정류장: %s (%s), Seq: %d",
							bus.PlateNo, bus.NodeNm, bus.NodeId, bus.StationSeq)
					}
				}
			} else if bus.NodeNm != "" {
				// 정류소명만 있는 경우
				if bus.NodeOrd > 0 {
					if bus.TotalStations > 0 {
						logger.Infof("정류장 변경 감지 - 차량번호: %s, 정류장: %s (ID: %d), 순서: %d/%d",
							bus.PlateNo, bus.NodeNm, bus.StationId, bus.NodeOrd, bus.TotalStations)
					} else {
						logger.Infof("정류장 변경 감지 - 차량번호: %s, 정류장: %s (ID: %d), 순서: %d",
							bus.PlateNo, bus.NodeNm, bus.StationId, bus.NodeOrd)
					}
				} else {
					if bus.TotalStations > 0 {
						logger.Infof("정류장 변경 감지 - 차량번호: %s, 정류장: %s (ID: %d), Seq: %d/%d",
							bus.PlateNo, bus.NodeNm, bus.StationId, bus.StationSeq, bus.TotalStations)
					} else {
						logger.Infof("정류장 변경 감지 - 차량번호: %s, 정류장: %s (ID: %d), Seq: %d",
							bus.PlateNo, bus.NodeNm, bus.StationId, bus.StationSeq)
					}
				}
			} else if bus.NodeOrd > 0 {
				// API2 - 정류소명이 없는 경우 (캐시 미스)
				if bus.TotalStations > 0 {
					logger.Infof("정류장 변경 감지 - 차량번호: %s, 정류장순서: %d/%d (정류소명 없음)",
						bus.PlateNo, bus.NodeOrd, bus.TotalStations)
				} else {
					logger.Infof("정류장 변경 감지 - 차량번호: %s, 정류장순서: %d (정류소명 없음)",
						bus.PlateNo, bus.NodeOrd)
				}
			} else {
				// API1 - StationId 기준
				if bus.TotalStations > 0 {
					logger.Infof("정류장 변경 감지 - 차량번호: %s, 정류장ID: %d, Seq: %d/%d",
						bus.PlateNo, bus.StationId, bus.StationSeq, bus.TotalStations)
				} else {
					logger.Infof("정류장 변경 감지 - 차량번호: %s, 정류장ID: %d, Seq: %d",
						bus.PlateNo, bus.StationId, bus.StationSeq)
				}
			}
		}
	}

	if len(changedBuses) > 0 {
		logger.Infof("정류장 변경된 버스: %d대 / 전체: %d대", len(changedBuses), len(busLocations))

		// 모든 변경된 버스 정보 상세 로깅
		for i, changedBus := range changedBuses {
			trackingKey := changedBus.PlateNo
			if trackingKey == "" {
				trackingKey = changedBus.NodeId
			}

			var currentPosition int64
			if changedBus.NodeOrd > 0 {
				currentPosition = int64(changedBus.NodeOrd)
			} else {
				currentPosition = changedBus.StationId
			}

			lastPosition, hasHistory := bt.GetLastStation(trackingKey)
			if hasHistory {
				if changedBus.NodeOrd > 0 {
					logger.Infof("변경 버스 %d/%d - 차량번호: %s, 이전 순서: %d → 현재 순서: %d",
						i+1, len(changedBuses), changedBus.PlateNo, lastPosition, currentPosition)
				} else {
					logger.Infof("변경 버스 %d/%d - 차량번호: %s, 이전 정류장: %d → 현재 정류장: %d",
						i+1, len(changedBuses), changedBus.PlateNo, lastPosition, currentPosition)
				}
			} else {
				if changedBus.NodeOrd > 0 {
					logger.Infof("변경 버스 %d/%d - 차량번호: %s, 현재 순서: %d (신규)",
						i+1, len(changedBuses), changedBus.PlateNo, currentPosition)
				} else {
					logger.Infof("변경 버스 %d/%d - 차량번호: %s, 현재 정류장: %d (신규)",
						i+1, len(changedBuses), changedBus.PlateNo, currentPosition)
				}
			}
		}
	} else {
		logger.Infof("정류장 변경된 버스 없음 (전체: %d대)", len(busLocations))
	}

	return changedBuses
}
