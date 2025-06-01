// internal/services/tracker/bus_tracker_cleanup.go - 정리 및 관리 기능
package tracker

import (
	"time"

	"bus-tracker/internal/utils"
)

// CleanupMissingBuses 버스 미목격 시간 초과로 정리
func (bt *BusTracker) CleanupMissingBuses(logger *utils.Logger) int {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()

	var terminatedBuses []string
	var removedBuses []string
	now := time.Now()

	for plateNo, info := range bt.busInfoMap {
		timeSinceLastSeen := now.Sub(info.LastSeenTime)

		// config에서 설정한 시간만큼 미목격시 종료
		if timeSinceLastSeen > bt.config.BusDisappearanceTimeout {
			if !info.IsTerminated {
				// 아직 종료되지 않은 버스는 종료 처리
				info.IsTerminated = true
				terminatedBuses = append(terminatedBuses, plateNo)
				if logger != nil {
					operatingDate := getDailyOperatingDate(now, bt.config)
					logger.Infof("버스 운행 종료 - 차량번호: %s, 노선ID: %d, %s %d차수 완료, 이유: %v 미목격",
						plateNo, info.RouteId, operatingDate, info.TripNumber, timeSinceLastSeen.Round(time.Minute))
				}
			} else {
				// 이미 종료된 버스는 메모리에서 완전 제거 (추가 5분 후)
				if timeSinceLastSeen > bt.config.BusDisappearanceTimeout+5*time.Minute {
					removedBuses = append(removedBuses, plateNo)
				}
			}
		}
	}

	// 메모리에서 완전 제거
	for _, plateNo := range removedBuses {
		delete(bt.busInfoMap, plateNo)
	}

	totalProcessed := len(terminatedBuses) + len(removedBuses)
	if totalProcessed > 0 && logger != nil {
		logger.Infof("버스 정리 완료 - 종료: %d대, 제거: %d대", len(terminatedBuses), len(removedBuses))
	}

	return totalProcessed
}

// UpdateLastSeenTime 마지막 목격 시간 업데이트
func (bt *BusTracker) UpdateLastSeenTime(plateNo string) {
	now := time.Now()

	// 일일 카운터 리셋 확인
	bt.checkAndResetDailyCounters(now)

	bt.mutex.Lock()
	defer bt.mutex.Unlock()

	if info, exists := bt.busInfoMap[plateNo]; exists {
		info.LastSeenTime = now
	}
}

// RemoveFromTracking 추적에서 제거
func (bt *BusTracker) RemoveFromTracking(plateNo string) {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()
	delete(bt.busInfoMap, plateNo)
}

// GetTrackedBusCount 추적 중인 버스 수 반환
func (bt *BusTracker) GetTrackedBusCount() int {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()
	return len(bt.busInfoMap)
}

// GetTripNumber 버스의 운행 차수 반환
func (bt *BusTracker) GetTripNumber(plateNo string) int {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	if info, exists := bt.busInfoMap[plateNo]; exists {
		return info.TripNumber
	}
	return 0
}

// GetBusTrackingInfo 버스 추적 정보 조회
func (bt *BusTracker) GetBusTrackingInfo(plateNo string) (*BusTrackingInfo, bool) {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	if info, exists := bt.busInfoMap[plateNo]; exists {
		infoCopy := *info
		return &infoCopy, true
	}
	return nil, false
}

// GetLastStation 마지막 정류장 반환
func (bt *BusTracker) GetLastStation(plateNo string) (int64, bool) {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	if info, exists := bt.busInfoMap[plateNo]; exists {
		return info.LastPosition, true
	}
	return 0, false
}

// GetPreviousPosition 이전 위치 반환
func (bt *BusTracker) GetPreviousPosition(plateNo string) (int64, bool) {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	if info, exists := bt.busInfoMap[plateNo]; exists && info.PreviousPosition > 0 {
		return info.PreviousPosition, true
	}
	return 0, false
}
