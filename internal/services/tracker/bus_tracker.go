package tracker

import (
	"sync"
	"time"

	"bus-tracker/internal/models"
	"bus-tracker/internal/utils"
)

// BusTrackingInfo 버스 추적 정보
type BusTrackingInfo struct {
	LastPosition     int64     // 마지막 위치
	PreviousPosition int64     // 이전 위치
	LastSeenTime     time.Time // 마지막 목격 시간
	StartPosition    int64     // 시작 위치
	RouteId          int64     // 노선 ID (추적 정보에 포함)
	RouteNm          string    // 노선번호 (문자열)
	TotalStations    int       // 전체 정류소 개수
	IsTerminated     bool      // 종료 상태 플래그
	TripNumber       int       // 운행 차수
}

// BusTracker 버스별 마지막 정류장 정보를 추적하는 서비스
type BusTracker struct {
	busInfoMap    map[string]*BusTrackingInfo // key: plateNo, value: 추적 정보
	mutex         sync.RWMutex
	tripCounters  map[string]int // 차량별 운행 차수 카운터 (key: plateNo)
	countersMutex sync.RWMutex
}

// NewBusTracker 새로운 BusTracker 생성
func NewBusTracker() *BusTracker {
	return &BusTracker{
		busInfoMap:   make(map[string]*BusTrackingInfo),
		tripCounters: make(map[string]int),
	}
}

// IsStationChanged 정류장 변경 여부 확인 및 상태 업데이트
func (bt *BusTracker) IsStationChanged(plateNo string, currentPosition int64, routeNm string, totalStations int) (bool, int) {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()

	info, exists := bt.busInfoMap[plateNo]

	if !exists {
		// 새로운 버스 - 운행 차수 할당 (해당 차량의 첫 번째 운행)
		tripNumber := bt.getNextTripNumber(plateNo)

		bt.busInfoMap[plateNo] = &BusTrackingInfo{
			LastPosition:  currentPosition,
			LastSeenTime:  time.Now(),
			StartPosition: currentPosition,
			RouteNm:       routeNm,
			TotalStations: totalStations,
			IsTerminated:  false,
			TripNumber:    tripNumber,
		}
		return true, tripNumber
	}

	// 종료된 버스가 다시 나타난 경우 (새로운 운행 시작)
	if info.IsTerminated {
		// 종점 근처에서 계속 나타나는 경우 재시작하지 않음
		if bt.isNearTerminal(currentPosition, totalStations) &&
			bt.isNearTerminal(info.LastPosition, totalStations) {
			// 종점 근처에서 계속 데이터가 오는 경우 무시
			info.LastSeenTime = time.Now()
			return false, info.TripNumber
		}

		// 실제 새로운 운행 시작 (종점에서 멀리 떨어진 곳에서 시작)
		if !bt.isNearTerminal(currentPosition, totalStations) {
			// 새로운 운행 차수 할당 (동일 차량의 다음 운행)
			tripNumber := bt.getNextTripNumber(plateNo)
			info.LastPosition = currentPosition
			info.PreviousPosition = 0
			info.LastSeenTime = time.Now()
			info.StartPosition = currentPosition
			info.IsTerminated = false
			info.TripNumber = tripNumber
			info.RouteNm = routeNm // 노선이 바뀔 수도 있으므로 업데이트
			return true, tripNumber
		}

		// 종점 근처에서의 데이터는 무시
		info.LastSeenTime = time.Now()
		return false, info.TripNumber
	}

	// 기존 버스 - 변경 확인
	if info.LastPosition != currentPosition {
		info.PreviousPosition = info.LastPosition
		info.LastPosition = currentPosition
		info.LastSeenTime = time.Now()
		return true, info.TripNumber
	}

	// 위치는 동일하지만 마지막 목격 시간은 업데이트
	info.LastSeenTime = time.Now()
	return false, info.TripNumber
}

// isNearTerminal 종점 근처인지 확인 (전체 정류소의 90% 이상)
func (bt *BusTracker) isNearTerminal(position int64, totalStations int) bool {
	if totalStations <= 0 {
		return false
	}
	threshold := int64(float64(totalStations) * 0.9)
	return position >= threshold
}

// getNextTripNumber 다음 운행 차수 반환 (차량별 관리)
func (bt *BusTracker) getNextTripNumber(plateNo string) int {
	bt.countersMutex.Lock()
	defer bt.countersMutex.Unlock()

	bt.tripCounters[plateNo]++
	return bt.tripCounters[plateNo]
}

// TerminateBusTracking 버스 운행 종료 처리
func (bt *BusTracker) TerminateBusTracking(plateNo string, reason string, logger *utils.Logger) bool {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()

	info, exists := bt.busInfoMap[plateNo]
	if !exists || info.IsTerminated {
		return false
	}

	// 종료 상태로 마킹
	info.IsTerminated = true

	if logger != nil {
		logger.Infof("버스 운행 종료 - 차량번호: %s, 노선: %s, %d차수 완료, 이유: %s",
			plateNo, info.RouteNm, info.TripNumber, reason)
	}

	return true
}

// FilterChangedStations 정류장 변경된 버스만 필터링
func (bt *BusTracker) FilterChangedStations(busLocations []models.BusLocation, logger *utils.Logger) []models.BusLocation {
	var changedBuses []models.BusLocation

	// 현재 배치에서 발견된 버스들의 마지막 목격 시간 업데이트
	for _, bus := range busLocations {
		bt.UpdateLastSeenTime(bus.PlateNo)
	}

	for _, bus := range busLocations {
		// StationId를 위치 정보로 사용
		currentPosition := bus.StationId
		if currentPosition <= 0 {
			// StationId가 없으면 NodeOrd 사용
			currentPosition = int64(bus.NodeOrd)
		}

		routeNm := bus.GetRouteIDString() // RouteNm 우선, 없으면 RouteId

		// 종료 조건 체크
		if bt.shouldTerminateBus(bus.PlateNo, currentPosition, int64(bus.TotalStations)) {
			bt.TerminateBusTracking(bus.PlateNo, "종점 도달", logger)
			// 종료된 버스도 한 번은 ES에 전송 (종료 표시를 위해)
			bus.TripNumber = bt.GetTripNumber(bus.PlateNo)
			changedBuses = append(changedBuses, bus)
			continue
		}

		// 정류장 변경 체크
		if changed, tripNumber := bt.IsStationChanged(bus.PlateNo, currentPosition, routeNm, bus.TotalStations); changed {
			bus.TripNumber = tripNumber
			changedBuses = append(changedBuses, bus)
		}
	}

	return changedBuses
}

// ShouldTerminateBus 버스 종료 조건 체크 (public 메서드)
func (bt *BusTracker) ShouldTerminateBus(plateNo string, currentPosition, totalStations int64) bool {
	return bt.shouldTerminateBus(plateNo, currentPosition, totalStations)
}

// shouldTerminateBus 버스 종료 조건 체크 (internal)
func (bt *BusTracker) shouldTerminateBus(plateNo string, currentPosition, totalStations int64) bool {
	bt.mutex.RLock()
	info, exists := bt.busInfoMap[plateNo]
	bt.mutex.RUnlock()

	if !exists || info.IsTerminated {
		return false
	}

	// 종점 도달 (전체 정류소의 95% 이상)
	if totalStations > 0 && currentPosition >= (totalStations-2) {
		return true
	}

	return false
}

// CleanupMissingBuses 일정 시간 동안 보이지 않은 버스들을 정리
func (bt *BusTracker) CleanupMissingBuses(timeout time.Duration, logger *utils.Logger) int {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()

	var removedBuses []string
	var terminatedBuses []string
	now := time.Now()

	for plateNo, info := range bt.busInfoMap {
		timeSinceLastSeen := now.Sub(info.LastSeenTime)

		if timeSinceLastSeen > timeout {
			if info.IsTerminated && timeSinceLastSeen > 5*time.Minute {
				removedBuses = append(removedBuses, plateNo)
			} else if !info.IsTerminated {
				info.IsTerminated = true
				terminatedBuses = append(terminatedBuses, plateNo)
			} else {
				removedBuses = append(removedBuses, plateNo)
			}
		}
	}

	// 실제 삭제 실행
	for _, plateNo := range removedBuses {
		delete(bt.busInfoMap, plateNo)
	}

	return len(terminatedBuses) + len(removedBuses)
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

// UpdateLastSeenTime 마지막 목격 시간 업데이트
func (bt *BusTracker) UpdateLastSeenTime(plateNo string) {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()

	if info, exists := bt.busInfoMap[plateNo]; exists {
		info.LastSeenTime = time.Now()
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

// ResetTripCounters 운행 차수 카운터 초기화 (일일 운영시간 시작 시 호출)
func (bt *BusTracker) ResetTripCounters() {
	bt.countersMutex.Lock()
	defer bt.countersMutex.Unlock()

	// 전체 차량의 운행 차수 카운터 초기화 (새로운 날 시작)
	bt.tripCounters = make(map[string]int)
}

// GetDailyTripStatistics 차량별 일일 운행 차수 통계 반환
func (bt *BusTracker) GetDailyTripStatistics() map[string]int {
	bt.countersMutex.RLock()
	defer bt.countersMutex.RUnlock()

	// 복사본 반환
	stats := make(map[string]int)
	for plateNo, tripCount := range bt.tripCounters {
		stats[plateNo] = tripCount
	}
	return stats
}

// GetBusTripCount 특정 차량의 일일 운행 차수 반환
func (bt *BusTracker) GetBusTripCount(plateNo string) int {
	bt.countersMutex.RLock()
	defer bt.countersMutex.RUnlock()

	return bt.tripCounters[plateNo]
}
