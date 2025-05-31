// internal/services/tracker/bus_tracker_core.go - 핵심 추적 기능
package tracker

import (
	"sync"
	"time"

	"bus-tracker/config"
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
	TripStartTime    time.Time // 해당 차수 시작 시간
}

// BusTracker 버스별 마지막 정류장 정보를 추적하는 서비스
type BusTracker struct {
	config            *config.Config
	busInfoMap        map[string]*BusTrackingInfo // key: plateNo, value: 추적 정보
	mutex             sync.RWMutex
	dailyTripCounters map[string]int // 차량별 일일 운행 차수 카운터 (key: plateNo)
	countersMutex     sync.RWMutex
	currentDate       string    // 현재 운영일자 (YYYY-MM-DD 형식)
	lastResetTime     time.Time // 마지막 리셋 시간
}

// NewBusTracker 새로운 BusTracker 생성
func NewBusTracker(cfg *config.Config) *BusTracker {
	now := time.Now()
	currentDate := getDailyOperatingDate(now, cfg)

	return &BusTracker{
		config:            cfg,
		busInfoMap:        make(map[string]*BusTrackingInfo),
		dailyTripCounters: make(map[string]int),
		currentDate:       currentDate,
		lastResetTime:     now,
	}
}

// IsStationChanged 정류장 변경 여부 확인 및 상태 업데이트
func (bt *BusTracker) IsStationChanged(plateNo string, currentPosition int64, routeNm string, totalStations int) (bool, int) {
	now := time.Now()

	// 일일 카운터 리셋 확인
	bt.checkAndResetDailyCounters(now)

	bt.mutex.Lock()
	defer bt.mutex.Unlock()

	info, exists := bt.busInfoMap[plateNo]

	if !exists {
		// 새로운 버스 - 일일 운행 차수 할당
		tripNumber := bt.getNextTripNumber(plateNo)

		bt.busInfoMap[plateNo] = &BusTrackingInfo{
			LastPosition:  currentPosition,
			LastSeenTime:  now,
			StartPosition: currentPosition,
			RouteNm:       routeNm,
			TotalStations: totalStations,
			IsTerminated:  false,
			TripNumber:    tripNumber,
			TripStartTime: now,
		}
		return true, tripNumber
	}

	// 종료된 버스가 다시 나타난 경우 (새로운 운행 시작)
	if info.IsTerminated {
		tripNumber := bt.getNextTripNumber(plateNo)
		info.LastPosition = currentPosition
		info.PreviousPosition = 0
		info.LastSeenTime = now
		info.StartPosition = currentPosition
		info.IsTerminated = false
		info.TripNumber = tripNumber
		info.TripStartTime = now
		info.RouteNm = routeNm
		return true, tripNumber
	}

	// 기존 버스 - 변경 확인
	if info.LastPosition != currentPosition {
		info.PreviousPosition = info.LastPosition
		info.LastPosition = currentPosition
		info.LastSeenTime = now
		return true, info.TripNumber
	}

	// 위치는 동일하지만 마지막 목격 시간은 업데이트
	info.LastSeenTime = now
	return false, info.TripNumber
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

		// 종점 도착 시 종료 (config에서 활성화된 경우만)
		if bt.config.EnableTerminalStop && bt.shouldTerminateAtTerminal(bus.PlateNo, currentPosition, int64(bus.TotalStations)) {
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

// shouldTerminateAtTerminal 종점 도착 시 종료
func (bt *BusTracker) shouldTerminateAtTerminal(plateNo string, currentPosition, totalStations int64) bool {
	if totalStations <= 0 {
		return false
	}

	// 마지막 정류장에 도착한 경우 (전체 정류소 수와 동일하거나 그 이상)
	return currentPosition >= totalStations
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
		operatingDate := getDailyOperatingDate(time.Now(), bt.config)
		logger.Infof("버스 운행 종료 - 차량번호: %s, 노선: %s, %s %d차수 완료, 이유: %s",
			plateNo, info.RouteNm, operatingDate, info.TripNumber, reason)
	}

	return true
}
