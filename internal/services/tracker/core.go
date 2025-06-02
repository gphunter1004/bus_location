// internal/services/tracker/core.go - 공통 로직 포함 기본 구현
package tracker

import (
	"fmt"
	"strconv"
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
	RouteId          int64     // 노선 ID (기본 키)
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

// isNearTerminal 종점 인근인지 확인 (전체 정류소 수의 90% 이상)
func (bt *BusTracker) isNearTerminal(position int64, totalStations int) bool {
	if totalStations <= 0 {
		return false
	}

	// 전체 정류소 수의 90% 이상이면 종점 인근으로 간주
	threshold := int64(float64(totalStations) * 0.9)
	if threshold < int64(totalStations-2) {
		threshold = int64(totalStations - 2) // 최소한 마지막 2개 정류소는 종점 인근
	}

	return position >= threshold
}

// shouldSkipTerminalAreaNewTrip 종점 인근에서 새로운 차수 시작 시 스킵 여부 결정
func (bt *BusTracker) shouldSkipTerminalAreaNewTrip(plateNo string, currentPosition int64, totalStations int, logger *utils.Logger) bool {
	info, exists := bt.busInfoMap[plateNo]

	// 기존 추적 정보가 없으면 스킵하지 않음
	if !exists {
		return false
	}

	// 종료되지 않은 버스면 스킵하지 않음
	if !info.IsTerminated {
		return false
	}

	// 현재 위치가 종점 인근이 아니면 스킵하지 않음
	if !bt.isNearTerminal(currentPosition, totalStations) {
		return false
	}

	// 마지막 위치도 종점 인근이었고, 현재도 종점 인근이면 스킵
	if bt.isNearTerminal(info.LastPosition, totalStations) {
		if logger != nil {
			logger.Infof("🚫 종점 인근 차수 시작 스킵 - 차량: %s, 위치: %d/%d (이전: %d)",
				plateNo, currentPosition, totalStations, info.LastPosition)
		}

		// 마지막 목격 시간만 업데이트하고 스킵
		info.LastSeenTime = time.Now()
		return true
	}

	return false
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
		logger.Infof("버스 운행 종료 - 차량번호: %s, 노선ID: %d, %s %d차수 완료, 이유: %s",
			plateNo, info.RouteId, operatingDate, info.TripNumber, reason)
	}

	return true
}

// IsStationChanged 정류장 변경 여부 확인 및 상태 업데이트
// IsStationChanged 정류장 변경 여부 확인 및 상태 업데이트 (수정됨)
func (bt *BusTracker) IsStationChanged(plateNo string, currentPosition int64, cacheKey string, totalStations int) (bool, int) {
	now := time.Now()

	// 일일 카운터 리셋 확인
	bt.checkAndResetDailyCounters(now)

	bt.mutex.Lock()
	defer bt.mutex.Unlock()

	info, exists := bt.busInfoMap[plateNo]

	if !exists {
		// 새로운 버스 - 일일 운행 차수 할당
		tripNumber := bt.getNextTripNumber(plateNo)

		// RouteId 파싱
		var routeId int64
		if parsed, err := strconv.ParseInt(cacheKey, 10, 64); err == nil {
			routeId = parsed
		}

		bt.busInfoMap[plateNo] = &BusTrackingInfo{
			LastPosition:  currentPosition,
			LastSeenTime:  now,
			StartPosition: currentPosition,
			RouteId:       routeId,
			TotalStations: totalStations,
			IsTerminated:  false,
			TripNumber:    tripNumber,
			TripStartTime: now,
		}
		return true, tripNumber
	}

	// 종료된 버스가 다시 나타난 경우 (새로운 운행 시작)
	if info.IsTerminated {
		// 🔧 종점 인근에서 새로운 차수 시작하는 경우 스킵 체크
		if bt.shouldSkipTerminalAreaNewTrip(plateNo, currentPosition, totalStations, nil) {
			return false, info.TripNumber // 데이터 업데이트 하지 않음
		}

		// 🔧 tripNumber 증가 전에 현재 값 확인
		currentTripNumber := info.TripNumber
		nextTripNumber := bt.getNextTripNumber(plateNo)

		// 🔧 실제로 증가했는지 확인
		if nextTripNumber <= currentTripNumber {
			// 카운터가 제대로 증가하지 않았으면 강제 증가
			bt.dailyTripCounters[plateNo] = currentTripNumber + 1
			nextTripNumber = bt.dailyTripCounters[plateNo]
			fmt.Printf("🔧 TripNumber 강제 증가: 차량=%s, %d → %d\n",
				plateNo, currentTripNumber, nextTripNumber)
		}

		info.LastPosition = currentPosition
		info.PreviousPosition = 0
		info.LastSeenTime = now
		info.StartPosition = currentPosition
		info.IsTerminated = false
		info.TripNumber = nextTripNumber
		info.TripStartTime = now

		// RouteId 업데이트
		if parsed, err := strconv.ParseInt(cacheKey, 10, 64); err == nil {
			info.RouteId = parsed
		}

		return true, nextTripNumber
	}

	// 기존 버스 - 변경 확인
	if info.LastPosition != currentPosition {
		info.PreviousPosition = info.LastPosition
		info.LastPosition = currentPosition
		info.LastSeenTime = now
		return true, info.TripNumber // 🔧 기존 tripNumber 유지
	}

	// 위치는 동일하지만 마지막 목격 시간은 업데이트
	info.LastSeenTime = now
	return false, info.TripNumber // 🔧 기존 tripNumber 유지
}

// FilterChangedStations 정류장 변경된 버스만 필터링 (공통 로직)
func (bt *BusTracker) FilterChangedStations(busLocations []models.BusLocation, logger *utils.Logger) []models.BusLocation {
	var changedBuses []models.BusLocation
	var skippedBuses []string

	// 현재 배치에서 발견된 버스들의 마지막 목격 시간 업데이트
	for _, bus := range busLocations {
		bt.UpdateLastSeenTime(bus.PlateNo)
	}

	for _, bus := range busLocations {
		// StationId를 위치 정보로 사용
		currentPosition := bus.StationId
		if currentPosition <= 0 {
			// StationId가 없으면 정류장 순서 사용
			currentPosition = int64(bus.GetStationOrder())
		}

		// 캐시 키 (RouteId 기반)
		cacheKey := bus.GetCacheKey()

		// 🔧 종점 인근 새로운 차수 시작 스킵 체크
		if bt.shouldSkipTerminalAreaNewTrip(bus.PlateNo, currentPosition, bus.TotalStations, logger) {
			skippedBuses = append(skippedBuses, bus.PlateNo)
			continue
		}

		// 종점 도착 시 종료 (config에서 활성화된 경우만)
		if bt.config.EnableTerminalStop && bt.shouldTerminateAtTerminal(bus.PlateNo, currentPosition, int64(bus.TotalStations)) {
			bt.TerminateBusTracking(bus.PlateNo, "종점 도달", logger)
			// 종료된 버스도 한 번은 ES에 전송 (종료 표시를 위해)
			bus.TripNumber = bt.GetTripNumber(bus.PlateNo)
			changedBuses = append(changedBuses, bus)
			continue
		}

		// 정류장 변경 체크
		if changed, tripNumber := bt.IsStationChanged(bus.PlateNo, currentPosition, cacheKey, bus.TotalStations); changed {
			bus.TripNumber = tripNumber
			changedBuses = append(changedBuses, bus)
		}
	}

	// 스킵된 버스가 있으면 로그 출력
	if len(skippedBuses) > 0 {
		logger.Infof("🚫 종점 인근 차수 시작 스킵: %d대 (%v)", len(skippedBuses), skippedBuses)
	}

	return changedBuses
}

// SetTripNumberDirectly 특정 버스의 tripNumber 직접 설정 (공개 메서드)
func (bt *BusTracker) SetTripNumberDirectly(plateNo string, tripNumber int) {
	bt.countersMutex.Lock()
	bt.dailyTripCounters[plateNo] = tripNumber
	bt.countersMutex.Unlock()

	bt.mutex.Lock()
	if info, exists := bt.busInfoMap[plateNo]; exists {
		info.TripNumber = tripNumber
	}
	bt.mutex.Unlock()
}
