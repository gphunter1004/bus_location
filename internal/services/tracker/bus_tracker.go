// internal/services/tracker/bus_tracker.go
package tracker

import (
	"sync"
	"time"

	"bus-tracker/config"
	"bus-tracker/internal/models"
	"bus-tracker/internal/services/storage"
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

// BusTrackerWithDuplicateCheck 중복 체크 기능이 추가된 BusTracker
type BusTrackerWithDuplicateCheck struct {
	*BusTracker
	duplicateChecker *storage.ElasticsearchDuplicateChecker
	isFirstRun       bool
	firstRunMutex    sync.Mutex
	recentESData     map[string]*storage.BusLastData // 첫 실행 시 ES에서 가져온 참고 데이터
}

// NewBusTrackerWithDuplicateCheck 중복 체크 기능이 추가된 BusTracker 생성
func NewBusTrackerWithDuplicateCheck(cfg *config.Config, duplicateChecker *storage.ElasticsearchDuplicateChecker) *BusTrackerWithDuplicateCheck {
	return &BusTrackerWithDuplicateCheck{
		BusTracker:       NewBusTracker(cfg),
		duplicateChecker: duplicateChecker,
		isFirstRun:       true,
		recentESData:     make(map[string]*storage.BusLastData),
	}
}

// getDailyOperatingDate 운영일자 계산 (운영시간 기준)
// 예: 04:55~01:00 운영시간의 경우, 새벽 1:30은 전날 운영일자에 속함
func getDailyOperatingDate(now time.Time, cfg *config.Config) string {
	// 현재 시간이 운영 종료 시간 이후이고 다음 운영 시작 시간 이전이면 전날로 계산
	if !cfg.IsOperatingTime(now) {
		nextOperatingTime := cfg.GetNextOperatingTime(now)

		// 다음 운영 시간이 다음날이면 현재는 전날 운영일자
		if nextOperatingTime.Day() != now.Day() {
			return now.AddDate(0, 0, -1).Format("2006-01-02")
		}
	}

	return now.Format("2006-01-02")
}

// checkAndResetDailyCounters 일일 카운터 리셋 확인
func (bt *BusTracker) checkAndResetDailyCounters(now time.Time) {
	bt.countersMutex.Lock()
	defer bt.countersMutex.Unlock()

	newDate := getDailyOperatingDate(now, bt.config)

	// 운영일자가 변경되었으면 카운터 리셋
	if newDate != bt.currentDate {
		bt.dailyTripCounters = make(map[string]int)
		bt.currentDate = newDate
		bt.lastResetTime = now

		// 기존 추적 정보의 종료 상태도 모두 리셋 (새로운 날 시작)
		//bt.mutex.Lock()
		//for plateNo, info := range bt.busInfoMap {
		//	if info.IsTerminated {
		//		info.IsTerminated = false
		//		info.TripNumber = 0 // 새로운 날이므로 차수도 리셋
		//	}
		//}
		bt.mutex.Unlock()
	}
}

// getNextTripNumber 다음 운행 차수 반환 (일일 기준)
func (bt *BusTracker) getNextTripNumber(plateNo string) int {
	bt.countersMutex.Lock()
	defer bt.countersMutex.Unlock()

	bt.dailyTripCounters[plateNo]++
	return bt.dailyTripCounters[plateNo]
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

// FilterChangedStationsWithDuplicateCheck 중복 체크가 포함된 필터링
func (bt *BusTrackerWithDuplicateCheck) FilterChangedStationsWithDuplicateCheck(busLocations []models.BusLocation, logger *utils.Logger) []models.BusLocation {
	// 첫 실행 시에만 Elasticsearch에서 최근 데이터 조회
	if bt.isFirstRun {
		bt.loadRecentESDataForDuplicateCheck(logger)
	}

	var changedBuses []models.BusLocation

	// 현재 배치에서 발견된 버스들의 마지막 목격 시간 업데이트
	for _, bus := range busLocations {
		bt.BusTracker.UpdateLastSeenTime(bus.PlateNo)
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
		if bt.BusTracker.config.EnableTerminalStop && bt.BusTracker.shouldTerminateAtTerminal(bus.PlateNo, currentPosition, int64(bus.TotalStations)) {
			bt.BusTracker.TerminateBusTracking(bus.PlateNo, "종점 도달", logger)
			// 종료된 버스도 한 번은 ES에 전송 (종료 표시를 위해)
			bus.TripNumber = bt.BusTracker.GetTripNumber(bus.PlateNo)
			changedBuses = append(changedBuses, bus)
			continue
		}

		// 정류장 변경 체크 (첫 실행 시 ES 중복 체크 포함)
		if changed, tripNumber := bt.isStationChangedWithDuplicateCheck(bus.PlateNo, currentPosition, routeNm, bus.TotalStations, bus, logger); changed {
			bus.TripNumber = tripNumber
			changedBuses = append(changedBuses, bus)
		}
	}

	return changedBuses
}

// loadRecentESDataForDuplicateCheck 첫 실행 시 ES에서 최근 데이터 로드 (중복 체크용)
func (bt *BusTrackerWithDuplicateCheck) loadRecentESDataForDuplicateCheck(logger *utils.Logger) {
	bt.firstRunMutex.Lock()
	defer bt.firstRunMutex.Unlock()

	if !bt.isFirstRun {
		return
	}

	if bt.duplicateChecker == nil {
		logger.Warn("중복 체크 서비스가 없어 첫 실행 중복 체크를 건너뜁니다")
		bt.isFirstRun = false
		return
	}

	logger.Info("첫 실행 - Elasticsearch에서 최근 데이터 조회 중...")

	// 최근 30분 내 데이터 조회
	recentData, err := bt.duplicateChecker.GetRecentBusData(30)
	if err != nil {
		logger.Errorf("첫 실행 ES 데이터 조회 실패: %v", err)
		// 실패해도 계속 진행
		bt.isFirstRun = false
		return
	}

	bt.recentESData = recentData
	bt.isFirstRun = false

	if len(recentData) > 0 {
		logger.Infof("첫 실행 중복 체크용 데이터 로드 완료 - %d대 버스", len(recentData))

		// 디버깅을 위한 샘플 출력 (처음 3개)
		count := 0
		for plateNo, data := range recentData {
			if count >= 3 {
				break
			}
			logger.Infof("ES 참고 데이터 - 차량: %s, StationSeq: %d, NodeOrd: %d, NodeId: %s, 시간: %s",
				plateNo, data.StationSeq, data.NodeOrd, data.NodeId, data.LastUpdate.Format("15:04:05"))
			count++
		}
	} else {
		logger.Info("첫 실행 - ES에 최근 데이터 없음")
	}
}

// isStationChangedWithDuplicateCheck 중복 체크를 포함한 정류장 변경 확인
func (bt *BusTrackerWithDuplicateCheck) isStationChangedWithDuplicateCheck(plateNo string, currentPosition int64, routeNm string, totalStations int, bus models.BusLocation, logger *utils.Logger) (bool, int) {
	now := time.Now()

	// 일일 카운터 리셋 확인
	bt.BusTracker.checkAndResetDailyCounters(now)

	bt.BusTracker.mutex.Lock()
	defer bt.BusTracker.mutex.Unlock()

	info, exists := bt.BusTracker.busInfoMap[plateNo]

	if !exists {
		// 새로운 버스인 경우
		// 첫 실행에서만 ES 중복 체크 수행
		if len(bt.recentESData) > 0 {
			if esData, found := bt.recentESData[plateNo]; found {
				// ES에 최근 데이터가 있는 경우 중복 체크
				if esData.IsDuplicateData(bus.StationSeq, bus.NodeOrd, bus.StationId, bus.NodeId) {
					logger.Infof("중복 데이터 감지 (첫 실행) - 차량: %s, 현재위치: StationSeq=%d/NodeOrd=%d, ES최종: %s",
						plateNo, bus.StationSeq, bus.NodeOrd, esData.LastUpdate.Format("15:04:05"))

					// 내부 상태는 업데이트하되 ES 전송은 하지 않음
					tripNumber := bt.BusTracker.getNextTripNumber(plateNo)
					bt.BusTracker.busInfoMap[plateNo] = &BusTrackingInfo{
						LastPosition:  currentPosition,
						LastSeenTime:  now,
						StartPosition: currentPosition,
						RouteNm:       routeNm,
						TotalStations: totalStations,
						IsTerminated:  false,
						TripNumber:    tripNumber,
						TripStartTime: now,
					}
					return false, tripNumber // 중복이므로 ES 전송하지 않음
				} else {
					logger.Infof("새로운 위치 데이터 - 차량: %s, 현재: StationSeq=%d/NodeOrd=%d, ES최종: StationSeq=%d/NodeOrd=%d",
						plateNo, bus.StationSeq, bus.NodeOrd, esData.StationSeq, esData.NodeOrd)
				}
			}
		}

		// 새로운 버스 또는 중복이 아닌 경우 - 일일 운행 차수 할당
		tripNumber := bt.BusTracker.getNextTripNumber(plateNo)
		bt.BusTracker.busInfoMap[plateNo] = &BusTrackingInfo{
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
		tripNumber := bt.BusTracker.getNextTripNumber(plateNo)
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

// shouldTerminateAtTerminal 종점 도착 시 종료
func (bt *BusTracker) shouldTerminateAtTerminal(plateNo string, currentPosition, totalStations int64) bool {
	if totalStations <= 0 {
		return false
	}

	// 마지막 정류장에 도착한 경우 (전체 정류소 수와 동일하거나 그 이상)
	return currentPosition >= totalStations
}

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
					logger.Infof("버스 운행 종료 - 차량번호: %s, 노선: %s, %s %d차수 완료, 이유: %v 미목격",
						plateNo, info.RouteNm, operatingDate, info.TripNumber, timeSinceLastSeen.Round(time.Minute))
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

// ResetDailyTripCounters 일일 운행 차수 카운터 수동 리셋
func (bt *BusTracker) ResetDailyTripCounters() {
	now := time.Now()

	bt.countersMutex.Lock()
	defer bt.countersMutex.Unlock()

	// 일일 차량 운행 차수 카운터 초기화
	bt.dailyTripCounters = make(map[string]int)
	bt.currentDate = getDailyOperatingDate(now, bt.config)
	bt.lastResetTime = now

	// 기존 추적 정보의 종료 상태도 모두 리셋 (새로운 날 시작)
	bt.mutex.Lock()
	for _, info := range bt.busInfoMap {
		if info.IsTerminated {
			info.IsTerminated = false
			info.TripNumber = 0 // 새로운 날이므로 차수도 리셋
		}
	}
	bt.mutex.Unlock()
}

// GetDailyTripStatistics 차량별 일일 운행 차수 통계 반환
func (bt *BusTracker) GetDailyTripStatistics() map[string]int {
	now := time.Now()

	// 자동 리셋 확인
	bt.checkAndResetDailyCounters(now)

	bt.countersMutex.RLock()
	defer bt.countersMutex.RUnlock()

	// 복사본 반환
	stats := make(map[string]int)
	for plateNo, tripCount := range bt.dailyTripCounters {
		stats[plateNo] = tripCount
	}
	return stats
}

// GetBusTripCount 특정 차량의 일일 운행 차수 반환
func (bt *BusTracker) GetBusTripCount(plateNo string) int {
	now := time.Now()

	// 자동 리셋 확인
	bt.checkAndResetDailyCounters(now)

	bt.countersMutex.RLock()
	defer bt.countersMutex.RUnlock()

	return bt.dailyTripCounters[plateNo]
}

// GetCurrentOperatingDate 현재 운영일자 반환
func (bt *BusTracker) GetCurrentOperatingDate() string {
	bt.countersMutex.RLock()
	defer bt.countersMutex.RUnlock()
	return bt.currentDate
}

// GetLastResetTime 마지막 리셋 시간 반환
func (bt *BusTracker) GetLastResetTime() time.Time {
	bt.countersMutex.RLock()
	defer bt.countersMutex.RUnlock()
	return bt.lastResetTime
}

// ForceNewOperatingDay 운영일 강제 변경 (테스트용)
func (bt *BusTracker) ForceNewOperatingDay(newDate string) {
	bt.countersMutex.Lock()
	defer bt.countersMutex.Unlock()

	bt.dailyTripCounters = make(map[string]int)
	bt.currentDate = newDate
	bt.lastResetTime = time.Now()

	// 기존 추적 정보의 종료 상태도 모두 리셋 (새로운 날 시작)
	bt.mutex.Lock()
	for _, info := range bt.busInfoMap {
		if info.IsTerminated {
			info.IsTerminated = false
			info.TripNumber = 0 // 새로운 날이므로 차수도 리셋
		}
	}
	bt.mutex.Unlock()
}
