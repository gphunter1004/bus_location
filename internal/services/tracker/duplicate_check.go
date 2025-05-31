// internal/services/tracker/bus_tracker_duplicate_check.go - 중복 체크 기능
package tracker

import (
	"sync"
	"time"

	"bus-tracker/config"
	"bus-tracker/internal/models"
	"bus-tracker/internal/services/storage"
	"bus-tracker/internal/utils"
)

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
