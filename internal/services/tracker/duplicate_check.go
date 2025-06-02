// internal/services/tracker/duplicate_check.go - 운행 차수 캐시 기능 추가
package tracker

import (
	"strconv"
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

// FilterChangedStationsWithDuplicateCheck 중복 체크가 포함된 필터링 - 기본 로직 재사용
func (bt *BusTrackerWithDuplicateCheck) FilterChangedStationsWithDuplicateCheck(busLocations []models.BusLocation, logger *utils.Logger) []models.BusLocation {
	// 첫 실행 시에만 Elasticsearch에서 최근 데이터 조회
	if bt.isFirstRun {
		bt.loadRecentESDataForDuplicateCheck(logger)
	}

	var changedBuses []models.BusLocation
	var skippedBuses []string

	// 현재 배치에서 발견된 버스들의 마지막 목격 시간 업데이트
	for _, bus := range busLocations {
		bt.BusTracker.UpdateLastSeenTime(bus.PlateNo)
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

		// 🔧 종점 인근 새로운 차수 시작 스킵 체크 (기본 로직 재사용)
		if bt.BusTracker.shouldSkipTerminalAreaNewTrip(bus.PlateNo, currentPosition, bus.TotalStations, logger) {
			skippedBuses = append(skippedBuses, bus.PlateNo)
			continue
		}

		// 종점 도착 시 종료 (기본 로직 재사용)
		if bt.BusTracker.config.EnableTerminalStop && bt.BusTracker.shouldTerminateAtTerminal(bus.PlateNo, currentPosition, int64(bus.TotalStations)) {
			bt.BusTracker.TerminateBusTracking(bus.PlateNo, "종점 도달", logger)
			// 종료된 버스도 한 번은 ES에 전송 (종료 표시를 위해)
			bus.TripNumber = bt.BusTracker.GetTripNumber(bus.PlateNo)
			changedBuses = append(changedBuses, bus)
			continue
		}

		// 🔧 정류장 변경 체크 (중복 체크 포함) - 유일한 차이점
		if changed, tripNumber := bt.isStationChangedWithDuplicateCheck(bus.PlateNo, currentPosition, cacheKey, bus.TotalStations, bus, logger); changed {
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

// loadRecentESDataForDuplicateCheck 첫 실행 시 ES에서 최근 데이터 로드 + 운행 차수 캐시 (한번만)
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

	logger.Info("첫 실행 - Elasticsearch에서 최근 데이터 + 운행 차수 조회 중... (최초 한번만)")

	// 최근 30분 내 데이터 조회 (운행 차수 포함) - 최초 실행 시에만
	recentData, err := bt.duplicateChecker.GetRecentBusData(30)
	if err != nil {
		logger.Errorf("첫 실행 ES 데이터 조회 실패: %v", err)
		// 실패해도 계속 진행
		bt.isFirstRun = false
		return
	}

	bt.recentESData = recentData

	// 🔢 운행 차수 캐시 로딩 (최초 한번만)
	if len(recentData) > 0 {
		bt.loadTripNumberCacheOnce(recentData, logger)
	}

	bt.isFirstRun = false

	if len(recentData) > 0 {
		logger.Infof("첫 실행 중복 체크 + 운행 차수 캐시 로드 완료 - %d대 버스 (이후 캐시 기반 운영)", len(recentData))

		// 디버깅을 위한 샘플 출력 (처음 3개)
		count := 0
		for plateNo, data := range recentData {
			if count >= 3 {
				break
			}
			logger.Infof("ES 참고 데이터 - 차량: %s, StationSeq: %d, NodeOrd: %d, TripNumber: %d, 시간: %s",
				plateNo, data.StationSeq, data.NodeOrd, data.TripNumber, data.LastUpdate.Format("15:04:05"))
			count++
		}
	} else {
		logger.Info("첫 실행 - ES에 최근 데이터 없음, 캐시 없이 시작")
	}
}

// 🔢 loadTripNumberCacheOnce ES 데이터에서 운행 차수 캐시 로딩 (최초 한번만)
func (bt *BusTrackerWithDuplicateCheck) loadTripNumberCacheOnce(recentData map[string]*storage.BusLastData, logger *utils.Logger) {
	bt.BusTracker.countersMutex.Lock()
	defer bt.BusTracker.countersMutex.Unlock()

	tripCacheCount := 0
	maxTripStats := make(map[string]int) // 통계용

	for plateNo, data := range recentData {
		if data.TripNumber > 0 {
			// ES에서 가져온 값으로 캐시 초기화 (기존 값 무시)
			bt.BusTracker.dailyTripCounters[plateNo] = data.TripNumber
			tripCacheCount++
			maxTripStats[plateNo] = data.TripNumber
		}
	}

	if tripCacheCount > 0 {
		logger.Infof("🔢 운행 차수 캐시 초기화 완료 - %d대 버스 (최근 ES 데이터 기반, 이후 캐시 운영)", tripCacheCount)

		// 상위 5개 차량의 운행 차수 출력
		count := 0
		for plateNo, tripNum := range maxTripStats {
			if count < 5 {
				logger.Infof("   📋 차량: %s -> T%d (캐시 초기화)", plateNo, tripNum)
				count++
			} else if count == 5 {
				logger.Infof("   📋 ... 외 %d대 더", len(maxTripStats)-5)
				break
			}
		}
	} else {
		logger.Info("🔢 운행 차수 캐시 초기화 - ES에 운행 차수 데이터 없음")
	}
}

// isStationChangedWithDuplicateCheck 중복 체크를 포함한 정류장 변경 확인 (기본 로직 + 중복 체크)
func (bt *BusTrackerWithDuplicateCheck) isStationChangedWithDuplicateCheck(plateNo string, currentPosition int64, cacheKey string, totalStations int, bus models.BusLocation, logger *utils.Logger) (bool, int) {
	now := time.Now()

	// 일일 카운터 리셋 확인 (기본 로직 재사용)
	bt.BusTracker.checkAndResetDailyCounters(now)

	bt.BusTracker.mutex.Lock()
	defer bt.BusTracker.mutex.Unlock()

	info, exists := bt.BusTracker.busInfoMap[plateNo]

	if !exists {
		// 새로운 버스인 경우 - 🔧 중복 체크 로직만 추가
		// 첫 실행에서만 ES 중복 체크 수행
		if len(bt.recentESData) > 0 {
			if esData, found := bt.recentESData[plateNo]; found {
				// ES에 최근 데이터가 있는 경우 중복 체크
				if esData.IsDuplicateData(bus.StationSeq, bus.NodeOrd, bus.StationId, bus.NodeId) {
					logger.Infof("중복 데이터 감지 (첫 실행) - 차량: %s, 현재위치: StationSeq=%d/NodeOrd=%d, ES최종: %s",
						plateNo, bus.StationSeq, bus.NodeOrd, esData.LastUpdate.Format("15:04:05"))

					// 🔢 중복 감지 시 ES에서 불러온 운행 차수 그대로 사용 (증가시키지 않음)
					tripNumber := esData.TripNumber

					// RouteId 파싱
					var routeId int64
					if parsed, err := strconv.ParseInt(cacheKey, 10, 64); err == nil {
						routeId = parsed
					}

					bt.BusTracker.busInfoMap[plateNo] = &BusTrackingInfo{
						LastPosition:  currentPosition,
						LastSeenTime:  now,
						StartPosition: currentPosition,
						RouteId:       routeId,
						TotalStations: totalStations,
						IsTerminated:  false,
						TripNumber:    tripNumber, // ES 원본 차수 그대로
						TripStartTime: now,
					}
					return false, tripNumber // 중복이므로 ES 전송하지 않음
				} else {
					logger.Infof("새로운 위치 데이터 - 차량: %s, 현재: StationSeq=%d/NodeOrd=%d, ES최종: StationSeq=%d/NodeOrd=%d",
						plateNo, bus.StationSeq, bus.NodeOrd, esData.StationSeq, esData.NodeOrd)
				}
			}
		}

		// 🔧 새로운 버스 처리 (캐시 기반 운행 차수)
		tripNumber := bt.BusTracker.getNextTripNumberFromCache(plateNo)

		// RouteId 파싱
		var routeId int64
		if parsed, err := strconv.ParseInt(cacheKey, 10, 64); err == nil {
			routeId = parsed
		}

		bt.BusTracker.busInfoMap[plateNo] = &BusTrackingInfo{
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

	// 🔧 종료된 버스가 다시 나타난 경우 - 기본 로직과 동일 (종점 인근 스킵 포함)
	if info.IsTerminated {
		// 종점 인근에서 새로운 차수 시작하는 경우 스킵 체크 (기본 로직 재사용)
		if bt.BusTracker.shouldSkipTerminalAreaNewTrip(plateNo, currentPosition, totalStations, logger) {
			return false, info.TripNumber // 데이터 업데이트 하지 않음
		}

		// 🆕 캐시 기반 다음 운행 차수 할당
		tripNumber := bt.BusTracker.getNextTripNumberFromCache(plateNo)
		info.LastPosition = currentPosition
		info.PreviousPosition = 0
		info.LastSeenTime = now
		info.StartPosition = currentPosition
		info.IsTerminated = false
		info.TripNumber = tripNumber
		info.TripStartTime = now

		// RouteId 업데이트
		if parsed, err := strconv.ParseInt(cacheKey, 10, 64); err == nil {
			info.RouteId = parsed
		}

		return true, tripNumber
	}

	// 🔧 기존 버스 변경 확인 - 기본 로직과 완전 동일
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
