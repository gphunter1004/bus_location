// internal/services/tracker/simplified_bus_tracker.go - ES tripNumber 로딩 개선 버전
package tracker

import (
	"sync"
	"time"

	"bus-tracker/config"
	"bus-tracker/internal/models"
	"bus-tracker/internal/services/storage"
	"bus-tracker/internal/utils"
)

// SimpleBusInfo 단순화된 버스 정보
type SimpleBusInfo struct {
	PlateNo      string    `json:"plateNo"`
	RouteId      int64     `json:"routeId"`
	LastPosition int64     `json:"lastPosition"`
	LastSeenTime time.Time `json:"lastSeenTime"`
	TripNumber   int       `json:"tripNumber"`
	IsTerminated bool      `json:"isTerminated"`
	FirstSeen    bool      `json:"firstSeen"` // 첫 번째 목격 여부
}

// SimpleBusTracker 단순화된 버스 추적기 (ES tripNumber 로딩 지원)
type SimpleBusTracker struct {
	config              *config.Config
	logger              *utils.Logger
	duplicateChecker    *storage.ElasticsearchDuplicateChecker
	busInfoMap          map[string]*SimpleBusInfo
	mutex               sync.RWMutex
	esTripNumbersLoaded bool
	esTripNumbersCache  map[string]int // ES에서 로딩한 tripNumber 캐시
	esTripNumbersMutex  sync.RWMutex
}

// NewSimpleBusTracker 새로운 단순화된 버스 추적기 생성
func NewSimpleBusTracker(cfg *config.Config, logger *utils.Logger) *SimpleBusTracker {
	return &SimpleBusTracker{
		config:              cfg,
		logger:              logger,
		busInfoMap:          make(map[string]*SimpleBusInfo),
		esTripNumbersLoaded: false,
		esTripNumbersCache:  make(map[string]int),
	}
}

// SetDuplicateChecker ES 중복 체크 서비스 설정
func (sbt *SimpleBusTracker) SetDuplicateChecker(duplicateChecker *storage.ElasticsearchDuplicateChecker) {
	sbt.duplicateChecker = duplicateChecker
	sbt.logger.Info("✅ SimpleBusTracker에 ES 중복 체크 서비스 설정 완료")
}

// loadESTripNumbers ES에서 tripNumber 캐시 로딩 (최초 한번만)
func (sbt *SimpleBusTracker) loadESTripNumbers() {
	sbt.esTripNumbersMutex.Lock()
	defer sbt.esTripNumbersMutex.Unlock()

	if sbt.esTripNumbersLoaded {
		return // 이미 로딩됨
	}

	if sbt.duplicateChecker == nil {
		sbt.logger.Warn("⚠️ ES 중복 체크 서비스 없음 - tripNumber 캐시 로딩 건너뛰기")
		sbt.esTripNumbersLoaded = true
		return
	}

	sbt.logger.Info("🔢 ES에서 tripNumber 캐시 로딩 중... (최초 한번만)")

	// ES에서 최근 30분 데이터 조회
	esData, err := sbt.duplicateChecker.GetRecentBusData(30)
	if err != nil {
		sbt.logger.Errorf("ES tripNumber 로딩 실패: %v", err)
		sbt.esTripNumbersLoaded = true
		return
	}

	// tripNumber 캐시 구성
	loadCount := 0
	for plateNo, data := range esData {
		if data.TripNumber > 0 {
			sbt.esTripNumbersCache[plateNo] = data.TripNumber
			loadCount++
			sbt.logger.Infof("🔢 ES tripNumber 캐시 - %s: T%d", plateNo, data.TripNumber)
		}
	}

	sbt.esTripNumbersLoaded = true
	sbt.logger.Infof("✅ ES tripNumber 캐시 로딩 완료 - %d건", loadCount)
}

// getESTripNumber ES 캐시에서 tripNumber 조회
func (sbt *SimpleBusTracker) getESTripNumber(plateNo string) int {
	sbt.esTripNumbersMutex.RLock()
	defer sbt.esTripNumbersMutex.RUnlock()

	if tripNumber, exists := sbt.esTripNumbersCache[plateNo]; exists {
		return tripNumber
	}
	return 0
}

// FilterChangedBuses 변경된 버스만 필터링 (ES tripNumber 로딩 지원 + 상세 로깅)
func (sbt *SimpleBusTracker) FilterChangedBuses(busLocations []models.BusLocation) []models.BusLocation {
	// 🔢 ES tripNumber 로딩 (최초 한번만)
	if !sbt.esTripNumbersLoaded {
		sbt.loadESTripNumbers()
	}

	var changedBuses []models.BusLocation
	now := time.Now()

	sbt.mutex.Lock()
	defer sbt.mutex.Unlock()

	for _, bus := range busLocations {
		plateNo := bus.PlateNo
		currentPosition := sbt.extractPosition(bus)

		// 기존 정보 확인
		existingInfo, exists := sbt.busInfoMap[plateNo]

		var shouldUpdate bool
		var tripNumber int
		var tripNumberChanged = false

		if !exists {
			// 🆕 새로운 버스 - ES에서 tripNumber 우선 조회
			esTripNumber := sbt.getESTripNumber(plateNo)
			if esTripNumber > 0 {
				tripNumber = esTripNumber
				sbt.logger.Infof("🔢 [TRIPNUMBER_CHANGE] 신규버스 ES적용 - 차량:%s, T0→T%d (소스:ES캐시)", plateNo, tripNumber)
				tripNumberChanged = true
			} else {
				tripNumber = 1 // 기본값
				sbt.logger.Infof("🔢 [TRIPNUMBER_CHANGE] 신규버스 기본값 - 차량:%s, T0→T1 (소스:기본값)", plateNo)
				tripNumberChanged = true
			}
			shouldUpdate = true
		} else {
			// 기존 버스 - 위치 변경 체크
			tripNumber = existingInfo.TripNumber

			if existingInfo.LastPosition != currentPosition {
				shouldUpdate = true
				sbt.logger.Infof("📍 위치 변경 감지 - 차량:%s, T%d, 위치:%d→%d", plateNo, tripNumber, existingInfo.LastPosition, currentPosition)
			} else {
				sbt.logger.Debugf("🔒 위치 동일 - 차량:%s, T%d, 위치:%d", plateNo, tripNumber, currentPosition)
			}
		}

		// 버스 정보 업데이트
		sbt.busInfoMap[plateNo] = &SimpleBusInfo{
			PlateNo:      plateNo,
			RouteId:      bus.RouteId,
			LastPosition: currentPosition,
			LastSeenTime: now,
			TripNumber:   tripNumber,
			IsTerminated: false,
			FirstSeen:    !exists,
		}

		if shouldUpdate {
			// 🔢 tripNumber가 변경된 경우 상세 로깅
			if tripNumberChanged {
				sbt.logger.Infof("🔢 [TRIPNUMBER_ASSIGNED] Tracker→Bus 할당 - 차량:%s, bus.TripNumber=%d", plateNo, tripNumber)
			}

			bus.TripNumber = tripNumber
			changedBuses = append(changedBuses, bus)
		}
	}

	sbt.logger.Infof("📊 Tracker 처리 결과 - 전체: %d건, 변경: %d건", len(busLocations), len(changedBuses))
	return changedBuses
}

// extractPosition 버스의 현재 위치 추출
func (sbt *SimpleBusTracker) extractPosition(bus models.BusLocation) int64 {
	if bus.StationId > 0 {
		return bus.StationId
	}
	if bus.NodeOrd > 0 {
		return int64(bus.NodeOrd)
	}
	if bus.StationSeq > 0 {
		return int64(bus.StationSeq)
	}
	return 0
}

// SetTripNumber 특정 버스의 tripNumber 설정 (상세 로깅)
func (sbt *SimpleBusTracker) SetTripNumber(plateNo string, tripNumber int) {
	sbt.mutex.Lock()
	defer sbt.mutex.Unlock()

	if info, exists := sbt.busInfoMap[plateNo]; exists {
		oldTripNumber := info.TripNumber
		if oldTripNumber != tripNumber {
			info.TripNumber = tripNumber
			sbt.logger.Infof("🔢 [TRIPNUMBER_CHANGE] Tracker 수동설정 - 차량:%s, T%d→T%d (소스:수동설정)", plateNo, oldTripNumber, tripNumber)
		} else {
			sbt.logger.Debugf("🔒 [TRIPNUMBER_SAME] Tracker 동일값 - 차량:%s, T%d (변경없음)", plateNo, tripNumber)
		}
	} else {
		// 새로운 버스 정보 생성
		sbt.busInfoMap[plateNo] = &SimpleBusInfo{
			PlateNo:      plateNo,
			TripNumber:   tripNumber,
			LastSeenTime: time.Now(),
			FirstSeen:    true,
		}
		sbt.logger.Infof("🔢 [TRIPNUMBER_CHANGE] Tracker 신규설정 - 차량:%s, T0→T%d (소스:신규생성)", plateNo, tripNumber)
	}

	// 🔢 ES 캐시에도 반영 (수동 설정된 값 우선 적용)
	sbt.esTripNumbersMutex.Lock()
	oldCacheValue := sbt.esTripNumbersCache[plateNo]
	if oldCacheValue != tripNumber {
		sbt.esTripNumbersCache[plateNo] = tripNumber
		sbt.logger.Infof("🔢 [TRIPNUMBER_CACHE] ES캐시 업데이트 - 차량:%s, cache:%d→%d", plateNo, oldCacheValue, tripNumber)
	}
	sbt.esTripNumbersMutex.Unlock()
}

// GetTripNumber 특정 버스의 tripNumber 조회
func (sbt *SimpleBusTracker) GetTripNumber(plateNo string) int {
	sbt.mutex.RLock()
	defer sbt.mutex.RUnlock()

	if info, exists := sbt.busInfoMap[plateNo]; exists {
		return info.TripNumber
	}

	// 🔢 tracker에 없으면 ES 캐시에서 조회
	return sbt.getESTripNumber(plateNo)
}

// UpdateLastSeenTime 마지막 목격 시간 업데이트
func (sbt *SimpleBusTracker) UpdateLastSeenTime(plateNo string) {
	sbt.mutex.Lock()
	defer sbt.mutex.Unlock()

	if info, exists := sbt.busInfoMap[plateNo]; exists {
		info.LastSeenTime = time.Now()
	}
}

// CleanupMissingBuses 미목격 버스 정리
func (sbt *SimpleBusTracker) CleanupMissingBuses() int {
	sbt.mutex.Lock()
	defer sbt.mutex.Unlock()

	now := time.Now()
	timeout := sbt.config.BusDisappearanceTimeout
	var removedBuses []string

	for plateNo, info := range sbt.busInfoMap {
		if now.Sub(info.LastSeenTime) > timeout {
			removedBuses = append(removedBuses, plateNo)
			delete(sbt.busInfoMap, plateNo)
		}
	}

	if len(removedBuses) > 0 {
		sbt.logger.Infof("🧹 Tracker 미목격 버스 정리 - %d대: %v", len(removedBuses), removedBuses)
	}

	return len(removedBuses)
}

// GetTrackedBusCount 추적 중인 버스 수
func (sbt *SimpleBusTracker) GetTrackedBusCount() int {
	sbt.mutex.RLock()
	defer sbt.mutex.RUnlock()
	return len(sbt.busInfoMap)
}

// GetBusInfo 버스 정보 조회
func (sbt *SimpleBusTracker) GetBusInfo(plateNo string) (*SimpleBusInfo, bool) {
	sbt.mutex.RLock()
	defer sbt.mutex.RUnlock()

	if info, exists := sbt.busInfoMap[plateNo]; exists {
		// 복사본 반환
		infoCopy := *info
		return &infoCopy, true
	}
	return nil, false
}

// PrintStatistics 통계 출력 (ES 캐시 정보 포함)
func (sbt *SimpleBusTracker) PrintStatistics() {
	sbt.mutex.RLock()
	trackedCount := len(sbt.busInfoMap)
	sbt.mutex.RUnlock()

	sbt.esTripNumbersMutex.RLock()
	esCacheCount := len(sbt.esTripNumbersCache)
	sbt.esTripNumbersMutex.RUnlock()

	sbt.logger.Infof("📊 Tracker 통계 - 추적: %d대, ES캐시: %d건", trackedCount, esCacheCount)

	// 상위 5개 버스 정보 출력
	sbt.mutex.RLock()
	count := 0
	for plateNo, info := range sbt.busInfoMap {
		if count < 5 {
			sbt.logger.Infof("   🚌 %s: T%d, 위치=%d, 최종목격=%s",
				plateNo, info.TripNumber, info.LastPosition,
				info.LastSeenTime.Format("15:04:05"))
			count++
		} else {
			break
		}
	}
	sbt.mutex.RUnlock()

	if trackedCount > 5 {
		sbt.logger.Infof("   ... 외 %d대 더", trackedCount-5)
	}
}

// GetESTripNumbersCacheCount ES tripNumber 캐시 개수 반환
func (sbt *SimpleBusTracker) GetESTripNumbersCacheCount() int {
	sbt.esTripNumbersMutex.RLock()
	defer sbt.esTripNumbersMutex.RUnlock()
	return len(sbt.esTripNumbersCache)
}

// ClearESTripNumbersCache ES tripNumber 캐시 초기화
func (sbt *SimpleBusTracker) ClearESTripNumbersCache() {
	sbt.esTripNumbersMutex.Lock()
	defer sbt.esTripNumbersMutex.Unlock()

	oldCount := len(sbt.esTripNumbersCache)
	sbt.esTripNumbersCache = make(map[string]int)
	sbt.esTripNumbersLoaded = false

	sbt.logger.Infof("🗑️ ES tripNumber 캐시 초기화 완료 - %d건 삭제", oldCount)
}

// IncrementTripNumber 특정 버스의 tripNumber 증가 (운행 차수 증가 시 + 상세 로깅)
func (sbt *SimpleBusTracker) IncrementTripNumber(plateNo string, reason string) int {
	sbt.mutex.Lock()
	defer sbt.mutex.Unlock()

	if info, exists := sbt.busInfoMap[plateNo]; exists {
		oldTripNumber := info.TripNumber
		info.TripNumber++
		sbt.logger.Infof("🔢 [TRIPNUMBER_CHANGE] Tracker 증가 - 차량:%s, T%d→T%d (이유:%s)",
			plateNo, oldTripNumber, info.TripNumber, reason)

		// ES 캐시에도 반영
		sbt.esTripNumbersMutex.Lock()
		oldCacheValue := sbt.esTripNumbersCache[plateNo]
		sbt.esTripNumbersCache[plateNo] = info.TripNumber
		sbt.logger.Infof("🔢 [TRIPNUMBER_CACHE] ES캐시 증가반영 - 차량:%s, cache:%d→%d", plateNo, oldCacheValue, info.TripNumber)
		sbt.esTripNumbersMutex.Unlock()

		return info.TripNumber
	}

	// 버스 정보가 없으면 1로 시작
	newTripNumber := 1
	sbt.busInfoMap[plateNo] = &SimpleBusInfo{
		PlateNo:      plateNo,
		TripNumber:   newTripNumber,
		LastSeenTime: time.Now(),
		FirstSeen:    true,
	}

	sbt.logger.Infof("🔢 [TRIPNUMBER_CHANGE] Tracker 신규시작 - 차량:%s, T0→T%d (이유:%s)", plateNo, newTripNumber, reason)

	// ES 캐시에도 추가
	sbt.esTripNumbersMutex.Lock()
	oldCacheValue := sbt.esTripNumbersCache[plateNo]
	sbt.esTripNumbersCache[plateNo] = newTripNumber
	sbt.logger.Infof("🔢 [TRIPNUMBER_CACHE] ES캐시 신규추가 - 차량:%s, cache:%d→%d", plateNo, oldCacheValue, newTripNumber)
	sbt.esTripNumbersMutex.Unlock()

	return newTripNumber
}
