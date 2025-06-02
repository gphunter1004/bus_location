// internal/services/tracker/trip_management.go - 캐시 기반 운행 차수 관리 추가
package tracker

import (
	"time"

	"bus-tracker/config"
)

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
		bt.mutex.Lock()
		for _, info := range bt.busInfoMap {
			if info.IsTerminated {
				info.IsTerminated = false
				info.TripNumber = 0 // 새로운 날이므로 차수도 리셋
			}
		}
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

// 🆕 getNextTripNumberFromCache 캐시 기반 다음 운행 차수 반환
func (bt *BusTracker) getNextTripNumberFromCache(plateNo string) int {
	bt.countersMutex.Lock()
	defer bt.countersMutex.Unlock()

	// 기존 캐시된 값이 있으면 그 다음 차수 반환
	currentTrip := bt.dailyTripCounters[plateNo]
	bt.dailyTripCounters[plateNo] = currentTrip + 1

	return bt.dailyTripCounters[plateNo]
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

// GetBusTripCount 특정 차량의 일일 운행 차수 반환
func (bt *BusTracker) GetBusTripCount(plateNo string) int {
	now := time.Now()

	// 자동 리셋 확인
	bt.checkAndResetDailyCounters(now)

	bt.countersMutex.RLock()
	defer bt.countersMutex.RUnlock()

	return bt.dailyTripCounters[plateNo]
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
