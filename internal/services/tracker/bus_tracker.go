// services/bus_tracker.go - 종료 처리 개선 버전

package tracker

import (
	"fmt"
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
	StartPosition    int64     // 시작 위치 (구간 운행 감지용)
	MaxPosition      int64     // 최대 도달 위치
	MinPosition      int64     // 최소 위치 (역방향 감지용)
	IsPartialRoute   bool      // 구간 운행 여부
	RouteId          int64     // 노선 ID (추적 정보에 포함)
	TotalStations    int       // 전체 정류소 개수
	IsTerminated     bool      // 종료 상태 플래그 추가
}

// BusTracker 버스별 마지막 정류장 정보를 추적하는 서비스
type BusTracker struct {
	busInfoMap map[string]*BusTrackingInfo // key: plateNo, value: 추적 정보
	mutex      sync.RWMutex
}

// NewBusTracker 새로운 BusTracker 생성
func NewBusTracker() *BusTracker {
	return &BusTracker{
		busInfoMap: make(map[string]*BusTrackingInfo),
	}
}

// IsStationChanged 정류장 변경 여부 확인 및 상태 업데이트 (종료 처리 개선)
func (bt *BusTracker) IsStationChanged(plateNo string, currentPosition int64) bool {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()

	info, exists := bt.busInfoMap[plateNo]

	if !exists {
		// 새로운 버스 - 추적 정보 생성
		bt.busInfoMap[plateNo] = &BusTrackingInfo{
			LastPosition:     currentPosition,
			PreviousPosition: 0,
			LastSeenTime:     time.Now(),
			StartPosition:    currentPosition,
			MaxPosition:      currentPosition,
			MinPosition:      currentPosition,
			IsPartialRoute:   false,
			IsTerminated:     false, // 새로 시작하는 버스
		}
		return true
	}

	// 🔧 종료된 버스가 다시 나타난 경우 - 새로운 운행으로 재시작
	if info.IsTerminated {
		bt.resetBusTracking(plateNo, currentPosition, "재운행 시작")
		return true
	}

	// 🔧 새로운 seq가 이전보다 훨씬 작은 경우 (새로운 운행 시작으로 판단)
	if info.LastPosition > 0 && currentPosition < (info.LastPosition-20) {
		bt.resetBusTracking(plateNo, currentPosition, "새 운행 감지 (큰 역순)")
		return true
	}

	// 기존 버스 - 변경 확인
	if info.LastPosition != currentPosition {
		info.PreviousPosition = info.LastPosition
		info.LastPosition = currentPosition
		info.LastSeenTime = time.Now()

		// 최대/최소 위치 업데이트
		if currentPosition > info.MaxPosition {
			info.MaxPosition = currentPosition
		}
		if currentPosition < info.MinPosition {
			info.MinPosition = currentPosition
		}

		// 구간 운행 패턴 감지
		bt.detectPartialRoutePattern(plateNo, info)

		return true
	}

	// 위치는 동일하지만 마지막 목격 시간은 업데이트
	info.LastSeenTime = time.Now()
	return false
}

// resetBusTracking 버스 추적 정보 리셋 (새로운 운행 시작)
func (bt *BusTracker) resetBusTracking(plateNo string, newPosition int64, reason string) {
	info := bt.busInfoMap[plateNo]
	oldPosition := info.LastPosition

	// 추적 정보 초기화
	info.LastPosition = newPosition
	info.PreviousPosition = 0
	info.LastSeenTime = time.Now()
	info.StartPosition = newPosition
	info.MaxPosition = newPosition
	info.MinPosition = newPosition
	info.IsPartialRoute = false
	info.IsTerminated = false

	// 로깅
	fmt.Printf("🔄 버스 추적 초기화 - 차량번호: %s, 이유: %s, %d → %d\n",
		plateNo, reason, oldPosition, newPosition)
}

// TerminateBusTracking 버스 운행 종료 처리 (명시적 종료)
func (bt *BusTracker) TerminateBusTracking(plateNo string, reason string, logger *utils.Logger) bool {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()

	info, exists := bt.busInfoMap[plateNo]
	if !exists {
		return false
	}

	// 이미 종료된 버스인지 확인
	if info.IsTerminated {
		return false
	}

	// 종료 상태로 마킹
	info.IsTerminated = true

	routeType := "전체 운행"
	if info.IsPartialRoute {
		routeType = "구간 운행"
	}

	logger.Infof("🏁 %s 종료 - 차량번호: %s, 이유: %s, 운행구간: %d→%d (최대: %d), 지속시간: %.1f분",
		routeType, plateNo, reason, info.StartPosition, info.LastPosition, info.MaxPosition,
		time.Since(info.LastSeenTime).Minutes())

	// 🔧 중요: 즉시 삭제하지 않고 종료 상태만 마킹
	// 잠시 후 다시 나타날 수 있으므로 일정 시간 후 정리
	return true
}

// FilterChangedStations 정류장 변경된 버스만 필터링 (종료 처리 개선)
func (bt *BusTracker) FilterChangedStations(busLocations []models.BusLocation, logger *utils.Logger) []models.BusLocation {
	var changedBuses []models.BusLocation

	// 현재 배치에서 발견된 버스들의 마지막 목격 시간 업데이트
	for _, bus := range busLocations {
		trackingKey := bus.PlateNo
		if trackingKey == "" {
			trackingKey = bus.NodeId
		}
		bt.UpdateLastSeenTime(trackingKey)
	}

	for _, bus := range busLocations {
		trackingKey := bus.PlateNo
		if trackingKey == "" {
			trackingKey = bus.NodeId
		}

		var currentPosition int64
		if bus.NodeOrd > 0 {
			currentPosition = int64(bus.NodeOrd)
		} else {
			currentPosition = bus.StationId
		}

		// 🔧 종료 조건 체크 (개선된 로직)
		if bt.shouldTerminateBus(trackingKey, currentPosition, int64(bus.TotalStations)) {
			bt.TerminateBusTracking(trackingKey, "종점 도달", logger)
			changedBuses = append(changedBuses, bus)
			continue
		}

		// 기존 변경 감지 로직
		if bt.IsStationChanged(trackingKey, currentPosition) {
			changedBuses = append(changedBuses, bus)

			// 추적 정보 조회
			info, _ := bt.GetBusTrackingInfo(trackingKey)
			routeTypeInfo := ""
			if info != nil {
				if info.IsPartialRoute {
					routeTypeInfo = fmt.Sprintf(" [구간운행: %d→%d]", info.StartPosition, info.MaxPosition)
				}
				// 🔧 종료 상태 표시
				if info.IsTerminated {
					routeTypeInfo += " [종료됨]"
				}
			}

			// 로깅 (기존 로직 + 종료 상태 정보 추가)
			if bus.NodeNm != "" && bus.NodeId != "" {
				if bus.NodeOrd > 0 {
					if bus.TotalStations > 0 {
						logger.Infof("정류장 변경 감지 - 차량번호: %s, 정류장: %s (%s), 순서: %d/%d%s",
							bus.PlateNo, bus.NodeNm, bus.NodeId, bus.NodeOrd, bus.TotalStations, routeTypeInfo)
					} else {
						logger.Infof("정류장 변경 감지 - 차량번호: %s, 정류장: %s (%s), 순서: %d%s",
							bus.PlateNo, bus.NodeNm, bus.NodeId, bus.NodeOrd, routeTypeInfo)
					}
				} else {
					if bus.TotalStations > 0 {
						logger.Infof("정류장 변경 감지 - 차량번호: %s, 정류장: %s (%s), Seq: %d/%d%s",
							bus.PlateNo, bus.NodeNm, bus.NodeId, bus.StationSeq, bus.TotalStations, routeTypeInfo)
					} else {
						logger.Infof("정류장 변경 감지 - 차량번호: %s, 정류장: %s (%s), Seq: %d%s",
							bus.PlateNo, bus.NodeNm, bus.NodeId, bus.StationSeq, routeTypeInfo)
					}
				}
			} else if bus.NodeOrd > 0 {
				if bus.TotalStations > 0 {
					logger.Infof("정류장 변경 감지 - 차량번호: %s, 정류장순서: %d/%d%s",
						bus.PlateNo, bus.NodeOrd, bus.TotalStations, routeTypeInfo)
				} else {
					logger.Infof("정류장 변경 감지 - 차량번호: %s, 정류장순서: %d%s",
						bus.PlateNo, bus.NodeOrd, routeTypeInfo)
				}
			} else {
				if bus.TotalStations > 0 {
					logger.Infof("정류장 변경 감지 - 차량번호: %s, 정류장ID: %d, Seq: %d/%d%s",
						bus.PlateNo, bus.StationId, bus.StationSeq, bus.TotalStations, routeTypeInfo)
				} else {
					logger.Infof("정류장 변경 감지 - 차량번호: %s, 정류장ID: %d, Seq: %d%s",
						bus.PlateNo, bus.StationId, bus.StationSeq, routeTypeInfo)
				}
			}
		}
	}

	// 기존 상세 로깅 로직
	if len(changedBuses) > 0 {
		logger.Infof("정류장 변경된 버스: %d대 / 전체: %d대", len(changedBuses), len(busLocations))
	} else {
		logger.Infof("정류장 변경된 버스 없음 (전체: %d대)", len(busLocations))
	}

	return changedBuses
}

// shouldTerminateBus 버스 종료 조건 체크 (통합된 로직)
func (bt *BusTracker) shouldTerminateBus(plateNo string, currentPosition, totalStations int64) bool {
	bt.mutex.RLock()
	info, exists := bt.busInfoMap[plateNo]
	bt.mutex.RUnlock()

	if !exists || info.IsTerminated {
		return false
	}

	// 1. 일반적인 종점 도달 (전체 정류소의 95% 이상)
	if totalStations > 0 && currentPosition >= (totalStations-2) {
		return true
	}

	// 2. 구간 운행별 종료 조건
	if info.IsPartialRoute {
		return bt.checkPartialRouteTermination(info, currentPosition, totalStations)
	}

	// 3. 전체 운행의 경우 마지막 몇 개 정류장에서 종료
	if totalStations > 0 && currentPosition >= (totalStations-1) {
		return true
	}

	return false
}

// checkPartialRouteTermination 구간 운행 종료 조건 체크
func (bt *BusTracker) checkPartialRouteTermination(info *BusTrackingInfo, currentPosition, totalStations int64) bool {
	// 시간대별 구간 운행 패턴 (기존 로직 유지하되 더 정확하게)

	// 일반적인 종점 도달
	if totalStations > 0 && currentPosition >= (totalStations-1) {
		return true
	}

	// 아침 구간 운행 패턴 (예: 10-35번)
	if bt.isMorningPeakTime() && currentPosition >= 35 && info.StartPosition >= 8 {
		return true
	}

	// 저녁 구간 운행 패턴 (예: 30-59번)
	if bt.isEveningPeakTime() && currentPosition >= 55 && info.StartPosition >= 25 {
		return true
	}

	// 심야 구간 운행 패턴 (예: 1-20번)
	if bt.isLateNightTime() && currentPosition >= 20 && info.StartPosition <= 5 {
		return true
	}

	// 점심시간 구간 운행 패턴 (예: 15-45번)
	if bt.isLunchTime() && currentPosition >= 45 && info.StartPosition >= 10 && info.StartPosition <= 20 {
		return true
	}

	return false
}

// CleanupMissingBuses 일정 시간 동안 보이지 않은 버스들을 정리 (종료 처리 개선)
func (bt *BusTracker) CleanupMissingBuses(timeout time.Duration, logger *utils.Logger) int {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()

	var removedBuses []string
	var terminatedBuses []string
	now := time.Now()

	for plateNo, info := range bt.busInfoMap {
		timeSinceLastSeen := now.Sub(info.LastSeenTime)

		if timeSinceLastSeen > timeout {
			// 🔧 이미 종료된 버스는 더 빨리 정리 (5분 후)
			if info.IsTerminated && timeSinceLastSeen > 5*time.Minute {
				removedBuses = append(removedBuses, plateNo)
			} else if !info.IsTerminated {
				// 종료되지 않은 버스는 먼저 종료 상태로 마킹
				info.IsTerminated = true
				terminatedBuses = append(terminatedBuses, plateNo)

				routeType := "전체 운행"
				if info.IsPartialRoute {
					routeType = "구간 운행"
				}

				logger.Infof("%s 종료 감지 - 차량번호: %s, 운행구간: %d→%d (최대: %d), 미목격: %.1f분 [종료 마킹]",
					routeType, plateNo, info.StartPosition, info.LastPosition, info.MaxPosition,
					timeSinceLastSeen.Minutes())
			} else {
				// 이미 종료된 버스가 오래된 경우 삭제
				removedBuses = append(removedBuses, plateNo)
			}
		}
	}

	// 실제 삭제 실행
	for _, plateNo := range removedBuses {
		info := bt.busInfoMap[plateNo]
		logger.Infof("🗑️ 버스 추적 정보 삭제 - 차량번호: %s, 종료 후 경과시간: %.1f분",
			plateNo, now.Sub(info.LastSeenTime).Minutes())
		delete(bt.busInfoMap, plateNo)
	}

	totalProcessed := len(terminatedBuses) + len(removedBuses)
	if totalProcessed > 0 {
		logger.Infof("버스 정리 완료 - 종료 마킹: %d대, 삭제: %d대, 현재 추적: %d대",
			len(terminatedBuses), len(removedBuses), len(bt.busInfoMap))
	}

	return totalProcessed
}

// GetBusTrackingInfo 버스 추적 정보 조회
func (bt *BusTracker) GetBusTrackingInfo(plateNo string) (*BusTrackingInfo, bool) {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	if info, exists := bt.busInfoMap[plateNo]; exists {
		// 복사본 반환 (동시성 안전)
		infoCopy := *info
		return &infoCopy, true
	}
	return nil, false
}

// 나머지 메서드들은 기존과 동일...
func (bt *BusTracker) detectPartialRoutePattern(plateNo string, info *BusTrackingInfo) {
	// 기존 로직 유지
	if info.StartPosition > 5 {
		info.IsPartialRoute = true
	}

	routeRange := info.MaxPosition - info.MinPosition + 1
	if routeRange < 25 {
		info.IsPartialRoute = true
	}

	if info.PreviousPosition > 0 && info.LastPosition < info.PreviousPosition-10 {
		info.StartPosition = info.LastPosition
		info.MinPosition = info.LastPosition
		info.IsPartialRoute = true
	}
}

// 시간대별 판별 함수들 (기존과 동일)
func (bt *BusTracker) isEarlyMorningTime() bool {
	hour := time.Now().Hour()
	return hour >= 5 && hour < 8
}

func (bt *BusTracker) isMorningPeakTime() bool {
	hour := time.Now().Hour()
	return hour >= 7 && hour < 10
}

func (bt *BusTracker) isLunchTime() bool {
	hour := time.Now().Hour()
	return hour >= 11 && hour < 14
}

func (bt *BusTracker) isEveningPeakTime() bool {
	hour := time.Now().Hour()
	return hour >= 17 && hour < 20
}

func (bt *BusTracker) isLateNightTime() bool {
	hour := time.Now().Hour()
	return hour >= 22 || hour < 5
}

// 기존 메서드들 (동일)
func (bt *BusTracker) UpdateLastSeenTime(plateNo string) {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()

	if info, exists := bt.busInfoMap[plateNo]; exists {
		info.LastSeenTime = time.Now()
	}
}

func (bt *BusTracker) RemoveFromTracking(plateNo string) {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()
	delete(bt.busInfoMap, plateNo)
}

func (bt *BusTracker) GetTrackedBusCount() int {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()
	return len(bt.busInfoMap)
}

func (bt *BusTracker) GetLastStation(plateNo string) (int64, bool) {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	if info, exists := bt.busInfoMap[plateNo]; exists {
		return info.LastPosition, true
	}
	return 0, false
}

func (bt *BusTracker) GetPreviousPosition(plateNo string) (int64, bool) {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	if info, exists := bt.busInfoMap[plateNo]; exists && info.PreviousPosition > 0 {
		return info.PreviousPosition, true
	}
	return 0, false
}
