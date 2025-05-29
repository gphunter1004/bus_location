package services

import (
	"fmt"
	"sync"
	"time"

	"bus-tracker/models"
	"bus-tracker/utils"
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

// IsStationChanged 정류장 변경 여부 확인 및 상태 업데이트
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
			IsPartialRoute:   false, // 처음에는 전체 운행으로 가정
		}
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

// detectPartialRoutePattern 구간 운행 패턴 감지
func (bt *BusTracker) detectPartialRoutePattern(plateNo string, info *BusTrackingInfo) {
	// 구간 운행 감지 조건들:

	// 1. 시작 위치가 1번이 아닌 경우 (중간 투입)
	if info.StartPosition > 5 { // 5번 이후부터 시작하면 구간 운행으로 간주
		info.IsPartialRoute = true
	}

	// 2. 운행 범위가 전체의 50% 미만인 경우
	routeRange := info.MaxPosition - info.MinPosition + 1
	if routeRange < 25 { // 25개 정류장 미만 운행하면 구간 운행
		info.IsPartialRoute = true
	}

	// 3. 역방향 운행 감지 (최대 위치에서 갑자기 낮은 위치로)
	if info.PreviousPosition > 0 && info.LastPosition < info.PreviousPosition-10 {
		// 10개 이상 정류장을 뒤로 이동했다면 새로운 구간 시작으로 판단
		info.StartPosition = info.LastPosition
		info.MinPosition = info.LastPosition
		info.IsPartialRoute = true
	}
}

// IsEndOfPartialRoute 구간 운행 종료 조건 확인
func (bt *BusTracker) IsEndOfPartialRoute(plateNo string, currentPosition, totalStations int64) bool {
	bt.mutex.RLock()
	info, exists := bt.busInfoMap[plateNo]
	bt.mutex.RUnlock()

	if !exists {
		return false
	}

	// 전체 운행 버스의 경우 기존 로직 사용
	if !info.IsPartialRoute {
		return totalStations > 0 && currentPosition >= (totalStations-1)
	}

	// 구간 운행 버스의 경우 다른 조건들 적용

	// 1. 일반적인 종점 도달
	if totalStations > 0 && currentPosition >= (totalStations-1) {
		return true
	}

	// 2. 아침 구간 운행 패턴 (예: 10-35번)
	if bt.isMorningPeakTime() && currentPosition >= 35 && info.StartPosition >= 8 {
		return true
	}

	// 3. 저녁 구간 운행 패턴 (예: 30-59번)
	if bt.isEveningPeakTime() && currentPosition >= 55 && info.StartPosition >= 25 {
		return true
	}

	// 4. 심야 구간 운행 패턴 (예: 1-20번)
	if bt.isLateNightTime() && currentPosition >= 20 && info.StartPosition <= 5 {
		return true
	}

	// 5. 점심시간 구간 운행 패턴 (예: 15-45번)
	if bt.isLunchTime() && currentPosition >= 45 && info.StartPosition >= 10 && info.StartPosition <= 20 {
		return true
	}

	return false
}

// 시간대별 판별 함수들
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

// UpdateLastSeenTime 마지막 목격 시간 업데이트
func (bt *BusTracker) UpdateLastSeenTime(plateNo string) {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()

	if info, exists := bt.busInfoMap[plateNo]; exists {
		info.LastSeenTime = time.Now()
	}
}

// RemoveFromTracking 트래킹에서 버스 제거
func (bt *BusTracker) RemoveFromTracking(plateNo string) {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()
	delete(bt.busInfoMap, plateNo)
}

// GetTrackedBusCount 현재 추적 중인 버스 개수 반환
func (bt *BusTracker) GetTrackedBusCount() int {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()
	return len(bt.busInfoMap)
}

// GetLastStation 특정 버스의 마지막 정류장 정보 조회
func (bt *BusTracker) GetLastStation(plateNo string) (int64, bool) {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	if info, exists := bt.busInfoMap[plateNo]; exists {
		return info.LastPosition, true
	}
	return 0, false
}

// GetPreviousPosition 이전 위치 정보 조회
func (bt *BusTracker) GetPreviousPosition(plateNo string) (int64, bool) {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	if info, exists := bt.busInfoMap[plateNo]; exists && info.PreviousPosition > 0 {
		return info.PreviousPosition, true
	}
	return 0, false
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

// CleanupMissingBuses 일정 시간 동안 보이지 않은 버스들을 정리
func (bt *BusTracker) CleanupMissingBuses(timeout time.Duration, logger *utils.Logger) int {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()

	var removedBuses []string
	now := time.Now()

	for plateNo, info := range bt.busInfoMap {
		if now.Sub(info.LastSeenTime) > timeout {
			removedBuses = append(removedBuses, plateNo)
		}
	}

	// 정리 실행 및 로깅
	for _, plateNo := range removedBuses {
		info := bt.busInfoMap[plateNo]

		routeType := "전체 운행"
		if info.IsPartialRoute {
			routeType = "구간 운행"
		}

		logger.Infof("%s 종료 감지 - 차량번호: %s, 운행구간: %d→%d (최대: %d), 미목격: %.1f분 [트래킹 종료]",
			routeType, plateNo, info.StartPosition, info.LastPosition, info.MaxPosition,
			now.Sub(info.LastSeenTime).Minutes())

		delete(bt.busInfoMap, plateNo)
	}

	if len(removedBuses) > 0 {
		logger.Infof("운행 종료 정리 완료 - %d대 버스 트래킹 종료", len(removedBuses))
	}

	return len(removedBuses)
}

// FilterChangedStations 정류장 변경된 버스만 필터링 (기존 로직 + 스마트 감지)
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

		// 구간 운행 종료 조건 확인 (스마트 감지)
		if bt.IsEndOfPartialRoute(trackingKey, currentPosition, int64(bus.TotalStations)) {
			info, _ := bt.GetBusTrackingInfo(trackingKey)

			if info != nil {
				routeType := "전체 운행"
				if info.IsPartialRoute {
					routeType = "구간 운행"
				}

				timeInfo := ""
				if bt.isMorningPeakTime() {
					timeInfo = " (출근시간)"
				} else if bt.isEveningPeakTime() {
					timeInfo = " (퇴근시간)"
				} else if bt.isLunchTime() {
					timeInfo = " (점심시간)"
				} else if bt.isLateNightTime() {
					timeInfo = " (심야시간)"
				}

				logger.Infof("%s 종료 - 차량번호: %s, 정류장: %s (%s), 순서: %d/%d, 운행구간: %d→%d%s [트래킹 종료]",
					routeType, bus.PlateNo, bus.NodeNm, bus.NodeId, bus.NodeOrd, bus.TotalStations,
					info.StartPosition, currentPosition, timeInfo)
			}

			bt.RemoveFromTracking(trackingKey)
			changedBuses = append(changedBuses, bus)
			continue
		}

		// 기존 변경 감지 로직
		if bt.IsStationChanged(trackingKey, currentPosition) {
			changedBuses = append(changedBuses, bus)

			// 추적 정보 조회
			info, _ := bt.GetBusTrackingInfo(trackingKey)
			routeTypeInfo := ""
			if info != nil && info.IsPartialRoute {
				routeTypeInfo = fmt.Sprintf(" [구간운행: %d→%d]", info.StartPosition, info.MaxPosition)
			}

			// 로깅 (기존 로직 + 구간 정보 추가)
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

	// 기존 상세 로깅 로직 (간소화)
	if len(changedBuses) > 0 {
		logger.Infof("정류장 변경된 버스: %d대 / 전체: %d대", len(changedBuses), len(busLocations))
	} else {
		logger.Infof("정류장 변경된 버스 없음 (전체: %d대)", len(busLocations))
	}

	return changedBuses
}
