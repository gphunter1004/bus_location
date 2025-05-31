// internal/web/services/tracking_service.go
package services

import (
	"fmt"
	"time"

	"bus-tracker/config"
	"bus-tracker/internal/services/tracker"
	"bus-tracker/internal/utils"
	"bus-tracker/internal/web/models/responses"
)

// TrackingService 트래킹 관련 서비스
type TrackingService struct {
	config     *config.Config
	logger     *utils.Logger
	busTracker *tracker.BusTrackerWithDuplicateCheck
}

// NewTrackingService 트래킹 서비스 생성
func NewTrackingService(
	cfg *config.Config,
	logger *utils.Logger,
	busTracker *tracker.BusTrackerWithDuplicateCheck,
) *TrackingService {
	return &TrackingService{
		config:     cfg,
		logger:     logger,
		busTracker: busTracker,
	}
}

// GetTrackedBuses 추적 중인 버스 목록 조회
func (s *TrackingService) GetTrackedBuses(includeDetails bool) (responses.TrackedBusesData, error) {
	currentDate := s.busTracker.GetCurrentOperatingDate()
	dailyStats := s.busTracker.GetDailyTripStatistics()
	totalTracked := s.busTracker.GetTrackedBusCount()

	// 일일 요약 계산
	var totalTrips, maxTrips int
	var activeBusesWithTrips int

	for _, tripCount := range dailyStats {
		if tripCount > 0 {
			activeBusesWithTrips++
			totalTrips += tripCount
			if tripCount > maxTrips {
				maxTrips = tripCount
			}
		}
	}

	var averageTrips float64
	if activeBusesWithTrips > 0 {
		averageTrips = float64(totalTrips) / float64(activeBusesWithTrips)
	}

	dailySummary := responses.DailySummary{
		ActiveBusesWithTrips: activeBusesWithTrips,
		TotalDailyTrips:      totalTrips,
		MaxDailyTrips:        maxTrips,
		AverageDailyTrips:    averageTrips,
	}

	data := responses.TrackedBusesData{
		TotalTracked:  totalTracked,
		OperatingDate: currentDate,
		DailySummary:  dailySummary,
	}

	// 상세 정보가 요청된 경우 버스 목록 추가
	if includeDetails {
		buses := s.getBusTrackingInfoList()
		data.Buses = buses
	}

	return data, nil
}

// getBusTrackingInfoList 버스 추적 정보 목록 생성
func (s *TrackingService) getBusTrackingInfoList() []responses.BusTrackingInfo {
	var buses []responses.BusTrackingInfo

	// 실제 구현: busTracker에서 모든 추적 중인 버스 정보를 가져옴
	// 위에서 제안한 GetAllBusTrackingInfo() 메서드가 구현되어 있다고 가정
	// 현재는 기존 메서드들을 활용하여 구현

	// 임시로 모의 데이터 반환 (실제로는 GetAllBusTrackingInfo() 사용)
	// allBusInfo := s.busTracker.GetAllBusTrackingInfo()
	// for plateNo, trackingInfo := range allBusInfo {
	//     buses = append(buses, responses.BusTrackingInfo{
	//         PlateNo:          plateNo,
	//         RouteNm:          trackingInfo.RouteNm,
	//         LastPosition:     trackingInfo.LastPosition,
	//         PreviousPosition: trackingInfo.PreviousPosition,
	//         LastSeenTime:     trackingInfo.LastSeenTime,
	//         TripNumber:       trackingInfo.TripNumber,
	//         IsTerminated:     trackingInfo.IsTerminated,
	//         TotalStations:    trackingInfo.TotalStations,
	//         TripStartTime:    trackingInfo.TripStartTime,
	//     })
	// }

	return buses
}

// GetBusInfo 특정 버스 정보 조회
func (s *TrackingService) GetBusInfo(plateNo string) (responses.BusInfoData, error) {
	// busTracker에서 버스 정보 조회
	trackingInfo, exists := s.busTracker.GetBusTrackingInfo(plateNo)
	if !exists {
		return responses.BusInfoData{}, fmt.Errorf("버스 %s를 찾을 수 없습니다", plateNo)
	}

	currentDate := s.busTracker.GetCurrentOperatingDate()
	dailyTripCount := s.busTracker.GetBusTripCount(plateNo)

	// 운행 시간 계산
	var tripDuration string
	if !trackingInfo.TripStartTime.IsZero() {
		duration := time.Since(trackingInfo.TripStartTime)
		tripDuration = formatDuration(duration)
	}

	// tracker.BusTrackingInfo를 responses.BusTrackingInfo로 변환
	// tracker.BusTrackingInfo의 실제 필드명에 맞춰 매핑
	busInfo := responses.BusTrackingInfo{
		PlateNo:          plateNo, // 파라미터로 받은 plateNo 사용
		RouteNm:          trackingInfo.RouteNm,
		LastPosition:     trackingInfo.LastPosition,
		PreviousPosition: trackingInfo.PreviousPosition,
		LastSeenTime:     trackingInfo.LastSeenTime,
		TripNumber:       trackingInfo.TripNumber,
		IsTerminated:     trackingInfo.IsTerminated,
		TotalStations:    trackingInfo.TotalStations,
		TripStartTime:    trackingInfo.TripStartTime,
	}

	data := responses.BusInfoData{
		BusInfo:        busInfo,
		OperatingDate:  currentDate,
		TripStartTime:  trackingInfo.TripStartTime,
		TripDuration:   tripDuration,
		DailyTripCount: dailyTripCount,
		RouteInfo: responses.RouteInfo{
			RouteID:       trackingInfo.RouteNm,
			RouteName:     trackingInfo.RouteNm,
			TotalStations: trackingInfo.TotalStations,
			RouteType:     "일반", // 실제로는 노선 정보에서 가져와야 함
		},
	}

	return data, nil
}

// RemoveBusTracking 버스 추적 제거
func (s *TrackingService) RemoveBusTracking(plateNo string) error {
	// 버스가 존재하는지 확인
	_, exists := s.busTracker.GetBusTrackingInfo(plateNo)
	if !exists {
		return fmt.Errorf("버스 %s를 찾을 수 없습니다", plateNo)
	}

	// 추적에서 제거
	s.busTracker.RemoveFromTracking(plateNo)
	s.logger.Infof("버스 추적 제거: %s", plateNo)

	return nil
}

// GetBusHistory 버스 이력 조회 (모의 구현)
func (s *TrackingService) GetBusHistory(plateNo string, limit, hours int) ([]map[string]interface{}, error) {
	// 실제 구현에서는 Elasticsearch에서 해당 버스의 이력을 조회해야 함
	// 현재는 모의 데이터 반환

	history := []map[string]interface{}{
		{
			"timestamp":   time.Now().Add(-30 * time.Minute),
			"plateNo":     plateNo,
			"stationName": "정류장1",
			"stationSeq":  1,
			"tripNumber":  1,
		},
		{
			"timestamp":   time.Now().Add(-1 * time.Hour),
			"plateNo":     plateNo,
			"stationName": "정류장2",
			"stationSeq":  2,
			"tripNumber":  1,
		},
	}

	if len(history) > limit {
		history = history[:limit]
	}

	return history, nil
}

// GetTripStatistics 운행 차수 통계 조회
func (s *TrackingService) GetTripStatistics() (responses.TripStatisticsData, error) {
	currentDate := s.busTracker.GetCurrentOperatingDate()
	lastResetTime := s.busTracker.GetLastResetTime()
	dailyStats := s.busTracker.GetDailyTripStatistics()

	// 통계 계산
	var totalTrips, maxTrips, minTrips int
	var activeVehicles int
	minTrips = 999999 // 초기값을 큰 수로 설정

	for _, tripCount := range dailyStats {
		if tripCount > 0 {
			activeVehicles++
			totalTrips += tripCount
			if tripCount > maxTrips {
				maxTrips = tripCount
			}
			if tripCount < minTrips {
				minTrips = tripCount
			}
		}
	}

	if activeVehicles == 0 {
		minTrips = 0
	}

	var averageTrips float64
	if activeVehicles > 0 {
		averageTrips = float64(totalTrips) / float64(activeVehicles)
	}

	summary := responses.TripStatisticsSummary{
		ActiveVehicles: activeVehicles,
		TotalTrips:     totalTrips,
		MaxTrips:       maxTrips,
		AverageTrips:   averageTrips,
		MinTrips:       minTrips,
	}

	// 상위 성과 차량 (최대 5대)
	topPerformers := s.getTopPerformers(dailyStats, 5)

	data := responses.TripStatisticsData{
		OperatingDate: currentDate,
		LastResetTime: lastResetTime,
		Summary:       summary,
		Statistics:    dailyStats,
		TopPerformers: topPerformers,
	}

	return data, nil
}

// getTopPerformers 상위 성과 차량 목록 생성
func (s *TrackingService) getTopPerformers(dailyStats map[string]int, limit int) []responses.VehiclePerformance {
	var performers []responses.VehiclePerformance

	for plateNo, tripCount := range dailyStats {
		if tripCount > 0 {
			// 실제로는 해당 차량의 노선 정보도 가져와야 함
			routeNm := "정보없음"
			if trackingInfo, exists := s.busTracker.GetBusTrackingInfo(plateNo); exists {
				routeNm = trackingInfo.RouteNm
			}

			performers = append(performers, responses.VehiclePerformance{
				PlateNo:    plateNo,
				RouteNm:    routeNm,
				TripCount:  tripCount,
				Efficiency: float64(tripCount) * 10.0, // 임시 효율성 계산
			})
		}
	}

	// 운행 차수 기준으로 정렬 (내림차순)
	for i := 0; i < len(performers)-1; i++ {
		for j := i + 1; j < len(performers); j++ {
			if performers[i].TripCount < performers[j].TripCount {
				performers[i], performers[j] = performers[j], performers[i]
			}
		}
	}

	// 제한된 수만 반환
	if len(performers) > limit {
		performers = performers[:limit]
	}

	return performers
}

// ResetTripCounters 운행 차수 카운터 리셋
func (s *TrackingService) ResetTripCounters() (responses.TripCounterResetData, error) {
	oldDate := s.busTracker.GetCurrentOperatingDate()
	oldStats := s.busTracker.GetDailyTripStatistics()
	affectedVehicles := len(oldStats)

	// 카운터 리셋
	s.busTracker.ResetDailyTripCounters()

	newDate := s.busTracker.GetCurrentOperatingDate()
	resetTime := time.Now()

	data := responses.TripCounterResetData{
		OldDate:            oldDate,
		NewDate:            newDate,
		ResetTime:          resetTime,
		AffectedVehicles:   affectedVehicles,
		PreviousStatistics: oldStats,
	}

	s.logger.Infof("운행 차수 카운터 리셋 완료 - 이전 일자: %s, 새 일자: %s, 영향받은 차량: %d대",
		oldDate, newDate, affectedVehicles)

	return data, nil
}

// GetDailyTripStatistics 특정 일자 운행 통계 조회 (모의 구현)
func (s *TrackingService) GetDailyTripStatistics(date string) (map[string]interface{}, error) {
	currentDate := s.busTracker.GetCurrentOperatingDate()

	if date == currentDate {
		// 현재 일자인 경우 실제 데이터 반환
		dailyStats := s.busTracker.GetDailyTripStatistics()
		return map[string]interface{}{
			"date":       date,
			"statistics": dailyStats,
			"isCurrent":  true,
		}, nil
	}

	// 과거 일자인 경우 모의 데이터 반환
	// 실제로는 데이터베이스나 로그에서 조회해야 함
	return map[string]interface{}{
		"date":       date,
		"statistics": map[string]int{},
		"isCurrent":  false,
		"message":    "과거 데이터는 현재 구현되지 않았습니다",
	}, nil
}

// GetActiveRoutes 활성 노선 목록 조회
func (s *TrackingService) GetActiveRoutes() ([]map[string]interface{}, error) {
	var routes []map[string]interface{}

	// API1 노선들
	for _, routeID := range s.config.API1Config.RouteIDs {
		routes = append(routes, map[string]interface{}{
			"routeId":   routeID,
			"routeName": routeID,
			"type":      "API1",
			"format":    "숫자형",
			"isActive":  true,
		})
	}

	// API2 노선들
	for _, routeID := range s.config.API2Config.RouteIDs {
		routes = append(routes, map[string]interface{}{
			"routeId":   routeID,
			"routeName": routeID,
			"type":      "API2",
			"format":    "GGB형식",
			"isActive":  true,
		})
	}

	return routes, nil
}

// GetRouteBuses 특정 노선의 버스 목록 조회
func (s *TrackingService) GetRouteBuses(routeId string) ([]map[string]interface{}, error) {
	var buses []map[string]interface{}

	// 실제 구현: 일일 통계에서 해당 노선의 버스들 찾기
	dailyStats := s.busTracker.GetDailyTripStatistics()

	for plateNo, tripCount := range dailyStats {
		// 각 버스의 추적 정보를 확인하여 노선 매칭
		if trackingInfo, exists := s.busTracker.GetBusTrackingInfo(plateNo); exists {
			if trackingInfo.RouteNm == routeId {
				buses = append(buses, map[string]interface{}{
					"plateNo":       plateNo,
					"routeNm":       trackingInfo.RouteNm,
					"tripCount":     tripCount,
					"lastPosition":  trackingInfo.LastPosition,
					"lastSeenTime":  trackingInfo.LastSeenTime,
					"isTerminated":  trackingInfo.IsTerminated,
					"tripNumber":    trackingInfo.TripNumber,
					"totalStations": trackingInfo.TotalStations,
				})
			}
		}
	}

	return buses, nil
}

// GetRouteStatistics 특정 노선 통계 조회
func (s *TrackingService) GetRouteStatistics(routeId string) (map[string]interface{}, error) {
	// 해당 노선의 버스들 통계 계산
	dailyStats := s.busTracker.GetDailyTripStatistics()

	var activeBuses int
	var totalTrips int
	var busesInRoute []string

	for plateNo, tripCount := range dailyStats {
		if trackingInfo, exists := s.busTracker.GetBusTrackingInfo(plateNo); exists {
			if trackingInfo.RouteNm == routeId {
				activeBuses++
				totalTrips += tripCount
				busesInRoute = append(busesInRoute, plateNo)
			}
		}
	}

	var averageTrips float64
	if activeBuses > 0 {
		averageTrips = float64(totalTrips) / float64(activeBuses)
	}

	stats := map[string]interface{}{
		"routeId":      routeId,
		"activeBuses":  activeBuses,
		"totalTrips":   totalTrips,
		"averageTrips": averageTrips,
		"busesInRoute": busesInRoute,
		"lastUpdate":   time.Now(),
	}

	return stats, nil
}

// formatDuration 기간을 사용자 친화적 형식으로 변환
func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%d초", int(d.Seconds()))
	} else if d < time.Hour {
		return fmt.Sprintf("%d분", int(d.Minutes()))
	} else {
		hours := int(d.Hours())
		minutes := int(d.Minutes()) % 60
		return fmt.Sprintf("%d시간 %d분", hours, minutes)
	}
}
