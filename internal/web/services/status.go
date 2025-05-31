// internal/web/services/status_service.go
package services

import (
	"fmt"
	"runtime"
	"time"

	"bus-tracker/config"
	"bus-tracker/internal/services"
	"bus-tracker/internal/services/tracker"
	"bus-tracker/internal/utils"
	"bus-tracker/internal/web/models/responses"
)

// StatusService 상태 관련 서비스
type StatusService struct {
	config       *config.Config
	logger       *utils.Logger
	orchestrator *services.MultiAPIOrchestrator
	busTracker   *tracker.BusTrackerWithDuplicateCheck
	dataManager  services.UnifiedDataManagerInterface
}

// NewStatusService 상태 서비스 생성
func NewStatusService(
	cfg *config.Config,
	logger *utils.Logger,
	orchestrator *services.MultiAPIOrchestrator,
	busTracker *tracker.BusTrackerWithDuplicateCheck,
	dataManager services.UnifiedDataManagerInterface,
) *StatusService {
	return &StatusService{
		config:       cfg,
		logger:       logger,
		orchestrator: orchestrator,
		busTracker:   busTracker,
		dataManager:  dataManager,
	}
}

// GetSystemStatus 시스템 상태 조회
func (s *StatusService) GetSystemStatus() (responses.StatusData, error) {
	now := time.Now()
	currentDate := s.busTracker.GetCurrentOperatingDate()
	lastResetTime := s.busTracker.GetLastResetTime()

	statusData := responses.StatusData{
		Status:              "running",
		Uptime:              s.calculateUptime(lastResetTime),
		Version:             "1.0.0",
		Mode:                "unified",
		OperatingTime:       s.config.IsOperatingTime(now),
		OrchestratorRunning: s.orchestrator.IsRunning(),
		Config: responses.ConfigInfo{
			API1Routes:              len(s.config.API1Config.RouteIDs),
			API2Routes:              len(s.config.API2Config.RouteIDs),
			BusDisappearanceTimeout: s.config.BusDisappearanceTimeout.String(),
			EnableTerminalStop:      s.config.EnableTerminalStop,
			OperatingSchedule: fmt.Sprintf("%02d:%02d ~ %02d:%02d",
				s.config.OperatingStartHour, s.config.OperatingStartMinute,
				s.config.OperatingEndHour, s.config.OperatingEndMinute),
		},
		OperatingInfo: responses.OperatingInfo{
			CurrentDate:      currentDate,
			LastCounterReset: lastResetTime,
		},
	}

	// 운영시간이 아닌 경우 다음 운영시간 설정
	if !statusData.OperatingTime {
		nextOperatingTime := s.config.GetNextOperatingTime(now)
		statusData.OperatingInfo.NextOperatingTime = &nextOperatingTime
	}

	return statusData, nil
}

// GetStatistics 통계 정보 조회
func (s *StatusService) GetStatistics() (responses.StatisticsData, error) {
	totalBuses, api1Only, api2Only, both := s.dataManager.GetStatistics()
	trackedBuses := s.busTracker.GetTrackedBusCount()

	// 시스템 메트릭 수집
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	statisticsData := responses.StatisticsData{
		BusStatistics: responses.BusStatistics{
			TotalBuses:   totalBuses,
			API1Only:     api1Only,
			API2Only:     api2Only,
			Both:         both,
			TrackedBuses: trackedBuses,
		},
		CacheStatistics: responses.CacheStatistics{
			Routes:   0, // TODO: 캐시 서비스에서 가져오기
			Stations: 0,
		},
		SystemMetrics: responses.SystemMetrics{
			MemoryUsage: fmt.Sprintf("%.2f MB", float64(m.Alloc)/1024/1024),
			Uptime:      s.calculateUptime(s.busTracker.GetLastResetTime()),
		},
	}

	return statisticsData, nil
}

// GetHealthCheck 헬스체크 수행
func (s *StatusService) GetHealthCheck() (responses.HealthCheckData, bool, error) {
	checks := make(map[string]string)
	isHealthy := true

	// 오케스트레이터 상태 확인
	if s.orchestrator.IsRunning() {
		checks["orchestrator"] = "healthy"
	} else {
		checks["orchestrator"] = "stopped"
		isHealthy = false
	}

	// 데이터 매니저 상태 확인
	totalBuses, _, _, _ := s.dataManager.GetStatistics()
	if totalBuses > 0 {
		checks["dataManager"] = "healthy"
	} else {
		checks["dataManager"] = "no_data"
	}

	// 버스 트래커 상태 확인
	trackedBuses := s.busTracker.GetTrackedBusCount()
	if trackedBuses >= 0 {
		checks["busTracker"] = "healthy"
	} else {
		checks["busTracker"] = "error"
		isHealthy = false
	}

	healthData := responses.HealthCheckData{
		Status: func() string {
			if isHealthy {
				return "healthy"
			}
			return "unhealthy"
		}(),
		Checks: checks,
		Statistics: responses.BusStatistics{
			TotalBuses:   totalBuses,
			TrackedBuses: trackedBuses,
		},
		Configuration: struct {
			API1Routes int `json:"api1Routes"`
			API2Routes int `json:"api2Routes"`
		}{
			API1Routes: len(s.config.API1Config.RouteIDs),
			API2Routes: len(s.config.API2Config.RouteIDs),
		},
	}

	return healthData, isHealthy, nil
}

// GetSystemMetrics 시스템 메트릭 조회
func (s *StatusService) GetSystemMetrics() (map[string]interface{}, error) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	metrics := map[string]interface{}{
		"memory": map[string]interface{}{
			"alloc":      fmt.Sprintf("%.2f MB", float64(m.Alloc)/1024/1024),
			"totalAlloc": fmt.Sprintf("%.2f MB", float64(m.TotalAlloc)/1024/1024),
			"sys":        fmt.Sprintf("%.2f MB", float64(m.Sys)/1024/1024),
			"numGC":      m.NumGC,
		},
		"goroutines": runtime.NumGoroutine(),
		"uptime":     s.calculateUptime(s.busTracker.GetLastResetTime()),
		"timestamp":  time.Now(),
	}

	return metrics, nil
}

// GetRecentLogs 최근 로그 조회 (모의 구현)
func (s *StatusService) GetRecentLogs(limit int, level string) ([]map[string]interface{}, error) {
	// 실제 구현에서는 로그 파일이나 로그 저장소에서 조회
	logs := []map[string]interface{}{
		{
			"timestamp": time.Now().Add(-1 * time.Minute),
			"level":     "INFO",
			"message":   "시스템이 정상적으로 작동 중입니다",
			"component": "status_service",
		},
		{
			"timestamp": time.Now().Add(-5 * time.Minute),
			"level":     "INFO",
			"message":   "버스 데이터 처리 완료",
			"component": "data_manager",
		},
	}

	return logs, nil
}

// GetActiveAlerts 활성 알림 조회 (모의 구현)
func (s *StatusService) GetActiveAlerts() ([]map[string]interface{}, error) {
	alerts := []map[string]interface{}{}

	// 운영시간 외 알림
	if !s.config.IsOperatingTime(time.Now()) {
		alerts = append(alerts, map[string]interface{}{
			"id":        "operating_time",
			"type":      "warning",
			"title":     "운영시간 외",
			"message":   "현재 운영시간이 아닙니다",
			"timestamp": time.Now(),
		})
	}

	// 추적 버스 수 확인
	trackedBuses := s.busTracker.GetTrackedBusCount()
	if trackedBuses == 0 {
		alerts = append(alerts, map[string]interface{}{
			"id":        "no_tracked_buses",
			"type":      "warning",
			"title":     "추적 버스 없음",
			"message":   "현재 추적 중인 버스가 없습니다",
			"timestamp": time.Now(),
		})
	}

	return alerts, nil
}

// GetNotifications 알림 목록 조회 (모의 구현)
func (s *StatusService) GetNotifications(page, limit int) ([]map[string]interface{}, responses.PaginationInfo, error) {
	// 실제 구현에서는 데이터베이스에서 조회
	notifications := []map[string]interface{}{
		{
			"id":        "1",
			"title":     "시스템 시작",
			"message":   "버스 트래커가 성공적으로 시작되었습니다",
			"type":      "info",
			"isRead":    false,
			"timestamp": time.Now().Add(-1 * time.Hour),
		},
	}

	pagination := responses.PaginationInfo{
		Page:       page,
		PerPage:    limit,
		Total:      len(notifications),
		TotalPages: 1,
		HasNext:    false,
		HasPrev:    false,
	}

	return notifications, pagination, nil
}

// CreateNotification 알림 생성 (모의 구현)
func (s *StatusService) CreateNotification(title, message, notificationType string, priority int) (map[string]interface{}, error) {
	notification := map[string]interface{}{
		"id":        fmt.Sprintf("%d", time.Now().UnixNano()),
		"title":     title,
		"message":   message,
		"type":      notificationType,
		"priority":  priority,
		"isRead":    false,
		"timestamp": time.Now(),
	}

	s.logger.Infof("알림 생성: %s - %s", title, message)

	return notification, nil
}

// UpdateNotification 알림 수정 (모의 구현)
func (s *StatusService) UpdateNotification(id, title, message, notificationType string, priority int, isRead *bool) (map[string]interface{}, error) {
	notification := map[string]interface{}{
		"id":        id,
		"title":     title,
		"message":   message,
		"type":      notificationType,
		"priority":  priority,
		"timestamp": time.Now(),
	}

	if isRead != nil {
		notification["isRead"] = *isRead
	}

	s.logger.Infof("알림 수정: %s", id)

	return notification, nil
}

// DeleteNotification 알림 삭제 (모의 구현)
func (s *StatusService) DeleteNotification(id string) error {
	s.logger.Infof("알림 삭제: %s", id)
	return nil
}

// calculateUptime 업타임 계산
func (s *StatusService) calculateUptime(startTime time.Time) string {
	if startTime.IsZero() {
		return "정보 없음"
	}

	duration := time.Since(startTime)

	if duration < time.Minute {
		return fmt.Sprintf("%d초", int(duration.Seconds()))
	} else if duration < time.Hour {
		return fmt.Sprintf("%d분", int(duration.Minutes()))
	} else if duration < 24*time.Hour {
		hours := int(duration.Hours())
		minutes := int(duration.Minutes()) % 60
		return fmt.Sprintf("%d시간 %d분", hours, minutes)
	} else {
		days := int(duration.Hours()) / 24
		hours := int(duration.Hours()) % 24
		return fmt.Sprintf("%d일 %d시간", days, hours)
	}
}
