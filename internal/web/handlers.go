package web

import (
	"time"

	"github.com/gofiber/fiber/v2"
)

// StatusResponse 상태 응답 구조체
type StatusResponse struct {
	Status              string                 `json:"status"`
	Uptime              string                 `json:"uptime"`
	Version             string                 `json:"version"`
	Mode                string                 `json:"mode"`
	OperatingTime       bool                   `json:"operatingTime"`
	OrchestratorRunning bool                   `json:"orchestratorRunning"`
	ConfigInfo          map[string]interface{} `json:"configInfo"`
	LastUpdate          time.Time              `json:"lastUpdate"`
}

// StatisticsResponse 통계 응답 구조체
type StatisticsResponse struct {
	TotalBuses   int            `json:"totalBuses"`
	API1Only     int            `json:"api1Only"`
	API2Only     int            `json:"api2Only"`
	Both         int            `json:"both"`
	TrackedBuses int            `json:"trackedBuses"`
	CacheStats   CacheStatsInfo `json:"cacheStats"`
	LastUpdate   time.Time      `json:"lastUpdate"`
}

// CacheStatsInfo 캐시 통계 정보
type CacheStatsInfo struct {
	Routes   int `json:"routes"`
	Stations int `json:"stations"`
}

// BusTrackingInfo 버스 추적 정보 (웹용)
type BusTrackingInfo struct {
	PlateNo          string    `json:"plateNo"`
	RouteNm          string    `json:"routeNm"`
	LastPosition     int64     `json:"lastPosition"`
	PreviousPosition int64     `json:"previousPosition"`
	LastSeenTime     time.Time `json:"lastSeenTime"`
	TripNumber       int       `json:"tripNumber"`
	IsTerminated     bool      `json:"isTerminated"`
	TotalStations    int       `json:"totalStations"`
}

// 상태 조회 핸들러
func (ws *WebServer) handleStatus(c *fiber.Ctx) error {
	now := time.Now()

	response := StatusResponse{
		Status:              "running",
		Uptime:              "계산 필요", // 실제로는 시작 시간을 저장해서 계산
		Version:             "1.0.0",
		Mode:                "unified",
		OperatingTime:       ws.config.IsOperatingTime(now),
		OrchestratorRunning: ws.orchestrator.IsRunning(),
		ConfigInfo: map[string]interface{}{
			"api1Routes":              len(ws.config.API1Config.RouteIDs),
			"api2Routes":              len(ws.config.API2Config.RouteIDs),
			"busDisappearanceTimeout": ws.config.BusDisappearanceTimeout.String(),
			"enableTerminalStop":      ws.config.EnableTerminalStop,
		},
		LastUpdate: now,
	}

	return c.JSON(response)
}

// 통계 조회 핸들러
func (ws *WebServer) handleStatistics(c *fiber.Ctx) error {
	totalBuses, api1Only, api2Only, both := ws.dataManager.GetStatistics()
	trackedBuses := ws.busTracker.GetTrackedBusCount()

	var routes, stations int
	if ws.stationCache != nil {
		routes, stations = ws.stationCache.GetCacheStatistics()
	}

	response := StatisticsResponse{
		TotalBuses:   totalBuses,
		API1Only:     api1Only,
		API2Only:     api2Only,
		Both:         both,
		TrackedBuses: trackedBuses,
		CacheStats: CacheStatsInfo{
			Routes:   routes,
			Stations: stations,
		},
		LastUpdate: time.Now(),
	}

	return c.JSON(response)
}

// 설정 조회 핸들러
func (ws *WebServer) handleGetConfig(c *fiber.Ctx) error {
	config := map[string]interface{}{
		"api1Config": map[string]interface{}{
			"interval": ws.config.API1Config.Interval.String(),
			"baseURL":  ws.config.API1Config.BaseURL,
			"routeIDs": ws.config.API1Config.RouteIDs,
			"priority": ws.config.API1Config.Priority,
		},
		"api2Config": map[string]interface{}{
			"interval": ws.config.API2Config.Interval.String(),
			"baseURL":  ws.config.API2Config.BaseURL,
			"routeIDs": ws.config.API2Config.RouteIDs,
			"priority": ws.config.API2Config.Priority,
		},
		"trackingConfig": map[string]interface{}{
			"busDisappearanceTimeout": ws.config.BusDisappearanceTimeout.String(),
			"enableTerminalStop":      ws.config.EnableTerminalStop,
			"busCleanupInterval":      ws.config.BusCleanupInterval.String(),
		},
		"operatingTime": map[string]interface{}{
			"startHour":   ws.config.OperatingStartHour,
			"startMinute": ws.config.OperatingStartMinute,
			"endHour":     ws.config.OperatingEndHour,
			"endMinute":   ws.config.OperatingEndMinute,
		},
	}

	return c.JSON(config)
}

// 정류소 캐시 조회 핸들러
func (ws *WebServer) handleGetStationCache(c *fiber.Ctx) error {
	routes, stations := ws.stationCache.GetCacheStatistics()

	// 전체 캐시 데이터 구성
	cacheData := make(map[string]interface{})

	// API1 노선별 정류소 정보
	api1Data := make(map[string]interface{})
	if len(ws.config.API1Config.RouteIDs) > 0 {
		for _, routeID := range ws.config.API1Config.RouteIDs {
			stationCount := ws.stationCache.GetRouteStationCount(routeID)
			api1Data[routeID] = map[string]interface{}{
				"stationCount": stationCount,
				"type":         "API1",
				"format":       "숫자형",
			}
		}
	}
	cacheData["api1Routes"] = api1Data

	// API2 노선별 정류소 정보
	api2Data := make(map[string]interface{})
	if len(ws.config.API2Config.RouteIDs) > 0 {
		for _, routeID := range ws.config.API2Config.RouteIDs {
			stationCount := ws.stationCache.GetRouteStationCount(routeID)
			api2Data[routeID] = map[string]interface{}{
				"stationCount": stationCount,
				"type":         "API2",
				"format":       "GGB형식",
			}
		}
	}
	cacheData["api2Routes"] = api2Data

	response := map[string]interface{}{
		"summary": map[string]interface{}{
			"totalRoutes":   routes,
			"totalStations": stations,
			"api1Count":     len(ws.config.API1Config.RouteIDs),
			"api2Count":     len(ws.config.API2Config.RouteIDs),
		},
		"detailedData": cacheData,
		"routeLists": map[string]interface{}{
			"api1RouteIDs": ws.config.API1Config.RouteIDs,
			"api2RouteIDs": ws.config.API2Config.RouteIDs,
		},
		"lastUpdate": time.Now(),
		"debug": map[string]interface{}{
			"api1RoutesCount": len(api1Data),
			"api2RoutesCount": len(api2Data),
			"cacheAvailable":  ws.stationCache != nil,
		},
	}

	return c.JSON(response)
}

// 특정 노선 정류소 조회 핸들러
func (ws *WebServer) handleGetRouteStations(c *fiber.Ctx) error {
	routeId := c.Params("routeId")
	if routeId == "" {
		return c.Status(400).JSON(fiber.Map{
			"error":   true,
			"message": "routeId 파라미터가 필요합니다",
		})
	}

	stationCount := ws.stationCache.GetRouteStationCount(routeId)

	response := map[string]interface{}{
		"routeId":      routeId,
		"stationCount": stationCount,
		"lastUpdate":   time.Now(),
	}

	return c.JSON(response)
}

// 정류소 캐시 새로고침 핸들러
func (ws *WebServer) handleReloadStationCache(c *fiber.Ctx) error {
	allRouteIDs := append(ws.config.API1Config.RouteIDs, ws.config.API2Config.RouteIDs...)

	if len(allRouteIDs) == 0 {
		return c.Status(400).JSON(fiber.Map{
			"error":   true,
			"message": "설정된 노선이 없습니다",
		})
	}

	err := ws.stationCache.LoadStationCache(allRouteIDs)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error":   true,
			"message": "캐시 새로고침 실패: " + err.Error(),
		})
	}

	routes, stations := ws.stationCache.GetCacheStatistics()

	return c.JSON(fiber.Map{
		"success": true,
		"message": "정류소 캐시가 성공적으로 새로고침되었습니다",
		"stats": map[string]int{
			"routes":   routes,
			"stations": stations,
		},
	})
}

// 정류소 캐시 삭제 핸들러 (실제로는 통계만 반환)
func (ws *WebServer) handleClearStationCache(c *fiber.Ctx) error {
	// 실제 캐시 삭제는 구현하지 않고 현재 상태만 반환
	routes, stations := ws.stationCache.GetCacheStatistics()

	return c.JSON(fiber.Map{
		"success": true,
		"message": "캐시 정보 조회 완료 (실제 삭제는 구현되지 않음)",
		"stats": map[string]int{
			"routes":   routes,
			"stations": stations,
		},
	})
}

// 추적 중인 버스 목록 조회 핸들러
func (ws *WebServer) handleGetTrackedBuses(c *fiber.Ctx) error {
	trackedCount := ws.busTracker.GetTrackedBusCount()

	// 실제 버스 목록을 가져오는 기능은 BusTracker에 추가 메서드가 필요
	response := map[string]interface{}{
		"totalTracked": trackedCount,
		"message":      "추적 중인 버스 수 조회 완료",
		"lastUpdate":   time.Now(),
	}

	return c.JSON(response)
}

// 특정 버스 정보 조회 핸들러
func (ws *WebServer) handleGetBusInfo(c *fiber.Ctx) error {
	plateNo := c.Params("plateNo")
	if plateNo == "" {
		return c.Status(400).JSON(fiber.Map{
			"error":   true,
			"message": "plateNo 파라미터가 필요합니다",
		})
	}

	info, exists := ws.busTracker.GetBusTrackingInfo(plateNo)
	if !exists {
		return c.Status(404).JSON(fiber.Map{
			"error":   true,
			"message": "해당 버스를 찾을 수 없습니다",
		})
	}

	response := BusTrackingInfo{
		PlateNo:          plateNo,
		RouteNm:          info.RouteNm,
		LastPosition:     info.LastPosition,
		PreviousPosition: info.PreviousPosition,
		LastSeenTime:     info.LastSeenTime,
		TripNumber:       info.TripNumber,
		IsTerminated:     info.IsTerminated,
		TotalStations:    info.TotalStations,
	}

	return c.JSON(response)
}

// 버스 추적 제거 핸들러
func (ws *WebServer) handleRemoveBusTracking(c *fiber.Ctx) error {
	plateNo := c.Params("plateNo")
	if plateNo == "" {
		return c.Status(400).JSON(fiber.Map{
			"error":   true,
			"message": "plateNo 파라미터가 필요합니다",
		})
	}

	ws.busTracker.RemoveFromTracking(plateNo)

	return c.JSON(fiber.Map{
		"success": true,
		"message": "버스 추적이 제거되었습니다: " + plateNo,
	})
}

// 운행 차수 카운터 리셋 핸들러
func (ws *WebServer) handleResetTripCounters(c *fiber.Ctx) error {
	ws.busTracker.ResetTripCounters()

	return c.JSON(fiber.Map{
		"success": true,
		"message": "모든 차량의 운행 차수 카운터가 리셋되었습니다",
	})
}

// 운행 차수 통계 조회 핸들러
func (ws *WebServer) handleGetTripStatistics(c *fiber.Ctx) error {
	stats := ws.busTracker.GetDailyTripStatistics()

	return c.JSON(fiber.Map{
		"success":    true,
		"statistics": stats,
		"totalBuses": len(stats),
		"lastUpdate": time.Now(),
	})
}

// 통합 데이터 조회 핸들러
func (ws *WebServer) handleGetUnifiedData(c *fiber.Ctx) error {
	totalBuses, api1Only, api2Only, both := ws.dataManager.GetStatistics()

	response := map[string]interface{}{
		"summary": map[string]int{
			"totalBuses": totalBuses,
			"api1Only":   api1Only,
			"api2Only":   api2Only,
			"both":       both,
		},
		"lastUpdate": time.Now(),
	}

	return c.JSON(response)
}

// 데이터 정리 핸들러
func (ws *WebServer) handleDataCleanup(c *fiber.Ctx) error {
	cleanedCount := ws.dataManager.CleanupOldData(ws.config.DataRetentionPeriod)

	return c.JSON(fiber.Map{
		"success":      true,
		"message":      "데이터 정리가 완료되었습니다",
		"cleanedCount": cleanedCount,
	})
}

// 시스템 시작 핸들러
func (ws *WebServer) handleStart(c *fiber.Ctx) error {
	if ws.orchestrator.IsRunning() {
		return c.JSON(fiber.Map{
			"success": true,
			"message": "시스템이 이미 실행 중입니다",
		})
	}

	err := ws.orchestrator.Start()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error":   true,
			"message": "시스템 시작 실패: " + err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"success": true,
		"message": "시스템이 성공적으로 시작되었습니다",
	})
}

// 시스템 정지 핸들러
func (ws *WebServer) handleStop(c *fiber.Ctx) error {
	if !ws.orchestrator.IsRunning() {
		return c.JSON(fiber.Map{
			"success": true,
			"message": "시스템이 이미 정지 상태입니다",
		})
	}

	ws.orchestrator.Stop()

	return c.JSON(fiber.Map{
		"success": true,
		"message": "시스템이 성공적으로 정지되었습니다",
	})
}

// 헬스체크 핸들러
func (ws *WebServer) handleHealthCheck(c *fiber.Ctx) error {
	now := time.Now()
	isOperating := ws.config.IsOperatingTime(now)
	isRunning := ws.orchestrator.IsRunning()

	status := "healthy"
	if !isRunning {
		status = "stopped"
	} else if !isOperating {
		status = "standby"
	}

	totalBuses, api1Only, api2Only, both := ws.dataManager.GetStatistics()
	routes, stations := ws.stationCache.GetCacheStatistics()

	response := map[string]interface{}{
		"status":              status,
		"timestamp":           now,
		"operatingTime":       isOperating,
		"orchestratorRunning": isRunning,
		"statistics": map[string]interface{}{
			"totalBuses":     totalBuses,
			"api1Only":       api1Only,
			"api2Only":       api2Only,
			"both":           both,
			"trackedBuses":   ws.busTracker.GetTrackedBusCount(),
			"cachedRoutes":   routes,
			"cachedStations": stations,
		},
		"config": map[string]interface{}{
			"api1Routes": len(ws.config.API1Config.RouteIDs),
			"api2Routes": len(ws.config.API2Config.RouteIDs),
		},
	}

	return c.JSON(response)
}
