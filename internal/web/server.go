// internal/web/server.go
package web

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"bus-tracker/config"
	"bus-tracker/internal/services"
	"bus-tracker/internal/services/api"
	"bus-tracker/internal/services/cache"
	"bus-tracker/internal/services/tracker"
	"bus-tracker/internal/utils"
)

// WebServer 웹 서버 구조체
type WebServer struct {
	config    *config.Config
	logger    *utils.Logger
	server    *http.Server
	isRunning bool
	// 의존성들
	orchestrator *services.MultiAPIOrchestrator
	busTracker   *tracker.BusTrackerWithDuplicateCheck
	stationCache *cache.StationCacheService
	api1Client   *api.API1Client
	api2Client   *api.API2Client
	dataManager  services.UnifiedDataManagerInterface
}

// NewWebServer 새로운 웹 서버 생성
func NewWebServer(
	cfg *config.Config,
	logger *utils.Logger,
	orchestrator *services.MultiAPIOrchestrator,
	busTracker *tracker.BusTrackerWithDuplicateCheck,
	stationCache *cache.StationCacheService,
	api1Client *api.API1Client,
	api2Client *api.API2Client,
	dataManager services.UnifiedDataManagerInterface,
) *WebServer {

	ws := &WebServer{
		config:       cfg,
		logger:       logger,
		isRunning:    false,
		orchestrator: orchestrator,
		busTracker:   busTracker,
		stationCache: stationCache,
		api1Client:   api1Client,
		api2Client:   api2Client,
		dataManager:  dataManager,
	}

	// HTTP 서버 설정
	mux := http.NewServeMux()
	ws.setupRoutes(mux)

	ws.server = &http.Server{
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	return ws
}

// setupRoutes 라우트 설정
func (ws *WebServer) setupRoutes(mux *http.ServeMux) {
	// 메인 대시보드
	mux.HandleFunc("/", ws.handleDashboard)

	// API 엔드포인트들
	mux.HandleFunc("/api/status", ws.handleAPIStatus)
	mux.HandleFunc("/api/statistics", ws.handleAPIStatistics)
	mux.HandleFunc("/api/health", ws.handleAPIHealth)

	// 설정 정보
	mux.HandleFunc("/api/config", ws.handleAPIConfig)
}

// handleDashboard 메인 대시보드 핸들러
func (ws *WebServer) handleDashboard(w http.ResponseWriter, r *http.Request) {
	// 통계 수집
	totalBuses, api1Only, api2Only, both := ws.dataManager.GetStatistics()
	trackedBuses := ws.busTracker.GetTrackedBusCount()
	currentDate := ws.busTracker.GetCurrentOperatingDate()

	html := fmt.Sprintf(`
<!DOCTYPE html>
<html>
<head>
    <title>Bus Tracker Dashboard</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body { 
            font-family: Arial, sans-serif; 
            margin: 20px; 
            background-color: #f5f5f5; 
        }
        .container { 
            max-width: 1200px; 
            margin: 0 auto; 
        }
        .card { 
            background: white; 
            padding: 20px; 
            margin: 10px 0; 
            border-radius: 8px; 
            box-shadow: 0 2px 4px rgba(0,0,0,0.1); 
        }
        .header { 
            text-align: center; 
            color: #333; 
        }
        .stats { 
            display: flex; 
            gap: 20px; 
            flex-wrap: wrap; 
        }
        .stat-box { 
            flex: 1; 
            min-width: 200px; 
            text-align: center; 
            padding: 15px; 
            background: #e8f4fd; 
            border-radius: 5px; 
        }
        .stat-number { 
            font-size: 2em; 
            font-weight: bold; 
            color: #2196F3; 
        }
        .stat-label { 
            color: #666; 
            margin-top: 5px; 
        }
        .refresh-btn { 
            background: #2196F3; 
            color: white; 
            padding: 10px 20px; 
            border: none; 
            border-radius: 4px; 
            cursor: pointer; 
            float: right; 
        }
        .refresh-btn:hover { 
            background: #1976D2; 
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="card">
            <h1 class="header">🚌 Bus Tracker Dashboard</h1>
            <button class="refresh-btn" onclick="location.reload()">🔄 새로고침</button>
        </div>
        
        <div class="card">
            <h2>📊 실시간 통계 <span style="font-size: 0.7em; color: #666;">(%s)</span></h2>
            <div class="stats">
                <div class="stat-box">
                    <div class="stat-number">%d</div>
                    <div class="stat-label">총 버스 수</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">%d</div>
                    <div class="stat-label">API1 전용</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">%d</div>
                    <div class="stat-label">API2 전용</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">%d</div>
                    <div class="stat-label">통합 데이터</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">%d</div>
                    <div class="stat-label">추적 중 버스</div>
                </div>
            </div>
        </div>

        <div class="card">
            <h3>📋 API 테스트</h3>
            <p>아래 링크를 클릭하여 API를 테스트해보세요:</p>
            <ul>
                <li><a href="/api/status" target="_blank">시스템 상태</a></li>
                <li><a href="/api/statistics" target="_blank">통계 정보</a></li>
                <li><a href="/api/health" target="_blank">헬스체크</a></li>
                <li><a href="/api/config" target="_blank">설정 정보</a></li>
            </ul>
        </div>

        <div class="card">
            <h3>ℹ️ 시스템 정보</h3>
            <p><strong>운영 일자:</strong> %s</p>
            <p><strong>현재 시간:</strong> %s</p>
            <p><strong>오케스트레이터:</strong> %s</p>
            <p><strong>운영 시간:</strong> %02d:%02d ~ %02d:%02d</p>
        </div>
    </div>

    <script>
        // 10초마다 자동 새로고침
        setTimeout(function() {
            location.reload();
        }, 10000);
    </script>
</body>
</html>`,
		time.Now().Format("15:04:05"),
		totalBuses, api1Only, api2Only, both, trackedBuses,
		currentDate,
		time.Now().Format("2006-01-02 15:04:05"),
		func() string {
			if ws.orchestrator.IsRunning() {
				return "실행 중"
			}
			return "정지됨"
		}(),
		ws.config.OperatingStartHour, ws.config.OperatingStartMinute,
		ws.config.OperatingEndHour, ws.config.OperatingEndMinute,
	)

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, html)
}

// handleAPIStatus 상태 API 핸들러
func (ws *WebServer) handleAPIStatus(w http.ResponseWriter, r *http.Request) {
	totalBuses, api1Only, api2Only, both := ws.dataManager.GetStatistics()

	response := fmt.Sprintf(`{
		"success": true,
		"data": {
			"status": "running",
			"currentTime": "%s",
			"operatingDate": "%s",
			"orchestratorRunning": %t,
			"operatingTime": %t,
			"statistics": {
				"totalBuses": %d,
				"api1Only": %d,
				"api2Only": %d,
				"both": %d,
				"trackedBuses": %d
			}
		},
		"message": "시스템 상태 조회 성공"
	}`,
		time.Now().Format(time.RFC3339),
		ws.busTracker.GetCurrentOperatingDate(),
		ws.orchestrator.IsRunning(),
		ws.config.IsOperatingTime(time.Now()),
		totalBuses, api1Only, api2Only, both,
		ws.busTracker.GetTrackedBusCount(),
	)

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, response)
}

// handleAPIStatistics 통계 API 핸들러
func (ws *WebServer) handleAPIStatistics(w http.ResponseWriter, r *http.Request) {
	totalBuses, api1Only, api2Only, both := ws.dataManager.GetStatistics()
	trackedBuses := ws.busTracker.GetTrackedBusCount()
	dailyStats := ws.busTracker.GetDailyTripStatistics()

	response := fmt.Sprintf(`{
		"success": true,
		"data": {
			"busStatistics": {
				"totalBuses": %d,
				"api1Only": %d,
				"api2Only": %d,
				"both": %d,
				"trackedBuses": %d
			},
			"dailyTrips": %d,
			"operatingDate": "%s",
			"lastUpdate": "%s"
		},
		"message": "통계 정보 조회 성공"
	}`,
		totalBuses, api1Only, api2Only, both, trackedBuses,
		len(dailyStats),
		ws.busTracker.GetCurrentOperatingDate(),
		time.Now().Format(time.RFC3339),
	)

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, response)
}

// handleAPIHealth 헬스체크 API 핸들러
func (ws *WebServer) handleAPIHealth(w http.ResponseWriter, r *http.Request) {
	isHealthy := true
	checks := make(map[string]string)

	// 오케스트레이터 상태
	if ws.orchestrator.IsRunning() {
		checks["orchestrator"] = "healthy"
	} else {
		checks["orchestrator"] = "stopped"
		isHealthy = false
	}

	// 데이터 매니저 상태
	totalBuses, _, _, _ := ws.dataManager.GetStatistics()
	if totalBuses >= 0 {
		checks["dataManager"] = "healthy"
	} else {
		checks["dataManager"] = "error"
		isHealthy = false
	}

	status := "healthy"
	if !isHealthy {
		status = "unhealthy"
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	response := fmt.Sprintf(`{
		"success": %t,
		"data": {
			"status": "%s",
			"checks": {
				"orchestrator": "%s",
				"dataManager": "%s"
			},
			"timestamp": "%s"
		},
		"message": "헬스체크 완료"
	}`,
		isHealthy, status,
		checks["orchestrator"], checks["dataManager"],
		time.Now().Format(time.RFC3339),
	)

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, response)
}

// handleAPIConfig 설정 API 핸들러
func (ws *WebServer) handleAPIConfig(w http.ResponseWriter, r *http.Request) {

	// JSON 응답 (간단히 구현)
	response := fmt.Sprintf(`{
		"success": true,
		"data": {
			"serviceKey": {
				"configured": %t,
				"masked": "%s"
			},
			"apis": {
				"api1": {
					"enabled": %t,
					"routeCount": %d
				},
				"api2": {
					"enabled": %t,
					"routeCount": %d
				}
			},
			"operatingTime": "%02d:%02d ~ %02d:%02d"
		},
		"message": "설정 정보 조회 성공"
	}`,
		!utils.Validate.IsEmpty(ws.config.ServiceKey),
		utils.String.MaskSensitive(ws.config.ServiceKey, 6, 4),
		len(ws.config.API1Config.RouteIDs) > 0,
		len(ws.config.API1Config.RouteIDs),
		len(ws.config.API2Config.RouteIDs) > 0,
		len(ws.config.API2Config.RouteIDs),
		ws.config.OperatingStartHour, ws.config.OperatingStartMinute,
		ws.config.OperatingEndHour, ws.config.OperatingEndMinute,
	)

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, response)
}

// Start 웹 서버 시작
func (ws *WebServer) Start(port int) error {
	ws.server.Addr = fmt.Sprintf(":%d", port)
	ws.isRunning = true

	ws.logger.Infof("🌐 웹 서버 시작 중...")
	ws.logger.Infof("📊 대시보드: http://localhost:%d", port)
	ws.logger.Infof("📡 API 엔드포인트: http://localhost:%d/api/", port)

	return ws.server.ListenAndServe()
}

// Stop 웹 서버 정지
func (ws *WebServer) Stop() error {
	if !ws.isRunning {
		return nil
	}

	ws.isRunning = false
	ws.logger.Info("🛑 웹 서버 정지 중...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return ws.server.Shutdown(ctx)
}

// IsRunning 실행 상태 확인
func (ws *WebServer) IsRunning() bool {
	return ws.isRunning
}

// 에러 처리 헬퍼
func (ws *WebServer) handleError(w http.ResponseWriter, err error, message string) {
	statusCode := http.StatusInternalServerError

	// 에러 타입에 따른 상태 코드 결정
	if strings.Contains(err.Error(), "not found") {
		statusCode = http.StatusNotFound
	} else if strings.Contains(err.Error(), "unauthorized") {
		statusCode = http.StatusUnauthorized
	}

	w.WriteHeader(statusCode)
	w.Header().Set("Content-Type", "application/json")

	response := fmt.Sprintf(`{
		"success": false,
		"error": {
			"message": "%s",
			"details": "%s",
			"code": %d
		},
		"timestamp": "%s"
	}`, message, err.Error(), statusCode, time.Now().Format(time.RFC3339))

	fmt.Fprint(w, response)
}
