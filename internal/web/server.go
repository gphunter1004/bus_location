package web

import (
	"context"
	"fmt"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	fiberLogger "github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"

	"bus-tracker/config"
	"bus-tracker/internal/services"
	"bus-tracker/internal/services/api"
	"bus-tracker/internal/services/cache"
	"bus-tracker/internal/services/tracker"
	"bus-tracker/internal/utils"
)

// WebServer Fiber 웹 서버
type WebServer struct {
	app          *fiber.App
	config       *config.Config
	logger       *utils.Logger
	orchestrator *services.MultiAPIOrchestrator
	busTracker   *tracker.BusTrackerWithDuplicateCheck
	stationCache *cache.StationCacheService
	api1Client   *api.API1Client
	api2Client   *api.API2Client
	dataManager  services.UnifiedDataManagerInterface

	// 서버 제어
	server    *fiber.App
	isRunning bool
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
	app := fiber.New(fiber.Config{
		AppName:      "Bus Tracker Web UI",
		ErrorHandler: customErrorHandler,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	})

	// 미들웨어 설정
	app.Use(recover.New())
	app.Use(fiberLogger.New(fiberLogger.Config{
		Format: "[${time}] ${status} - ${method} ${path} (${latency})\n",
	}))
	app.Use(cors.New(cors.Config{
		AllowOrigins: "*",
		AllowHeaders: "Origin, Content-Type, Accept",
		AllowMethods: "GET, POST, PUT, DELETE",
	}))

	ws := &WebServer{
		app:          app,
		config:       cfg,
		logger:       logger,
		orchestrator: orchestrator,
		busTracker:   busTracker,
		stationCache: stationCache,
		api1Client:   api1Client,
		api2Client:   api2Client,
		dataManager:  dataManager,
	}

	ws.setupRoutes()
	return ws
}

// setupRoutes 라우트 설정
func (ws *WebServer) setupRoutes() {
	// 메인 대시보드
	ws.app.Get("/", ws.handleDashboard)

	// API 그룹
	api := ws.app.Group("/api/v1")

	// 상태 조회
	api.Get("/status", ws.handleStatus)
	api.Get("/statistics", ws.handleStatistics)
	api.Get("/config", ws.handleGetConfig)

	// 캐시 관리
	cache := api.Group("/cache")
	cache.Get("/stations", ws.handleGetStationCache)
	cache.Get("/stations/:routeId", ws.handleGetRouteStations)
	cache.Post("/stations/reload", ws.handleReloadStationCache)
	cache.Delete("/stations", ws.handleClearStationCache)

	// 버스 트래킹
	tracking := api.Group("/tracking")
	tracking.Get("/buses", ws.handleGetTrackedBuses)
	tracking.Get("/buses/:plateNo", ws.handleGetBusInfo)
	tracking.Delete("/buses/:plateNo", ws.handleRemoveBusTracking)
	tracking.Post("/buses/reset-counters", ws.handleResetTripCounters)
	tracking.Get("/trips/statistics", ws.handleGetTripStatistics)

	// 데이터 관리
	data := api.Group("/data")
	data.Get("/unified", ws.handleGetUnifiedData)
	data.Post("/cleanup", ws.handleDataCleanup)

	// 조작 API
	control := api.Group("/control")
	control.Post("/start", ws.handleStart)
	control.Post("/stop", ws.handleStop)
	control.Get("/health", ws.handleHealthCheck)
}

// Start 웹 서버 시작
func (ws *WebServer) Start(port int) error {
	ws.isRunning = true
	ws.logger.Infof("웹 서버 시작 - 포트: %d", port)
	ws.logger.Infof("대시보드: http://localhost:%d", port)
	ws.logger.Infof("API 문서: http://localhost:%d/api/v1/status", port)

	return ws.app.Listen(fmt.Sprintf(":%d", port))
}

// Stop 웹 서버 정지
func (ws *WebServer) Stop() error {
	if !ws.isRunning {
		return nil
	}

	ws.isRunning = false
	ws.logger.Info("웹 서버 정지 중...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return ws.app.ShutdownWithContext(ctx)
}

// 대시보드 핸들러
func (ws *WebServer) handleDashboard(c *fiber.Ctx) error {
	html := `
<!DOCTYPE html>
<html>
<head>
    <title>Bus Tracker Dashboard</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; }
        .card { background: white; padding: 20px; margin: 10px 0; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .header { text-align: center; color: #333; }
        .status { display: flex; gap: 20px; flex-wrap: wrap; }
        .stat-box { flex: 1; min-width: 200px; text-align: center; padding: 15px; background: #e8f4fd; border-radius: 5px; }
        .stat-number { font-size: 2em; font-weight: bold; color: #2196F3; }
        .stat-label { color: #666; margin-top: 5px; }
        .api-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
        .btn { background: #2196F3; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; margin: 5px; }
        .btn:hover { background: #1976D2; }
        .btn.danger { background: #f44336; }
        .btn.danger:hover { background: #d32f2f; }
        .btn.success { background: #4CAF50; }
        .btn.success:hover { background: #45a049; }
        pre { background: #f8f8f8; padding: 10px; border-radius: 4px; overflow-x: auto; }
        .refresh-btn { float: right; }
    </style>
</head>
<body>
    <div class="container">
        <div class="card">
            <h1 class="header">🚌 Bus Tracker Dashboard</h1>
            <button class="btn refresh-btn" onclick="location.reload()">🔄 새로고침</button>
        </div>
        
        <div class="card">
            <h2>📊 실시간 통계 <span id="updateTime"></span></h2>
            <div class="status" id="statistics">
                <div class="stat-box">
                    <div class="stat-number" id="totalBuses">-</div>
                    <div class="stat-label">총 버스 수</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number" id="api1Only">-</div>
                    <div class="stat-label">API1 전용</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number" id="api2Only">-</div>
                    <div class="stat-label">API2 전용</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number" id="bothAPIs">-</div>
                    <div class="stat-label">통합 데이터</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number" id="trackedBuses">-</div>
                    <div class="stat-label">추적 중 버스</div>
                </div>
            </div>
        </div>

        <div class="api-grid">
            <div class="card">
                <h3>🔧 시스템 제어</h3>
                <button class="btn success" onclick="controlAction('start')">▶️ 시작</button>
                <button class="btn danger" onclick="controlAction('stop')">⏹️ 정지</button>
                <button class="btn" onclick="controlAction('health')">❤️ 상태확인</button>
                <div id="controlResult"></div>
            </div>

            <div class="card">
                <h3>🗃️ 캐시 관리</h3>
                <button class="btn" onclick="cacheAction('reload')">🔄 캐시 새로고침</button>
                <button class="btn danger" onclick="cacheAction('clear')">🗑️ 캐시 삭제</button>
                <button class="btn" onclick="showStationCache()">📍 정류소 목록</button>
                <div id="cacheResult"></div>
            </div>

            <div class="card">
                <h3>🚌 버스 트래킹</h3>
                <button class="btn" onclick="trackingAction('reset')">🔄 차수 리셋</button>
                <button class="btn" onclick="trackingAction('cleanup')">🧹 데이터 정리</button>
                <button class="btn" onclick="showTrackedBuses()">📋 추적 버스 목록</button>
                <div id="trackingResult"></div>
            </div>
        </div>

        <div class="card">
            <h3>📋 최근 로그</h3>
            <pre id="logs">시스템 로그가 여기에 표시됩니다...</pre>
        </div>
    </div>

    <script>
        // 실시간 업데이트
        function updateStatistics() {
            fetch('/api/v1/statistics')
                .then(response => response.json())
                .then(data => {
                    document.getElementById('totalBuses').textContent = data.totalBuses || 0;
                    document.getElementById('api1Only').textContent = data.api1Only || 0;
                    document.getElementById('api2Only').textContent = data.api2Only || 0;
                    document.getElementById('bothAPIs').textContent = data.both || 0;
                    document.getElementById('trackedBuses').textContent = data.trackedBuses || 0;
                    document.getElementById('updateTime').textContent = '(' + new Date().toLocaleTimeString() + ')';
                })
                .catch(err => console.error('통계 업데이트 실패:', err));
        }

        // 시스템 제어
        function controlAction(action) {
            const resultDiv = document.getElementById('controlResult');
            fetch('/api/v1/control/' + action, { method: 'POST' })
                .then(response => response.json())
                .then(data => {
                    resultDiv.innerHTML = '<div style="color: green; margin-top: 10px;">✅ ' + data.message + '</div>';
                })
                .catch(err => {
                    resultDiv.innerHTML = '<div style="color: red; margin-top: 10px;">❌ 오류: ' + err.message + '</div>';
                });
        }

        // 캐시 관리
        function cacheAction(action) {
            const resultDiv = document.getElementById('cacheResult');
            const endpoint = action === 'reload' ? '/api/v1/cache/stations/reload' : '/api/v1/cache/stations';
            const method = action === 'reload' ? 'POST' : 'DELETE';
            
            fetch(endpoint, { method: method })
                .then(response => response.json())
                .then(data => {
                    resultDiv.innerHTML = '<div style="color: green; margin-top: 10px;">✅ ' + data.message + '</div>';
                    updateStatistics();
                })
                .catch(err => {
                    resultDiv.innerHTML = '<div style="color: red; margin-top: 10px;">❌ 오류: ' + err.message + '</div>';
                });
        }

        // 트래킹 관리
        function trackingAction(action) {
            const resultDiv = document.getElementById('trackingResult');
            const endpoint = action === 'reset' ? '/api/v1/tracking/buses/reset-counters' : '/api/v1/data/cleanup';
            
            fetch(endpoint, { method: 'POST' })
                .then(response => response.json())
                .then(data => {
                    resultDiv.innerHTML = '<div style="color: green; margin-top: 10px;">✅ ' + data.message + '</div>';
                    updateStatistics();
                })
                .catch(err => {
                    resultDiv.innerHTML = '<div style="color: red; margin-top: 10px;">❌ 오류: ' + err.message + '</div>';
                });
        }

        // 정류소 캐시 표시
        function showStationCache() {
            fetch('/api/v1/cache/stations')
                .then(response => response.json())
                .then(data => {
                    const resultDiv = document.getElementById('cacheResult');
                    
                    // 안전한 속성 접근을 위한 체크
                    const summary = data.summary || {};
                    const detailedData = data.detailedData || {};
                    const routeLists = data.routeLists || {};
                    
                    // 더 보기 좋은 형태로 표시
                    let html = '<div style="max-height: 400px; overflow-y: auto; border: 1px solid #ddd; padding: 10px; border-radius: 4px; background: #f9f9f9;">';
                    
                    // 요약 정보
                    html += '<h4>📊 캐시 요약</h4>';
                    html += '<p><strong>총 노선:</strong> ' + (summary.totalRoutes || 0) + '개 | <strong>총 정류소:</strong> ' + (summary.totalStations || 0) + '개</p>';
                    html += '<p><strong>API1 노선:</strong> ' + (summary.api1Count || 0) + '개 | <strong>API2 노선:</strong> ' + (summary.api2Count || 0) + '개</p>';
                    
                    // API1 노선 상세
                    if (detailedData.api1Routes && Object.keys(detailedData.api1Routes).length > 0) {
                        html += '<h4>🔵 API1 노선 (경기도 API)</h4>';
                        html += '<table style="width: 100%; border-collapse: collapse; margin-bottom: 15px;">';
                        html += '<tr style="background: #e3f2fd;"><th style="border: 1px solid #ddd; padding: 8px;">노선ID</th><th style="border: 1px solid #ddd; padding: 8px;">정류소 수</th><th style="border: 1px solid #ddd; padding: 8px;">타입</th></tr>';
                        
                        for (const [routeId, info] of Object.entries(detailedData.api1Routes)) {
                            const routeInfo = info || {};
                            html += '<tr><td style="border: 1px solid #ddd; padding: 8px;">' + routeId + '</td>';
                            html += '<td style="border: 1px solid #ddd; padding: 8px; text-align: center;">' + (routeInfo.stationCount || 0) + '개</td>';
                            html += '<td style="border: 1px solid #ddd; padding: 8px; text-align: center;">' + (routeInfo.format || '숫자형') + '</td></tr>';
                        }
                        html += '</table>';
                    } else {
                        html += '<h4>🔵 API1 노선 (경기도 API)</h4>';
                        html += '<p style="color: #666;">설정된 API1 노선이 없습니다.</p>';
                    }
                    
                    // API2 노선 상세
                    if (detailedData.api2Routes && Object.keys(detailedData.api2Routes).length > 0) {
                        html += '<h4>🟢 API2 노선 (공공데이터포털)</h4>';
                        html += '<table style="width: 100%; border-collapse: collapse; margin-bottom: 15px;">';
                        html += '<tr style="background: #e8f5e8;"><th style="border: 1px solid #ddd; padding: 8px;">노선ID</th><th style="border: 1px solid #ddd; padding: 8px;">정류소 수</th><th style="border: 1px solid #ddd; padding: 8px;">타입</th></tr>';
                        
                        for (const [routeId, info] of Object.entries(detailedData.api2Routes)) {
                            const routeInfo = info || {};
                            html += '<tr><td style="border: 1px solid #ddd; padding: 8px;">' + routeId + '</td>';
                            html += '<td style="border: 1px solid #ddd; padding: 8px; text-align: center;">' + (routeInfo.stationCount || 0) + '개</td>';
                            html += '<td style="border: 1px solid #ddd; padding: 8px; text-align: center;">' + (routeInfo.format || 'GGB형식') + '</td></tr>';
                        }
                        html += '</table>';
                    } else {
                        html += '<h4>🟢 API2 노선 (공공데이터포털)</h4>';
                        html += '<p style="color: #666;">설정된 API2 노선이 없습니다.</p>';
                    }
                    
                    // 노선 목록
                    html += '<h4>📋 설정된 노선 목록</h4>';
                    if (routeLists.api1RouteIDs && routeLists.api1RouteIDs.length > 0) {
                        html += '<p><strong>API1:</strong> ' + routeLists.api1RouteIDs.join(', ') + '</p>';
                    } else {
                        html += '<p><strong>API1:</strong> <span style="color: #666;">설정된 노선 없음</span></p>';
                    }
                    
                    if (routeLists.api2RouteIDs && routeLists.api2RouteIDs.length > 0) {
                        html += '<p><strong>API2:</strong> ' + routeLists.api2RouteIDs.join(', ') + '</p>';
                    } else {
                        html += '<p><strong>API2:</strong> <span style="color: #666;">설정된 노선 없음</span></p>';
                    }
                    
                    // 디버깅 정보 (개발용)
                    if (!data.summary && !data.detailedData) {
                        html += '<div style="background: #ffebee; padding: 10px; border-radius: 4px; margin: 10px 0;">';
                        html += '<h5>🔍 디버깅 정보</h5>';
                        html += '<p>서버 응답 구조:</p>';
                        html += '<pre style="font-size: 12px; max-height: 100px; overflow: auto;">' + JSON.stringify(data, null, 2) + '</pre>';
                        html += '</div>';
                    }
                    
                    html += '<p><small>마지막 업데이트: ' + new Date(data.lastUpdate || Date.now()).toLocaleString() + '</small></p>';
                    html += '</div>';
                    
                    resultDiv.innerHTML = html;
                })
                .catch(err => {
                    console.error('캐시 조회 오류:', err);
                    document.getElementById('cacheResult').innerHTML = 
                        '<div style="color: red;">❌ 캐시 조회 실패: ' + err.message + '<br><small>자세한 내용은 브라우저 콘솔을 확인하세요.</small></div>';
                });
        }

        // 추적 버스 목록 표시
        function showTrackedBuses() {
            fetch('/api/v1/tracking/buses')
                .then(response => response.json())
                .then(data => {
                    const resultDiv = document.getElementById('trackingResult');
                    
                    let html = '<div style="max-height: 400px; overflow-y: auto; border: 1px solid #ddd; padding: 10px; border-radius: 4px; background: #f9f9f9;">';
                    html += '<h4>🚌 추적 중인 버스 현황</h4>';
                    html += '<p><strong>총 추적 버스:</strong> ' + data.totalTracked + '대</p>';
                    
                    if (data.totalTracked > 0) {
                        html += '<p>추적 중인 버스들이 정상적으로 모니터링되고 있습니다.</p>';
                        html += '<div style="background: #e8f4fd; padding: 10px; border-radius: 4px; margin: 10px 0;">';
                        html += '<h5>📊 추적 정보</h5>';
                        html += '<p>• 실시간 위치 업데이트 중</p>';
                        html += '<p>• 정류장 변경 감지 활성</p>';
                        html += '<p>• 운행 차수 자동 기록</p>';
                        html += '</div>';
                        
                        // 운행 통계도 함께 조회
                        fetch('/api/v1/tracking/trips/statistics')
                            .then(res => res.json())
                            .then(tripData => {
                                if (tripData.success && tripData.totalBuses > 0) {
                                    html += '<h5>📈 일일 운행 통계</h5>';
                                    html += '<table style="width: 100%; border-collapse: collapse; margin-top: 10px;">';
                                    html += '<tr style="background: #f0f0f0;"><th style="border: 1px solid #ddd; padding: 8px;">차량번호</th><th style="border: 1px solid #ddd; padding: 8px;">운행 차수</th></tr>';
                                    
                                    for (const [plateNo, tripCount] of Object.entries(tripData.statistics)) {
                                        html += '<tr><td style="border: 1px solid #ddd; padding: 8px;">' + plateNo + '</td>';
                                        html += '<td style="border: 1px solid #ddd; padding: 8px; text-align: center;">' + tripCount + '차수</td></tr>';
                                    }
                                    html += '</table>';
                                }
                                html += '<p><small>마지막 업데이트: ' + new Date(data.lastUpdate).toLocaleString() + '</small></p>';
                                html += '</div>';
                                resultDiv.innerHTML = html;
                            })
                            .catch(() => {
                                html += '<p><small>마지막 업데이트: ' + new Date(data.lastUpdate).toLocaleString() + '</small></p>';
                                html += '</div>';
                                resultDiv.innerHTML = html;
                            });
                    } else {
                        html += '<p style="color: #666;">현재 추적 중인 버스가 없습니다.</p>';
                        html += '<div style="background: #fff3cd; padding: 10px; border-radius: 4px; margin: 10px 0;">';
                        html += '<p>💡 <strong>참고:</strong></p>';
                        html += '<p>• 운영 시간 내에 버스가 운행해야 추적이 시작됩니다</p>';
                        html += '<p>• API에서 버스 데이터를 받으면 자동으로 추적을 시작합니다</p>';
                        html += '</div>';
                        html += '</div>';
                        resultDiv.innerHTML = html;
                    }
                })
                .catch(err => {
                    document.getElementById('trackingResult').innerHTML = 
                        '<div style="color: red;">❌ 버스 목록 조회 실패: ' + err.message + '</div>';
                });
        }

        // 초기 로드 및 주기적 업데이트
        updateStatistics();
        setInterval(updateStatistics, 5000); // 5초마다 업데이트
    </script>
</body>
</html>
`
	c.Set("Content-Type", "text/html")
	return c.SendString(html)
}

// 에러 핸들러
func customErrorHandler(c *fiber.Ctx, err error) error {
	code := fiber.StatusInternalServerError
	if e, ok := err.(*fiber.Error); ok {
		code = e.Code
	}

	return c.Status(code).JSON(fiber.Map{
		"error":   true,
		"message": err.Error(),
		"code":    code,
	})
}
