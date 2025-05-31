// internal/web/routes/routes.go
package routes

import (
	"strings"

	"github.com/gofiber/fiber/v2"

	"bus-tracker/config"
	"bus-tracker/internal/services"
	"bus-tracker/internal/services/api"
	"bus-tracker/internal/services/cache"
	"bus-tracker/internal/services/tracker"
	"bus-tracker/internal/utils"
	apiHandlers "bus-tracker/internal/web/handlers/api"
	"bus-tracker/internal/web/middleware"
	webServices "bus-tracker/internal/web/services"
)

// Router 라우터 구조체
type Router struct {
	app      *fiber.App
	config   *config.Config
	logger   *utils.Logger
	handlers *Handlers
}

// Handlers API 핸들러들
type Handlers struct {
	Status *apiHandlers.StatusHandler
	Config *apiHandlers.ConfigHandler
	// 추가 핸들러들은 필요에 따라 구현
}

// Dependencies 의존성 구조체
type Dependencies struct {
	Config       *config.Config
	Logger       *utils.Logger
	Orchestrator *services.MultiAPIOrchestrator
	BusTracker   *tracker.BusTrackerWithDuplicateCheck
	StationCache *cache.StationCacheService
	API1Client   *api.API1Client
	API2Client   *api.API2Client
	DataManager  services.UnifiedDataManagerInterface
}

// NewRouter 새로운 라우터 생성
func NewRouter(app *fiber.App, deps *Dependencies) *Router {
	// 웹 서비스 레이어 생성
	statusService := webServices.NewStatusService(
		deps.Config,
		deps.Logger,
		deps.Orchestrator,
		deps.BusTracker,
		deps.DataManager,
	)

	// 핸들러 생성
	handlers := &Handlers{
		Status: apiHandlers.NewStatusHandler(statusService),
		Config: apiHandlers.NewConfigHandler(deps.Config, deps.Logger),
	}

	return &Router{
		app:      app,
		config:   deps.Config,
		logger:   deps.Logger,
		handlers: handlers,
	}
}

// SetupRoutes 모든 라우트 설정
func (r *Router) SetupRoutes() {
	// API 라우트 설정
	r.SetupAPIV1Routes()

	// 웹 페이지 라우트 설정
	r.setupWebRoutes()

	// 정적 파일 라우트 설정
	r.setupStaticRoutes()

	// 404 핸들러 설정
	r.setup404Handler()
}

// SetupAPIV1Routes API v1 라우트 설정
func (r *Router) SetupAPIV1Routes() {
	// API v1 그룹 생성
	v1 := r.app.Group("/api/v1")

	// 공통 미들웨어 적용
	v1.Use(middleware.RequestLogger())
	v1.Use(middleware.APIKeyAuth())

	// 상태 관련 라우트
	status := v1.Group("/status")
	status.Get("/", r.handlers.Status.GetStatus)
	status.Get("/health", r.handlers.Status.GetHealthCheck)
	status.Get("/statistics", r.handlers.Status.GetStatistics)

	// 설정 관련 라우트
	config := v1.Group("/config")
	config.Get("/", r.handlers.Config.GetConfig)
	config.Put("/", r.handlers.Config.UpdateConfig)

	// 모니터링 라우트
	monitoring := v1.Group("/monitoring")
	monitoring.Get("/metrics", r.handlers.Status.GetMetrics)
	monitoring.Get("/logs", r.handlers.Status.GetLogs)
	monitoring.Get("/alerts", r.handlers.Status.GetAlerts)

	// 알림 관리 라우트
	notifications := v1.Group("/notifications")
	notifications.Get("/", r.handlers.Status.GetNotifications)
	notifications.Post("/", r.handlers.Status.CreateNotification)
	notifications.Put("/:id", r.handlers.Status.UpdateNotification)
	notifications.Delete("/:id", r.handlers.Status.DeleteNotification)
}

// setupWebRoutes 웹 페이지 라우트 설정
func (r *Router) setupWebRoutes() {
	// 메인 대시보드 (기존 handlers.go의 handleDashboard 사용)
	r.app.Get("/", r.handleDashboard)
	r.app.Get("/dashboard", r.handleDashboard)
}

// setupStaticRoutes 정적 파일 라우트 설정
func (r *Router) setupStaticRoutes() {
	// CSS, JS, 이미지 등 정적 파일 (현재는 기본 제공)
	r.app.Static("/static", "./static")
}

// setup404Handler 404 에러 핸들러 설정
func (r *Router) setup404Handler() {
	r.app.Use(func(c *fiber.Ctx) error {
		// API 경로인 경우 JSON 에러 응답
		if strings.HasPrefix(c.Path(), "/api/") {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error":   true,
				"message": "API 엔드포인트를 찾을 수 없습니다",
				"path":    c.Path(),
			})
		}

		// 웹 페이지인 경우 404 메시지
		return c.Status(fiber.StatusNotFound).SendString("페이지를 찾을 수 없습니다: " + c.Path())
	})
}

// handleDashboard 대시보드 핸들러 (기존 코드 재사용)
func (r *Router) handleDashboard(c *fiber.Ctx) error {
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
                <button class="btn" onclick="getStatus()">📊 상태 확인</button>
                <button class="btn" onclick="getHealth()">❤️ 헬스체크</button>
                <div id="controlResult"></div>
            </div>

            <div class="card">
                <h3>📋 시스템 정보</h3>
                <button class="btn" onclick="getConfig()">⚙️ 설정 조회</button>
                <button class="btn" onclick="getMetrics()">📈 메트릭</button>
                <div id="infoResult"></div>
            </div>
        </div>

        <div class="card">
            <h3>📋 API 테스트</h3>
            <p>아래 링크를 클릭하여 API를 테스트해보세요:</p>
            <ul>
                <li><a href="/api/v1/status" target="_blank">시스템 상태</a></li>
                <li><a href="/api/v1/status/statistics" target="_blank">통계 정보</a></li>
                <li><a href="/api/v1/status/health" target="_blank">헬스체크</a></li>
                <li><a href="/api/v1/config" target="_blank">설정 정보</a></li>
                <li><a href="/api/v1/monitoring/metrics" target="_blank">시스템 메트릭</a></li>
            </ul>
        </div>
    </div>

    <script>
        // 상태 조회
        function getStatus() {
            fetch('/api/v1/status')
                .then(response => response.json())
                .then(data => {
                    document.getElementById('controlResult').innerHTML = 
                        '<pre>' + JSON.stringify(data, null, 2) + '</pre>';
                })
                .catch(err => {
                    document.getElementById('controlResult').innerHTML = 
                        '<div style="color: red;">오류: ' + err.message + '</div>';
                });
        }

        // 헬스체크
        function getHealth() {
            fetch('/api/v1/status/health')
                .then(response => response.json())
                .then(data => {
                    document.getElementById('controlResult').innerHTML = 
                        '<pre>' + JSON.stringify(data, null, 2) + '</pre>';
                })
                .catch(err => {
                    document.getElementById('controlResult').innerHTML = 
                        '<div style="color: red;">오류: ' + err.message + '</div>';
                });
        }

        // 설정 조회
        function getConfig() {
            fetch('/api/v1/config')
                .then(response => response.json())
                .then(data => {
                    document.getElementById('infoResult').innerHTML = 
                        '<pre>' + JSON.stringify(data, null, 2) + '</pre>';
                })
                .catch(err => {
                    document.getElementById('infoResult').innerHTML = 
                        '<div style="color: red;">오류: ' + err.message + '</div>';
                });
        }

        // 메트릭 조회
        function getMetrics() {
            fetch('/api/v1/monitoring/metrics')
                .then(response => response.json())
                .then(data => {
                    document.getElementById('infoResult').innerHTML = 
                        '<pre>' + JSON.stringify(data, null, 2) + '</pre>';
                })
                .catch(err => {
                    document.getElementById('infoResult').innerHTML = 
                        '<div style="color: red;">오류: ' + err.message + '</div>';
                });
        }

        // 통계 업데이트
        function updateStatistics() {
            fetch('/api/v1/status/statistics')
                .then(response => response.json())
                .then(data => {
                    if (data.success && data.data) {
                        const stats = data.data.busStatistics;
                        document.getElementById('totalBuses').textContent = stats.totalBuses || 0;
                        document.getElementById('api1Only').textContent = stats.api1Only || 0;
                        document.getElementById('api2Only').textContent = stats.api2Only || 0;
                        document.getElementById('bothAPIs').textContent = stats.both || 0;
                        document.getElementById('trackedBuses').textContent = stats.trackedBuses || 0;
                        document.getElementById('updateTime').textContent = '(' + new Date().toLocaleTimeString() + ')';
                    }
                })
                .catch(err => console.error('통계 업데이트 실패:', err));
        }

        // 초기 로드 및 주기적 업데이트
        updateStatistics();
        setInterval(updateStatistics, 10000); // 10초마다 업데이트
    </script>
</body>
</html>
`
	c.Set("Content-Type", "text/html")
	return c.SendString(html)
}
