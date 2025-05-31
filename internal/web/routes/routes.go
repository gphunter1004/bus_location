// internal/web/routes/routes.go (Updated for Fiber)
package routes

import (
	"github.com/gofiber/fiber/v2"

	"bus-tracker/config"
	"bus-tracker/internal/services"
	"bus-tracker/internal/services/api"
	"bus-tracker/internal/services/cache"
	"bus-tracker/internal/services/tracker"
	"bus-tracker/internal/utils"
	"bus-tracker/internal/web/handlers"
	apiHandlers "bus-tracker/internal/web/handlers/api"
	"bus-tracker/internal/web/middleware"
	webServices "bus-tracker/internal/web/services"
)

// Router Fiber 라우터 구조체
type Router struct {
	app             *fiber.App
	config          *config.Config
	logger          *utils.Logger
	apiHandlers     *APIHandlers
	templateHandler *handlers.TemplateHandler
}

// APIHandlers API 핸들러들
type APIHandlers struct {
	Status   *apiHandlers.StatusHandler
	Config   *apiHandlers.ConfigHandler
	Control  *apiHandlers.ControlHandler
	Cache    *apiHandlers.CacheHandler
	Tracking *apiHandlers.TrackingHandler
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

	cacheService := webServices.NewCacheService(
		deps.Config,
		deps.Logger,
		deps.StationCache,
	)

	trackingService := webServices.NewTrackingService(
		deps.Config,
		deps.Logger,
		deps.BusTracker,
	)

	// API 핸들러 생성
	apiHandlers := &APIHandlers{
		Status:   apiHandlers.NewStatusHandler(statusService),
		Config:   apiHandlers.NewConfigHandler(deps.Config, deps.Logger),
		Control:  apiHandlers.NewControlHandler(deps.Orchestrator, deps.Logger),
		Cache:    apiHandlers.NewCacheHandler(cacheService),
		Tracking: apiHandlers.NewTrackingHandler(trackingService),
	}

	// 템플릿 핸들러 생성
	templateHandler, err := handlers.NewTemplateHandler(
		deps.Config,
		deps.Logger,
		deps.Orchestrator,
		deps.BusTracker,
		deps.DataManager,
		"./web/templates",
	)
	if err != nil {
		deps.Logger.Fatalf("템플릿 핸들러 생성 실패: %v", err)
	}

	return &Router{
		app:             app,
		config:          deps.Config,
		logger:          deps.Logger,
		apiHandlers:     apiHandlers,
		templateHandler: templateHandler,
	}
}

// SetupRoutes 모든 라우트 설정
func (r *Router) SetupRoutes() {
	// API 라우트 설정
	r.SetupAPIV1Routes()

	// 웹 페이지 라우트 설정 (이미 FiberServer에서 처리됨)
	// r.setupWebRoutes()

	// 정적 파일 라우트는 FiberServer에서 처리됨
}

// SetupAPIV1Routes API v1 라우트 설정
func (r *Router) SetupAPIV1Routes() {
	// API v1 그룹 생성
	v1 := r.app.Group("/api/v1")

	// 공통 미들웨어 적용
	v1.Use(middleware.APIKeyAuth())

	// 상태 관련 라우트
	status := v1.Group("/status")
	status.Get("/", r.apiHandlers.Status.GetStatus)
	status.Get("/health", r.apiHandlers.Status.GetHealthCheck)
	status.Get("/statistics", r.apiHandlers.Status.GetStatistics)

	// 설정 관련 라우트
	config := v1.Group("/config")
	config.Get("/", r.apiHandlers.Config.GetConfig)
	config.Put("/", r.apiHandlers.Config.UpdateConfig)
	config.Get("/operating-status", r.apiHandlers.Config.GetOperatingStatus)

	// 시스템 제어 라우트
	control := v1.Group("/control")
	control.Post("/start", r.apiHandlers.Control.StartSystem)
	control.Post("/stop", r.apiHandlers.Control.StopSystem)
	control.Post("/restart", r.apiHandlers.Control.RestartSystem)
	control.Get("/status", r.apiHandlers.Control.GetSystemStatus)
	control.Post("/maintenance", r.apiHandlers.Control.SetMaintenanceMode)
	control.Get("/info", r.apiHandlers.Control.GetSystemInfo)

	// 모니터링 라우트
	monitoring := v1.Group("/monitoring")
	monitoring.Get("/metrics", r.apiHandlers.Status.GetMetrics)
	monitoring.Get("/logs", r.apiHandlers.Status.GetLogs)
	monitoring.Get("/alerts", r.apiHandlers.Status.GetAlerts)

	// 캐시 관리 라우트
	cache := v1.Group("/cache")
	cache.Get("/stations", r.apiHandlers.Cache.GetStationCache)
	cache.Get("/stations/:routeId", r.apiHandlers.Cache.GetRouteStations)
	cache.Post("/stations/reload", r.apiHandlers.Cache.ReloadStationCache)
	cache.Delete("/stations", r.apiHandlers.Cache.ClearStationCache)
	cache.Get("/stats", r.apiHandlers.Cache.GetCacheStatistics)

	// 트래킹 관리 라우트
	tracking := v1.Group("/tracking")
	tracking.Get("/buses", r.apiHandlers.Tracking.GetTrackedBuses)
	tracking.Get("/buses/:plateNo", r.apiHandlers.Tracking.GetBusInfo)
	tracking.Delete("/buses/:plateNo", r.apiHandlers.Tracking.RemoveBusTracking)
	tracking.Get("/buses/:plateNo/history", r.apiHandlers.Tracking.GetBusHistory)
	tracking.Get("/trips/statistics", r.apiHandlers.Tracking.GetTripStatistics)
	tracking.Post("/trips/reset-counters", r.apiHandlers.Tracking.ResetTripCounters)
	tracking.Get("/trips/daily/:date", r.apiHandlers.Tracking.GetDailyTripStatistics)
	tracking.Get("/routes", r.apiHandlers.Tracking.GetActiveRoutes)
	tracking.Get("/routes/:routeId/buses", r.apiHandlers.Tracking.GetRouteBuses)
	tracking.Get("/routes/:routeId/statistics", r.apiHandlers.Tracking.GetRouteStatistics)

	// 알림 관리 라우트
	notifications := v1.Group("/notifications")
	notifications.Get("/", r.apiHandlers.Status.GetNotifications)
	notifications.Post("/", r.apiHandlers.Status.CreateNotification)
	notifications.Put("/:id", r.apiHandlers.Status.UpdateNotification)
	notifications.Delete("/:id", r.apiHandlers.Status.DeleteNotification)
}
