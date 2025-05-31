// internal/web/fiber_server.go
package web

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/template/html/v2"

	"bus-tracker/config"
	"bus-tracker/internal/services"
	"bus-tracker/internal/services/api"
	"bus-tracker/internal/services/cache"
	"bus-tracker/internal/services/tracker"
	"bus-tracker/internal/utils"
	"bus-tracker/internal/web/handlers"
	"bus-tracker/internal/web/middleware"
	"bus-tracker/internal/web/routes"
)

// customErrorHandler Fiber용 커스텀 에러 핸들러
func customErrorHandler(c *fiber.Ctx, err error) error {
	// 기본 상태 코드
	code := fiber.StatusInternalServerError

	// Fiber 에러인 경우 코드 추출
	if e, ok := err.(*fiber.Error); ok {
		code = e.Code
	}

	// API 요청인 경우 JSON 응답
	if strings.HasPrefix(c.Path(), "/api/") {
		return c.Status(code).JSON(fiber.Map{
			"error":     true,
			"message":   getErrorMessage(code),
			"details":   err.Error(),
			"code":      code,
			"timestamp": time.Now(),
			"path":      c.Path(),
		})
	}

	// 웹 페이지 요청인 경우 에러 페이지 렌더링
	return c.Status(code).Render("error", fiber.Map{
		"Title":     "오류 발생",
		"Code":      code,
		"Message":   getErrorMessage(code),
		"Details":   err.Error(),
		"Timestamp": time.Now().Format("2006-01-02 15:04:05"),
		"Path":      c.Path(),
	})
}

// getErrorMessage 상태 코드에 따른 사용자 친화적 메시지
func getErrorMessage(code int) string {
	switch code {
	case fiber.StatusBadRequest:
		return "잘못된 요청입니다"
	case fiber.StatusUnauthorized:
		return "인증이 필요합니다"
	case fiber.StatusForbidden:
		return "접근 권한이 없습니다"
	case fiber.StatusNotFound:
		return "요청한 리소스를 찾을 수 없습니다"
	case fiber.StatusMethodNotAllowed:
		return "허용되지 않은 메서드입니다"
	case fiber.StatusTooManyRequests:
		return "요청 한도를 초과했습니다"
	case fiber.StatusInternalServerError:
		return "서버 내부 오류가 발생했습니다"
	case fiber.StatusBadGateway:
		return "게이트웨이 오류입니다"
	case fiber.StatusServiceUnavailable:
		return "서비스를 일시적으로 사용할 수 없습니다"
	case fiber.StatusGatewayTimeout:
		return "게이트웨이 시간 초과입니다"
	default:
		return "예상치 못한 오류가 발생했습니다"
	}
}

// FiberServer Fiber 기반 웹 서버
type FiberServer struct {
	app             *fiber.App
	config          *config.Config
	logger          *utils.Logger
	isRunning       bool
	templateHandler *handlers.TemplateHandler

	// 의존성들
	orchestrator *services.MultiAPIOrchestrator
	busTracker   *tracker.BusTrackerWithDuplicateCheck
	stationCache *cache.StationCacheService
	api1Client   *api.API1Client
	api2Client   *api.API2Client
	dataManager  services.UnifiedDataManagerInterface
}

// NewFiberServer 새로운 Fiber 웹 서버 생성
func NewFiberServer(
	cfg *config.Config,
	logger *utils.Logger,
	orchestrator *services.MultiAPIOrchestrator,
	busTracker *tracker.BusTrackerWithDuplicateCheck,
	stationCache *cache.StationCacheService,
	api1Client *api.API1Client,
	api2Client *api.API2Client,
	dataManager services.UnifiedDataManagerInterface,
) *FiberServer {

	// 템플릿 엔진 설정
	engine := html.New("./web/templates", ".html")
	engine.Reload(true) // 개발 중에는 true, 프로덕션에서는 false
	engine.Debug(true)  // 개발 중에는 true

	// Fiber 앱 생성
	app := fiber.New(fiber.Config{
		Views:            engine,
		ErrorHandler:     customErrorHandler,
		DisableKeepalive: false,
		ReadTimeout:      30000,
		WriteTimeout:     30000,
		IdleTimeout:      120000,
		Prefork:          false, // 개발 중에는 false
	})

	// 글로벌 미들웨어 설정
	app.Use(middleware.CORSConfig())
	app.Use(middleware.RequestIDMiddleware())
	app.Use(middleware.RequestLogger())
	// 에러 미들웨어는 마지막에 추가하지 않음 (Fiber Config에서 처리)

	// 템플릿 핸들러 생성
	templateHandler, err := handlers.NewTemplateHandler(
		cfg, logger, orchestrator, busTracker, dataManager, "./web/templates")
	if err != nil {
		log.Fatalf("템플릿 핸들러 생성 실패: %v", err)
	}

	fs := &FiberServer{
		app:             app,
		config:          cfg,
		logger:          logger,
		isRunning:       false,
		templateHandler: templateHandler,
		orchestrator:    orchestrator,
		busTracker:      busTracker,
		stationCache:    stationCache,
		api1Client:      api1Client,
		api2Client:      api2Client,
		dataManager:     dataManager,
	}

	// 라우트 설정
	fs.setupRoutes()

	return fs
}

// setupRoutes 라우트 설정
func (fs *FiberServer) setupRoutes() {
	// 정적 파일 제공
	fs.app.Static("/static", "./web/static")

	// 웹 페이지 라우트
	fs.setupWebRoutes()

	// API 라우트 설정
	fs.setupAPIRoutes()

	// 404 핸들러
	fs.app.Use(func(c *fiber.Ctx) error {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error":   true,
			"message": "페이지를 찾을 수 없습니다",
			"path":    c.Path(),
		})
	})
}

// setupWebRoutes 웹 페이지 라우트 설정
func (fs *FiberServer) setupWebRoutes() {
	// 메인 대시보드
	fs.app.Get("/", fs.templateHandler.HandleDashboard)
	fs.app.Get("/dashboard", fs.templateHandler.HandleDashboard)

	// 모니터링 페이지
	fs.app.Get("/monitoring", fs.templateHandler.HandleMonitoring)

	// API 문서 페이지
	fs.app.Get("/api-doc", fs.templateHandler.HandleAPIDoc)
}

// setupAPIRoutes API 라우트 설정
func (fs *FiberServer) setupAPIRoutes() {
	// 라우터 의존성 준비
	deps := &routes.Dependencies{
		Config:       fs.config,
		Logger:       fs.logger,
		Orchestrator: fs.orchestrator,
		BusTracker:   fs.busTracker,
		StationCache: fs.stationCache,
		API1Client:   fs.api1Client,
		API2Client:   fs.api2Client,
		DataManager:  fs.dataManager,
	}

	// 라우터 생성 및 설정
	router := routes.NewRouter(fs.app, deps)
	router.SetupRoutes()
}

// Start 웹 서버 시작
func (fs *FiberServer) Start(port int) error {
	fs.isRunning = true

	address := fmt.Sprintf(":%d", port)

	fs.logger.Infof("🌐 Fiber 웹 서버 시작 중...")
	fs.logger.Infof("📊 대시보드: http://localhost:%d", port)
	fs.logger.Infof("📡 API 엔드포인트: http://localhost:%d/api/", port)

	return fs.app.Listen(address)
}

// Stop 웹 서버 정지
func (fs *FiberServer) Stop() error {
	if !fs.isRunning {
		return nil
	}

	fs.isRunning = false
	fs.logger.Info("🛑 Fiber 웹 서버 정지 중...")

	return fs.app.Shutdown()
}

// IsRunning 실행 상태 확인
func (fs *FiberServer) IsRunning() bool {
	return fs.isRunning
}

// GetApp Fiber 앱 인스턴스 반환 (테스트용)
func (fs *FiberServer) GetApp() *fiber.App {
	return fs.app
}

// ReloadTemplates 템플릿 재로드 (개발 중 유용)
func (fs *FiberServer) ReloadTemplates() error {
	return fs.templateHandler.ReloadTemplates("./web/templates")
}

// SetupDevelopmentMode 개발 모드 설정
func (fs *FiberServer) SetupDevelopmentMode() {
	// 개발 환경에서만 사용할 추가 라우트
	dev := fs.app.Group("/dev")

	// 템플릿 재로드 엔드포인트
	dev.Post("/reload-templates", func(c *fiber.Ctx) error {
		err := fs.ReloadTemplates()
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error":   true,
				"message": "템플릿 재로드 실패",
				"details": err.Error(),
			})
		}

		return c.JSON(fiber.Map{
			"success": true,
			"message": "템플릿이 성공적으로 재로드되었습니다",
		})
	})

	// 환경변수 확인 엔드포인트
	dev.Get("/env", func(c *fiber.Ctx) error {
		envVars := map[string]string{
			"LOG_LEVEL":         os.Getenv("LOG_LEVEL"),
			"WEB_PORT":          os.Getenv("WEB_PORT"),
			"ELASTICSEARCH_URL": os.Getenv("ELASTICSEARCH_URL"),
			"SERVICE_KEY":       utils.String.MaskSensitive(os.Getenv("SERVICE_KEY"), 6, 4),
			"API1_ROUTE_IDS":    os.Getenv("API1_ROUTE_IDS"),
			"API2_ROUTE_IDS":    os.Getenv("API2_ROUTE_IDS"),
		}

		return c.JSON(fiber.Map{
			"environment": envVars,
			"timestamp":   time.Now(),
		})
	})

	fs.logger.Info("개발 모드 라우트가 활성화되었습니다 (/dev/*)")
}
