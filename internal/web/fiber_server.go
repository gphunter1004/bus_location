// internal/web/fiber_server.go - 강제 종료 기능 포함
package web

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/recover"
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

// FiberServer Fiber 기반 웹 서버 (강제 종료 기능 포함)
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
	engine := html.New("./internal/web/templates", ".html")
	engine.Reload(true) // 개발 중에는 true, 프로덕션에서는 false
	engine.Debug(true)  // 개발 중에는 true

	// Fiber 앱 생성 (개선된 설정)
	app := fiber.New(fiber.Config{
		Views:                        engine,
		ErrorHandler:                 customErrorHandler,
		DisableKeepalive:             false,
		ReadTimeout:                  30 * time.Second,
		WriteTimeout:                 30 * time.Second,
		IdleTimeout:                  120 * time.Second,
		Prefork:                      false, // 개발 중에는 false
		ServerHeader:                 "Bus-Tracker-Server",
		StrictRouting:                false,
		CaseSensitive:                false,
		UnescapePath:                 false,
		ETag:                         true,
		BodyLimit:                    4 * 1024 * 1024, // 4MB
		Concurrency:                  256 * 1024,
		DisableDefaultDate:           false,
		DisableDefaultContentType:    false,
		DisableHeaderNormalizing:     false,
		DisableStartupMessage:        false,
		AppName:                      "Bus Tracker",
		GETOnly:                      false,
		Network:                      "tcp",
		EnableTrustedProxyCheck:      false,
		TrustedProxies:               []string{},
		EnableIPValidation:           false,
		EnablePrintRoutes:            false,
		ColorScheme:                  fiber.DefaultColors,
		RequestMethods:               fiber.DefaultMethods,
		EnableSplittingOnParsers:     false,
		DisableDefaultErrorHandler:   false,
		StructTag:                    "json",
		StreamRequestBody:            false,
		DisablePreParseMultipartForm: false,
		ReduceMemoryUsage:            false,
		CompressedFileSuffix:         ".fiber.gz",
	})

	// 복구 미들웨어 추가 (패닉 방지)
	app.Use(recover.New(recover.Config{
		EnableStackTrace: true,
	}))

	// 글로벌 미들웨어 설정
	app.Use(middleware.CORSConfig())
	app.Use(middleware.RequestIDMiddleware())
	app.Use(middleware.RequestLogger())

	// 템플릿 핸들러 생성
	templateHandler, err := handlers.NewTemplateHandler(
		cfg, logger, orchestrator, busTracker, dataManager, "./internal/web/templates")
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

// setupRoutes 라우트 설정
func (fs *FiberServer) setupRoutes() {
	// 정적 파일 제공
	fs.app.Static("/static", "./internal/web/static", fiber.Static{
		Compress:      true,
		ByteRange:     true,
		Browse:        false,
		CacheDuration: 10 * time.Minute,
		MaxAge:        3600,
	})

	// 헬스체크 엔드포인트 (가장 먼저)
	fs.app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"status":    "healthy",
			"timestamp": time.Now(),
			"service":   "bus-tracker",
		})
	})

	// 루트 경로 처리
	fs.app.Get("/", func(c *fiber.Ctx) error {
		// Accept 헤더 확인
		accept := c.Get("Accept")
		if strings.Contains(accept, "application/json") {
			// JSON 요청인 경우 API 응답
			return c.JSON(fiber.Map{
				"message": "Bus Tracker API",
				"version": "1.0.0",
				"endpoints": fiber.Map{
					"dashboard":  "/dashboard",
					"monitoring": "/monitoring",
					"api_docs":   "/api-doc",
					"api_v1":     "/api/v1/",
					"health":     "/health",
				},
			})
		}
		// HTML 요청인 경우 대시보드로 리다이렉트
		return fs.templateHandler.HandleDashboard(c)
	})

	// 웹 페이지 라우트
	fs.setupWebRoutes()

	// API 라우트 설정
	fs.setupAPIRoutes()

	// 404 핸들러 (마지막에)
	fs.app.Use(func(c *fiber.Ctx) error {
		// API 요청인 경우
		if strings.HasPrefix(c.Path(), "/api/") {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error":   true,
				"message": "API 엔드포인트를 찾을 수 없습니다",
				"path":    c.Path(),
			})
		}

		// 웹 페이지 요청인 경우
		return c.Status(fiber.StatusNotFound).Render("error", fiber.Map{
			"Title":     "페이지를 찾을 수 없습니다",
			"Code":      404,
			"Message":   "요청한 페이지를 찾을 수 없습니다",
			"Details":   "URL을 확인하거나 홈페이지로 돌아가세요",
			"Timestamp": time.Now().Format("2006-01-02 15:04:05"),
			"Path":      c.Path(),
		})
	})
}

// setupWebRoutes 웹 페이지 라우트 설정
func (fs *FiberServer) setupWebRoutes() {
	// 메인 대시보드
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

	fs.logger.Infof("🌐 Fiber 웹 서버 시작 중... (포트: %d)", port)
	fs.logger.Infof("📊 대시보드: http://localhost:%d", port)
	fs.logger.Infof("📡 API 엔드포인트: http://localhost:%d/api/", port)

	// 서버 시작 전 로그
	fs.logger.Info("웹 서버 바인딩 시도 중...")

	return fs.app.Listen(address)
}

// Stop 웹 서버 정지 (강제 종료 포함)
func (fs *FiberServer) Stop() error {
	if !fs.isRunning {
		return nil
	}

	fs.isRunning = false
	fs.logger.Info("🛑 Fiber 웹 서버 정지 시작...")

	// Graceful shutdown 시도
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	shutdownDone := make(chan error, 1)

	// Graceful shutdown 시도
	go func() {
		fs.logger.Info("📝 Graceful shutdown 시도 중...")
		shutdownDone <- fs.app.Shutdown()
	}()

	select {
	case err := <-shutdownDone:
		if err != nil {
			fs.logger.Errorf("Graceful shutdown 실패: %v", err)
		} else {
			fs.logger.Info("✅ Graceful shutdown 성공")
			return nil
		}
	case <-shutdownCtx.Done():
		fs.logger.Warn("⚠️ Graceful shutdown 타임아웃 - 강제 종료 시도")
	}

	// 강제 종료 시도
	fs.logger.Info("🔨 강제 종료 시도 중...")
	forceCtx, forceCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer forceCancel()

	forceDone := make(chan error, 1)
	go func() {
		// FastHTTP 서버에 직접 접근하여 강제 종료
		if app := fs.app; app != nil {
			// Fiber의 내부 서버에 접근 (v2에서는 ShutdownWithContext 사용)
			forceDone <- app.ShutdownWithTimeout(3 * time.Second)
		} else {
			forceDone <- fmt.Errorf("앱이 이미 종료됨")
		}
	}()

	select {
	case err := <-forceDone:
		if err != nil {
			fs.logger.Errorf("강제 종료 실패: %v", err)
			return err
		} else {
			fs.logger.Info("✅ 강제 종료 성공")
			return nil
		}
	case <-forceCtx.Done():
		fs.logger.Error("❌ 강제 종료도 실패 - 프로세스 종료 권장")
		return fmt.Errorf("서버 종료 실패 - 수동 종료 필요")
	}
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
	return fs.templateHandler.ReloadTemplates("./internal/web/templates")
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

	// 강제 종료 엔드포인트 (개발용)
	dev.Post("/force-shutdown", func(c *fiber.Ctx) error {
		fs.logger.Warn("🔨 개발자 요청으로 강제 종료")

		// 비동기로 종료 처리
		go func() {
			time.Sleep(1 * time.Second) // 응답 전송 시간 확보
			if err := fs.Stop(); err != nil {
				fs.logger.Errorf("강제 종료 실패: %v", err)
				os.Exit(1)
			}
			os.Exit(0)
		}()

		return c.JSON(fiber.Map{
			"message": "강제 종료가 요청되었습니다",
			"status":  "shutting_down",
		})
	})

	fs.logger.Info("개발 모드 라우트가 활성화되었습니다 (/dev/*)")
}
