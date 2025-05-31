// internal/web/server.go
package web

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"

	"bus-tracker/config"
	"bus-tracker/internal/services"
	"bus-tracker/internal/services/api"
	"bus-tracker/internal/services/cache"
	"bus-tracker/internal/services/tracker"
	"bus-tracker/internal/utils"
	"bus-tracker/internal/web/routes"
)

// WebServer 웹 서버 구조체
type WebServer struct {
	app       *fiber.App
	config    *config.Config
	logger    *utils.Logger
	router    *routes.Router
	isRunning bool
}

// NewWebServer 새로운 웹 서버 생성
func NewWebServer(
	cfg *config.Config,
	webLogger *utils.Logger,
	orchestrator *services.MultiAPIOrchestrator,
	busTracker *tracker.BusTrackerWithDuplicateCheck,
	stationCache *cache.StationCacheService,
	api1Client *api.API1Client,
	api2Client *api.API2Client,
	dataManager services.UnifiedDataManagerInterface,
) *WebServer {
	// Fiber 앱 생성 (템플릿 엔진 제거)
	app := fiber.New(fiber.Config{
		AppName:       "Bus Tracker Web Service",
		ErrorHandler:  customErrorHandler,
		ReadTimeout:   30 * time.Second,
		WriteTimeout:  30 * time.Second,
		IdleTimeout:   120 * time.Second,
		ServerHeader:  "Bus-Tracker/1.0",
		StrictRouting: false,
		CaseSensitive: false,
	})

	// 글로벌 미들웨어 설정
	setupGlobalMiddleware(app, webLogger)

	// 의존성 구성
	deps := &routes.Dependencies{
		Config:       cfg,
		Logger:       webLogger,
		Orchestrator: orchestrator,
		BusTracker:   busTracker,
		StationCache: stationCache,
		API1Client:   api1Client,
		API2Client:   api2Client,
		DataManager:  dataManager,
	}

	// 라우터 생성 및 설정
	router := routes.NewRouter(app, deps)
	router.SetupRoutes()

	return &WebServer{
		app:    app,
		config: cfg,
		logger: webLogger,
		router: router,
	}
}

// setupGlobalMiddleware 글로벌 미들웨어 설정
func setupGlobalMiddleware(app *fiber.App, webLogger *utils.Logger) {
	// Panic Recovery
	app.Use(recover.New(recover.Config{
		EnableStackTrace: true,
	}))

	// CORS 설정
	app.Use(cors.New(cors.Config{
		AllowOrigins:     "*",
		AllowMethods:     "GET,POST,HEAD,PUT,DELETE,PATCH,OPTIONS",
		AllowHeaders:     "Origin,Content-Type,Accept,Authorization,X-Requested-With",
		AllowCredentials: false,
		MaxAge:           86400, // 24시간
	}))

	// Request 로깅
	app.Use(logger.New(logger.Config{
		Format:     "[${time}] ${status} - ${method} ${path} (${latency}) ${ip} \"${ua}\"\n",
		TimeFormat: "2006-01-02 15:04:05",
		TimeZone:   "Asia/Seoul",
		Output:     webLogger,
	}))

	// 보안 헤더 설정
	app.Use(func(c *fiber.Ctx) error {
		c.Set("X-Content-Type-Options", "nosniff")
		c.Set("X-Frame-Options", "SAMEORIGIN")
		c.Set("X-XSS-Protection", "1; mode=block")
		c.Set("Referrer-Policy", "strict-origin-when-cross-origin")

		// API가 아닌 경우 CSP 헤더 추가
		if !strings.HasPrefix(c.Path(), "/api/") {
			c.Set("Content-Security-Policy", "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline';")
		}

		return c.Next()
	})

	// Request ID 추가
	app.Use(func(c *fiber.Ctx) error {
		requestID := c.Get("X-Request-ID")
		if requestID == "" {
			requestID = generateRequestID()
			c.Set("X-Request-ID", requestID)
		}
		c.Locals("requestId", requestID)
		return c.Next()
	})
}

// customErrorHandler 커스텀 에러 핸들러
func customErrorHandler(c *fiber.Ctx, err error) error {
	code := fiber.StatusInternalServerError

	// Fiber 에러인 경우 상태 코드 추출
	if e, ok := err.(*fiber.Error); ok {
		code = e.Code
	}

	// API 요청인 경우 JSON 응답
	if strings.HasPrefix(c.Path(), "/api/") {
		return c.Status(code).JSON(fiber.Map{
			"error":     true,
			"message":   err.Error(),
			"code":      code,
			"timestamp": time.Now(),
			"path":      c.Path(),
		})
	}

	// 웹 페이지 요청인 경우 에러 페이지 HTML
	errorHTML := fmt.Sprintf(`
<!DOCTYPE html>
<html>
<head>
    <title>오류 발생</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 50px; text-align: center; }
        .error-container { max-width: 600px; margin: 0 auto; }
        .error-code { font-size: 4em; color: #e74c3c; }
        .error-message { font-size: 1.5em; margin: 20px 0; }
        .error-details { background: #f8f9fa; padding: 20px; border-radius: 5px; margin: 20px 0; }
    </style>
</head>
<body>
    <div class="error-container">
        <div class="error-code">%d</div>
        <div class="error-message">%s</div>
        <div class="error-details">
            <p><strong>경로:</strong> %s</p>
            <p><strong>시간:</strong> %s</p>
            <p><strong>세부사항:</strong> %s</p>
        </div>
        <a href="/">메인 페이지로 돌아가기</a>
    </div>
</body>
</html>`, code, getErrorMessage(code), c.Path(), time.Now().Format("2006-01-02 15:04:05"), err.Error())

	return c.Status(code).Type("html").SendString(errorHTML)
}

// getErrorMessage 상태 코드에 따른 사용자 친화적 메시지
func getErrorMessage(code int) string {
	switch code {
	case fiber.StatusNotFound:
		return "요청하신 페이지를 찾을 수 없습니다."
	case fiber.StatusInternalServerError:
		return "서버 내부 오류가 발생했습니다."
	case fiber.StatusBadRequest:
		return "잘못된 요청입니다."
	case fiber.StatusUnauthorized:
		return "인증이 필요합니다."
	case fiber.StatusForbidden:
		return "접근 권한이 없습니다."
	case fiber.StatusServiceUnavailable:
		return "서비스를 일시적으로 사용할 수 없습니다."
	default:
		return "예상치 못한 오류가 발생했습니다."
	}
}

// generateRequestID 요청 ID 생성
func generateRequestID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// Start 웹 서버 시작
func (ws *WebServer) Start(port int) error {
	ws.isRunning = true

	ws.logger.Infof("🌐 웹 서버 시작 중...")
	ws.logger.Infof("📊 대시보드: http://localhost:%d", port)
	ws.logger.Infof("🔧 관리자 페이지: http://localhost:%d/admin", port)
	ws.logger.Infof("📡 API 엔드포인트: http://localhost:%d/api/v1", port)
	ws.logger.Infof("📈 실시간 모니터링: http://localhost:%d/realtime", port)

	return ws.app.Listen(fmt.Sprintf(":%d", port))
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

	return ws.app.ShutdownWithContext(ctx)
}

// IsRunning 실행 상태 확인
func (ws *WebServer) IsRunning() bool {
	return ws.isRunning
}

// GetApp Fiber 앱 인스턴스 반환 (테스트용)
func (ws *WebServer) GetApp() *fiber.App {
	return ws.app
}
