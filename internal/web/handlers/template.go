// internal/web/handlers/template_handler.go
package handlers

import (
	"fmt"
	"html/template"
	"path/filepath"
	"time"

	"github.com/gofiber/fiber/v2"

	"bus-tracker/config"
	"bus-tracker/internal/services"
	"bus-tracker/internal/services/tracker"
	"bus-tracker/internal/utils"
)

// TemplateHandler 템플릿 기반 웹 페이지 핸들러
type TemplateHandler struct {
	config       *config.Config
	logger       *utils.Logger
	orchestrator *services.MultiAPIOrchestrator
	busTracker   *tracker.BusTrackerWithDuplicateCheck
	dataManager  services.UnifiedDataManagerInterface
	templates    *template.Template
}

// DashboardData 대시보드 템플릿 데이터
type DashboardData struct {
	TotalBuses         int
	API1Only           int
	API2Only           int
	Both               int
	TrackedBuses       int
	CurrentDate        string
	CurrentTime        string
	OrchestratorStatus string
	OperatingSchedule  string
	UpdateTime         string
	IsOperatingTime    bool
	NextOperatingTime  *time.Time
}

// NewTemplateHandler 템플릿 핸들러 생성
func NewTemplateHandler(
	cfg *config.Config,
	logger *utils.Logger,
	orchestrator *services.MultiAPIOrchestrator,
	busTracker *tracker.BusTrackerWithDuplicateCheck,
	dataManager services.UnifiedDataManagerInterface,
	templateDir string,
) (*TemplateHandler, error) {

	// 템플릿 로드
	templates, err := loadTemplates(templateDir)
	if err != nil {
		return nil, fmt.Errorf("템플릿 로드 실패: %v", err)
	}

	return &TemplateHandler{
		config:       cfg,
		logger:       logger,
		orchestrator: orchestrator,
		busTracker:   busTracker,
		dataManager:  dataManager,
		templates:    templates,
	}, nil
}

// loadTemplates 템플릿 파일들을 로드
func loadTemplates(templateDir string) (*template.Template, error) {
	// 템플릿 파일 패턴
	pattern := filepath.Join(templateDir, "*.html")

	// 템플릿 파싱
	templates, err := template.ParseGlob(pattern)
	if err != nil {
		return nil, err
	}

	return templates, nil
}

// HandleDashboard 대시보드 페이지 핸들러
func (h *TemplateHandler) HandleDashboard(c *fiber.Ctx) error {
	now := time.Now()

	// 통계 데이터 수집
	totalBuses, api1Only, api2Only, both := h.dataManager.GetStatistics()
	trackedBuses := h.busTracker.GetTrackedBusCount()
	currentDate := h.busTracker.GetCurrentOperatingDate()

	// 오케스트레이터 상태
	orchestratorStatus := "정지됨"
	if h.orchestrator.IsRunning() {
		orchestratorStatus = "실행 중"
	}

	// 운영 스케줄
	operatingSchedule := fmt.Sprintf("%02d:%02d ~ %02d:%02d",
		h.config.OperatingStartHour, h.config.OperatingStartMinute,
		h.config.OperatingEndHour, h.config.OperatingEndMinute)

	// 다음 운영 시간 (운영시간이 아닌 경우만)
	var nextOperatingTime *time.Time
	isOperatingTime := h.config.IsOperatingTime(now)
	if !isOperatingTime {
		next := h.config.GetNextOperatingTime(now)
		nextOperatingTime = &next
	}

	// 템플릿 데이터 준비
	data := DashboardData{
		TotalBuses:         totalBuses,
		API1Only:           api1Only,
		API2Only:           api2Only,
		Both:               both,
		TrackedBuses:       trackedBuses,
		CurrentDate:        currentDate,
		CurrentTime:        now.Format("2006-01-02 15:04:05"),
		OrchestratorStatus: orchestratorStatus,
		OperatingSchedule:  operatingSchedule,
		UpdateTime:         fmt.Sprintf("(%s)", now.Format("15:04:05")),
		IsOperatingTime:    isOperatingTime,
		NextOperatingTime:  nextOperatingTime,
	}

	// HTML 응답 설정
	c.Set("Content-Type", "text/html; charset=utf-8")

	// 템플릿 렌더링
	return c.Render("dashboard", data)
}

// HandleMonitoring 모니터링 페이지 핸들러
func (h *TemplateHandler) HandleMonitoring(c *fiber.Ctx) error {
	// 모니터링 페이지 데이터 수집
	data := struct {
		Title       string
		CurrentTime string
		SystemInfo  map[string]interface{}
	}{
		Title:       "시스템 모니터링",
		CurrentTime: time.Now().Format("2006-01-02 15:04:05"),
		SystemInfo: map[string]interface{}{
			"uptime": utils.Time.CalculateUptime(h.busTracker.GetLastResetTime()),
			"status": "running",
		},
	}

	c.Set("Content-Type", "text/html; charset=utf-8")
	return c.Render("monitoring", data)
}

// HandleAPI 대시보드 페이지 내 API 문서 섹션
func (h *TemplateHandler) HandleAPIDoc(c *fiber.Ctx) error {
	data := struct {
		Title       string
		CurrentTime string
		APIList     []APIEndpoint
	}{
		Title:       "API 문서",
		CurrentTime: time.Now().Format("2006-01-02 15:04:05"),
		APIList:     getAPIEndpoints(),
	}

	c.Set("Content-Type", "text/html; charset=utf-8")
	return c.Render("api-doc", data)
}

// APIEndpoint API 엔드포인트 정보
type APIEndpoint struct {
	Method      string
	Path        string
	Description string
	Example     string
}

// getAPIEndpoints API 엔드포인트 목록 반환
func getAPIEndpoints() []APIEndpoint {
	return []APIEndpoint{
		{
			Method:      "GET",
			Path:        "/api/v1/status",
			Description: "시스템 상태 조회",
			Example:     "/api/v1/status",
		},
		{
			Method:      "GET",
			Path:        "/api/v1/status/statistics",
			Description: "통계 정보 조회",
			Example:     "/api/v1/status/statistics",
		},
		{
			Method:      "GET",
			Path:        "/api/v1/status/health",
			Description: "헬스체크",
			Example:     "/api/v1/status/health",
		},
		{
			Method:      "GET",
			Path:        "/api/v1/config",
			Description: "설정 정보 조회",
			Example:     "/api/v1/config",
		},
		{
			Method:      "GET",
			Path:        "/api/v1/monitoring/metrics",
			Description: "시스템 메트릭 조회",
			Example:     "/api/v1/monitoring/metrics",
		},
		{
			Method:      "GET",
			Path:        "/api/v1/monitoring/logs",
			Description: "시스템 로그 조회",
			Example:     "/api/v1/monitoring/logs?limit=100",
		},
	}
}

// ReloadTemplates 템플릿 재로드 (개발 중 유용)
func (h *TemplateHandler) ReloadTemplates(templateDir string) error {
	templates, err := loadTemplates(templateDir)
	if err != nil {
		return err
	}

	h.templates = templates
	h.logger.Info("템플릿이 성공적으로 재로드되었습니다")
	return nil
}
