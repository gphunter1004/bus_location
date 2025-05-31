// internal/web/handlers/api/control.go
package api

import (
	"time"

	"github.com/gofiber/fiber/v2"

	"bus-tracker/internal/services"
	"bus-tracker/internal/utils"
	"bus-tracker/internal/web/models/responses"
	webUtils "bus-tracker/internal/web/utils"
)

// ControlHandler 시스템 제어 API 핸들러
type ControlHandler struct {
	orchestrator *services.MultiAPIOrchestrator
	logger       *utils.Logger
}

// NewControlHandler 제어 핸들러 생성
func NewControlHandler(orchestrator *services.MultiAPIOrchestrator, logger *utils.Logger) *ControlHandler {
	return &ControlHandler{
		orchestrator: orchestrator,
		logger:       logger,
	}
}

// StartSystem 시스템 시작
// @Summary 시스템 시작
// @Description 버스 트래킹 시스템을 시작합니다
// @Tags control
// @Accept json
// @Produce json
// @Success 200 {object} responses.OperationResponse
// @Failure 409 {object} responses.ErrorResponse "이미 실행 중"
// @Failure 500 {object} responses.ErrorResponse
// @Router /api/v1/control/start [post]
func (h *ControlHandler) StartSystem(c *fiber.Ctx) error {
	if h.orchestrator.IsRunning() {
		return c.Status(fiber.StatusConflict).JSON(responses.ErrorResponse{
			Error:   true,
			Message: "시스템이 이미 실행 중입니다",
			Code:    fiber.StatusConflict,
		})
	}

	err := h.orchestrator.Start()
	if err != nil {
		h.logger.Errorf("시스템 시작 실패: %v", err)
		return webUtils.HandleError(c, err, "시스템 시작 실패")
	}

	h.logger.Info("시스템이 성공적으로 시작되었습니다")

	result := map[string]interface{}{
		"status":    "started",
		"startTime": time.Now(),
		"message":   "버스 트래킹 시스템이 성공적으로 시작되었습니다",
	}

	return webUtils.SendOperationResponse(c, "system_start", result, "시스템이 성공적으로 시작되었습니다")
}

// StopSystem 시스템 정지
// @Summary 시스템 정지
// @Description 버스 트래킹 시스템을 정지합니다
// @Tags control
// @Accept json
// @Produce json
// @Success 200 {object} responses.OperationResponse
// @Failure 409 {object} responses.ErrorResponse "이미 정지됨"
// @Router /api/v1/control/stop [post]
func (h *ControlHandler) StopSystem(c *fiber.Ctx) error {
	if !h.orchestrator.IsRunning() {
		return c.Status(fiber.StatusConflict).JSON(responses.ErrorResponse{
			Error:   true,
			Message: "시스템이 이미 정지 상태입니다",
			Code:    fiber.StatusConflict,
		})
	}

	h.orchestrator.Stop()
	h.logger.Info("시스템이 성공적으로 정지되었습니다")

	result := map[string]interface{}{
		"status":   "stopped",
		"stopTime": time.Now(),
		"message":  "버스 트래킹 시스템이 성공적으로 정지되었습니다",
	}

	return webUtils.SendOperationResponse(c, "system_stop", result, "시스템이 성공적으로 정지되었습니다")
}

// RestartSystem 시스템 재시작
// @Summary 시스템 재시작
// @Description 버스 트래킹 시스템을 재시작합니다
// @Tags control
// @Accept json
// @Produce json
// @Success 200 {object} responses.OperationResponse
// @Failure 500 {object} responses.ErrorResponse
// @Router /api/v1/control/restart [post]
func (h *ControlHandler) RestartSystem(c *fiber.Ctx) error {
	restartStartTime := time.Now()
	h.logger.Info("시스템 재시작을 시작합니다")

	// 실행 중이면 먼저 정지
	if h.orchestrator.IsRunning() {
		h.logger.Info("시스템 정지 중...")
		h.orchestrator.Stop()

		// 정지 완료까지 잠시 대기
		time.Sleep(2 * time.Second)
	}

	// 시스템 시작
	h.logger.Info("시스템 시작 중...")
	err := h.orchestrator.Start()
	if err != nil {
		h.logger.Errorf("시스템 재시작 실패: %v", err)
		return webUtils.HandleError(c, err, "시스템 재시작 실패")
	}

	restartDuration := time.Since(restartStartTime)
	h.logger.Infof("시스템 재시작 완료 (소요시간: %v)", restartDuration)

	result := map[string]interface{}{
		"status":        "restarted",
		"restartTime":   restartStartTime,
		"completedTime": time.Now(),
		"duration":      restartDuration.String(),
		"message":       "버스 트래킹 시스템이 성공적으로 재시작되었습니다",
	}

	return webUtils.SendOperationResponse(c, "system_restart", result, "시스템이 성공적으로 재시작되었습니다")
}

// GetSystemStatus 시스템 상태 조회
// @Summary 시스템 제어 상태 조회
// @Description 시스템의 현재 제어 상태를 조회합니다
// @Tags control
// @Accept json
// @Produce json
// @Success 200 {object} responses.DataResponse
// @Router /api/v1/control/status [get]
func (h *ControlHandler) GetSystemStatus(c *fiber.Ctx) error {
	isRunning := h.orchestrator.IsRunning()

	status := map[string]interface{}{
		"isRunning": isRunning,
		"status": func() string {
			if isRunning {
				return "running"
			}
			return "stopped"
		}(),
		"checkTime":    time.Now(),
		"uptime":       h.calculateUptime(),
		"capabilities": h.getSystemCapabilities(),
		"lastAction":   h.getLastAction(),
	}

	message := "시스템이 정상적으로 실행 중입니다"
	if !isRunning {
		message = "시스템이 정지 상태입니다"
	}

	return webUtils.SendSuccessResponse(c, status, message)
}

// SetMaintenanceMode 유지보수 모드 설정
// @Summary 유지보수 모드 설정
// @Description 시스템을 유지보수 모드로 전환합니다
// @Tags control
// @Accept json
// @Produce json
// @Param body body object true "유지보수 모드 설정"
// @Success 200 {object} responses.OperationResponse
// @Failure 400 {object} responses.ErrorResponse
// @Router /api/v1/control/maintenance [post]
func (h *ControlHandler) SetMaintenanceMode(c *fiber.Ctx) error {
	var req struct {
		Enable   bool   `json:"enable" validate:"required"`
		Reason   string `json:"reason,omitempty"`
		Duration string `json:"duration,omitempty"` // 예: "30m", "2h"
	}

	if err := c.BodyParser(&req); err != nil {
		return webUtils.HandleValidationError(c, err, "요청 데이터가 올바르지 않습니다")
	}

	if err := webUtils.ValidateStruct(&req); err != nil {
		return webUtils.HandleValidationError(c, err, "입력 데이터 검증 실패")
	}

	var duration time.Duration
	if req.Duration != "" {
		var err error
		duration, err = time.ParseDuration(req.Duration)
		if err != nil {
			return webUtils.HandleValidationError(c, err, "유지보수 시간 형식이 올바르지 않습니다")
		}
	}

	// 유지보수 모드 설정 (모의 구현)
	result := map[string]interface{}{
		"maintenanceMode": req.Enable,
		"reason":          req.Reason,
		"duration":        req.Duration,
		"startTime":       time.Now(),
		"endTime": func() *time.Time {
			if duration > 0 {
				endTime := time.Now().Add(duration)
				return &endTime
			}
			return nil
		}(),
	}

	message := "유지보수 모드가 비활성화되었습니다"
	if req.Enable {
		message = "유지보수 모드가 활성화되었습니다"
		h.logger.Infof("유지보수 모드 활성화 - 사유: %s, 기간: %s", req.Reason, req.Duration)
	} else {
		h.logger.Info("유지보수 모드 비활성화")
	}

	return webUtils.SendOperationResponse(c, "maintenance_mode", result, message)
}

// GetSystemInfo 시스템 정보 조회
// @Summary 시스템 정보 조회
// @Description 시스템의 상세 정보를 조회합니다
// @Tags control
// @Accept json
// @Produce json
// @Success 200 {object} responses.DataResponse
// @Router /api/v1/control/info [get]
func (h *ControlHandler) GetSystemInfo(c *fiber.Ctx) error {
	// 시스템 통계 수집
	totalBuses, api1Only, api2Only, both := h.getSystemStatistics()

	systemInfo := map[string]interface{}{
		"version":     "1.0.0",
		"buildTime":   "2024-12-19T10:30:00Z", // 빌드 시간 (모의)
		"environment": "production",
		"features": map[string]bool{
			"api1Enabled":          true,
			"api2Enabled":          true,
			"unifiedMode":          true,
			"duplicateCheck":       true,
			"webInterface":         true,
			"elasticsearchEnabled": true,
		},
		"statistics": map[string]interface{}{
			"totalBuses": totalBuses,
			"api1Only":   api1Only,
			"api2Only":   api2Only,
			"both":       both,
		},
		"runtime": map[string]interface{}{
			"isRunning":     h.orchestrator.IsRunning(),
			"uptime":        h.calculateUptime(),
			"lastStartTime": h.getLastStartTime(),
			"restartCount":  h.getRestartCount(),
		},
		"configuration": h.getSystemConfiguration(),
	}

	return webUtils.SendSuccessResponse(c, systemInfo, "시스템 정보 조회 성공")
}

// 내부 헬퍼 메서드들

// calculateUptime 업타임 계산
func (h *ControlHandler) calculateUptime() string {
	// 실제 구현에서는 시작 시간을 추적해야 함
	return "2시간 30분" // 모의 데이터
}

// getSystemCapabilities 시스템 기능 조회
func (h *ControlHandler) getSystemCapabilities() map[string]interface{} {
	return map[string]interface{}{
		"canStart":       !h.orchestrator.IsRunning(),
		"canStop":        h.orchestrator.IsRunning(),
		"canRestart":     true,
		"canMaintenance": true,
		"supportedAPIs":  []string{"API1", "API2"},
		"features":       []string{"unified_mode", "duplicate_check", "web_interface"},
	}
}

// getLastAction 마지막 작업 조회
func (h *ControlHandler) getLastAction() map[string]interface{} {
	// 실제 구현에서는 작업 히스토리를 추적해야 함
	return map[string]interface{}{
		"action":    "start",
		"timestamp": time.Now().Add(-30 * time.Minute),
		"user":      "system",
		"success":   true,
	}
}

// getSystemStatistics 시스템 통계 조회
func (h *ControlHandler) getSystemStatistics() (int, int, int, int) {
	// orchestrator에서 통계를 가져오는 로직이 필요하다면 추가
	// 현재는 모의 데이터 반환
	return 150, 50, 60, 40 // totalBuses, api1Only, api2Only, both
}

// getLastStartTime 마지막 시작 시간 조회
func (h *ControlHandler) getLastStartTime() time.Time {
	// 실제 구현에서는 시작 시간을 추적해야 함
	return time.Now().Add(-2*time.Hour - 30*time.Minute) // 모의 데이터
}

// getRestartCount 재시작 횟수 조회
func (h *ControlHandler) getRestartCount() int {
	// 실제 구현에서는 재시작 횟수를 추적해야 함
	return 3 // 모의 데이터
}

// getSystemConfiguration 시스템 설정 조회
func (h *ControlHandler) getSystemConfiguration() map[string]interface{} {
	return map[string]interface{}{
		"operatingMode":   "unified",
		"dataRetention":   "24h",
		"cleanupInterval": "5m",
		"apiCallInterval": map[string]string{
			"api1": "30s",
			"api2": "45s",
		},
		"maxConcurrency": 10,
		"timeouts": map[string]string{
			"api":     "30s",
			"cleanup": "60s",
		},
	}
}
