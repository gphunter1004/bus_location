// internal/web/handlers/api/status.go
package api

import (
	"time"

	"github.com/gofiber/fiber/v2"

	"bus-tracker/internal/web/models/responses"
	"bus-tracker/internal/web/services"
	"bus-tracker/internal/web/utils"
)

// StatusHandler 상태 관련 API 핸들러
type StatusHandler struct {
	statusService *services.StatusService
}

// NewStatusHandler 상태 핸들러 생성
func NewStatusHandler(statusService *services.StatusService) *StatusHandler {
	return &StatusHandler{
		statusService: statusService,
	}
}

// GetStatus 시스템 상태 조회
// @Summary 시스템 상태 조회
// @Description 현재 시스템의 상태와 설정 정보를 반환합니다
// @Tags status
// @Accept json
// @Produce json
// @Success 200 {object} responses.StatusResponse
// @Failure 500 {object} responses.ErrorResponse
// @Router /api/v1/status [get]
func (h *StatusHandler) GetStatus(c *fiber.Ctx) error {
	statusData, err := h.statusService.GetSystemStatus()
	if err != nil {
		return utils.HandleError(c, err, "시스템 상태 조회 실패")
	}

	response := responses.StatusResponse{
		BaseResponse: responses.NewSuccessResponse("시스템 상태 조회 성공"),
		Data:         statusData,
	}

	return c.JSON(response)
}

// GetStatistics 통계 정보 조회
// @Summary 시스템 통계 조회
// @Description 버스, 캐시, 시스템 메트릭 등의 통계를 반환합니다
// @Tags status
// @Accept json
// @Produce json
// @Success 200 {object} responses.StatisticsResponse
// @Failure 500 {object} responses.ErrorResponse
// @Router /api/v1/status/statistics [get]
func (h *StatusHandler) GetStatistics(c *fiber.Ctx) error {
	statisticsData, err := h.statusService.GetStatistics()
	if err != nil {
		return utils.HandleError(c, err, "통계 정보 조회 실패")
	}

	response := responses.StatisticsResponse{
		BaseResponse: responses.NewSuccessResponse("통계 정보 조회 성공"),
		Data:         statisticsData,
	}

	return c.JSON(response)
}

// GetHealthCheck 헬스체크
// @Summary 헬스체크
// @Description 시스템의 전반적인 건강 상태를 확인합니다
// @Tags status
// @Accept json
// @Produce json
// @Success 200 {object} responses.HealthCheckResponse
// @Failure 503 {object} responses.ErrorResponse
// @Router /api/v1/status/health [get]
func (h *StatusHandler) GetHealthCheck(c *fiber.Ctx) error {
	healthData, isHealthy, err := h.statusService.GetHealthCheck()
	if err != nil {
		return utils.HandleError(c, err, "헬스체크 실패")
	}

	status := fiber.StatusOK
	message := "시스템이 정상적으로 작동 중입니다"

	if !isHealthy {
		status = fiber.StatusServiceUnavailable
		message = "시스템에 문제가 발견되었습니다"
	}

	response := responses.HealthCheckResponse{
		BaseResponse: responses.BaseResponse{
			Success:   isHealthy,
			Message:   message,
			Timestamp: time.Now(),
		},
		Data: healthData,
	}

	return c.Status(status).JSON(response)
}

// GetMetrics 시스템 메트릭 조회
// @Summary 시스템 메트릭 조회
// @Description CPU, 메모리, 디스크 사용량 등의 시스템 메트릭을 반환합니다
// @Tags monitoring
// @Accept json
// @Produce json
// @Success 200 {object} responses.DataResponse
// @Failure 500 {object} responses.ErrorResponse
// @Router /api/v1/monitoring/metrics [get]
func (h *StatusHandler) GetMetrics(c *fiber.Ctx) error {
	metrics, err := h.statusService.GetSystemMetrics()
	if err != nil {
		return utils.HandleError(c, err, "시스템 메트릭 조회 실패")
	}

	response := responses.NewDataResponse(metrics, "시스템 메트릭 조회 성공")
	return c.JSON(response)
}

// GetLogs 시스템 로그 조회
// @Summary 시스템 로그 조회
// @Description 최근 시스템 로그를 조회합니다
// @Tags monitoring
// @Accept json
// @Produce json
// @Param limit query int false "조회할 로그 수" default(100)
// @Param level query string false "로그 레벨" Enums(debug,info,warn,error)
// @Success 200 {object} responses.ListResponse
// @Failure 500 {object} responses.ErrorResponse
// @Router /api/v1/monitoring/logs [get]
func (h *StatusHandler) GetLogs(c *fiber.Ctx) error {
	// 쿼리 파라미터 파싱
	limit := c.QueryInt("limit", 100)
	level := c.Query("level", "")

	logs, err := h.statusService.GetRecentLogs(limit, level)
	if err != nil {
		return utils.HandleError(c, err, "로그 조회 실패")
	}

	response := responses.ListResponse{
		BaseResponse: responses.NewSuccessResponse("로그 조회 성공"),
		Data:         logs,
		Count:        len(logs),
	}

	return c.JSON(response)
}

// GetAlerts 시스템 알림 조회
// @Summary 시스템 알림 조회
// @Description 활성화된 시스템 알림을 조회합니다
// @Tags monitoring
// @Accept json
// @Produce json
// @Success 200 {object} responses.ListResponse
// @Failure 500 {object} responses.ErrorResponse
// @Router /api/v1/monitoring/alerts [get]
func (h *StatusHandler) GetAlerts(c *fiber.Ctx) error {
	alerts, err := h.statusService.GetActiveAlerts()
	if err != nil {
		return utils.HandleError(c, err, "알림 조회 실패")
	}

	response := responses.ListResponse{
		BaseResponse: responses.NewSuccessResponse("알림 조회 성공"),
		Data:         alerts,
		Count:        len(alerts),
	}

	return c.JSON(response)
}

// GetNotifications 알림 목록 조회
// @Summary 알림 목록 조회
// @Description 사용자 알림 목록을 조회합니다
// @Tags notifications
// @Accept json
// @Produce json
// @Param page query int false "페이지 번호" default(1)
// @Param limit query int false "페이지당 항목 수" default(20)
// @Success 200 {object} responses.PaginatedResponse
// @Failure 500 {object} responses.ErrorResponse
// @Router /api/v1/notifications [get]
func (h *StatusHandler) GetNotifications(c *fiber.Ctx) error {
	page := c.QueryInt("page", 1)
	limit := c.QueryInt("limit", 20)

	notifications, pagination, err := h.statusService.GetNotifications(page, limit)
	if err != nil {
		return utils.HandleError(c, err, "알림 목록 조회 실패")
	}

	response := responses.PaginatedResponse{
		BaseResponse: responses.NewSuccessResponse("알림 목록 조회 성공"),
		Data:         notifications,
		Pagination:   pagination,
	}

	return c.JSON(response)
}

// CreateNotification 알림 생성
// @Summary 알림 생성
// @Description 새로운 알림을 생성합니다
// @Tags notifications
// @Accept json
// @Produce json
// @Param notification body requests.CreateNotificationRequest true "알림 정보"
// @Success 201 {object} responses.DataResponse
// @Failure 400 {object} responses.ErrorResponse
// @Failure 500 {object} responses.ErrorResponse
// @Router /api/v1/notifications [post]
func (h *StatusHandler) CreateNotification(c *fiber.Ctx) error {
	var req struct {
		Title    string `json:"title" validate:"required,min=1,max=100"`
		Message  string `json:"message" validate:"required,min=1,max=500"`
		Type     string `json:"type" validate:"required,oneof=info warning error"`
		Priority int    `json:"priority" validate:"min=1,max=5"`
	}

	if err := c.BodyParser(&req); err != nil {
		return utils.HandleValidationError(c, err, "요청 데이터가 올바르지 않습니다")
	}

	if err := utils.ValidateStruct(&req); err != nil {
		return utils.HandleValidationError(c, err, "입력 데이터 검증 실패")
	}

	notification, err := h.statusService.CreateNotification(req.Title, req.Message, req.Type, req.Priority)
	if err != nil {
		return utils.HandleError(c, err, "알림 생성 실패")
	}

	response := responses.NewDataResponse(notification, "알림이 성공적으로 생성되었습니다")
	return c.Status(fiber.StatusCreated).JSON(response)
}

// UpdateNotification 알림 수정
// @Summary 알림 수정
// @Description 기존 알림을 수정합니다
// @Tags notifications
// @Accept json
// @Produce json
// @Param id path string true "알림 ID"
// @Param notification body requests.UpdateNotificationRequest true "수정할 알림 정보"
// @Success 200 {object} responses.DataResponse
// @Failure 400 {object} responses.ErrorResponse
// @Failure 404 {object} responses.ErrorResponse
// @Failure 500 {object} responses.ErrorResponse
// @Router /api/v1/notifications/{id} [put]
func (h *StatusHandler) UpdateNotification(c *fiber.Ctx) error {
	notificationID := c.Params("id")
	if notificationID == "" {
		return utils.HandleValidationError(c, nil, "알림 ID가 필요합니다")
	}

	var req struct {
		Title    string `json:"title,omitempty" validate:"omitempty,min=1,max=100"`
		Message  string `json:"message,omitempty" validate:"omitempty,min=1,max=500"`
		Type     string `json:"type,omitempty" validate:"omitempty,oneof=info warning error"`
		Priority int    `json:"priority,omitempty" validate:"omitempty,min=1,max=5"`
		IsRead   *bool  `json:"isRead,omitempty"`
	}

	if err := c.BodyParser(&req); err != nil {
		return utils.HandleValidationError(c, err, "요청 데이터가 올바르지 않습니다")
	}

	if err := utils.ValidateStruct(&req); err != nil {
		return utils.HandleValidationError(c, err, "입력 데이터 검증 실패")
	}

	notification, err := h.statusService.UpdateNotification(notificationID, req.Title, req.Message, req.Type, req.Priority, req.IsRead)
	if err != nil {
		return utils.HandleError(c, err, "알림 수정 실패")
	}

	response := responses.NewDataResponse(notification, "알림이 성공적으로 수정되었습니다")
	return c.JSON(response)
}

// DeleteNotification 알림 삭제
// @Summary 알림 삭제
// @Description 알림을 삭제합니다
// @Tags notifications
// @Accept json
// @Produce json
// @Param id path string true "알림 ID"
// @Success 200 {object} responses.BaseResponse
// @Failure 404 {object} responses.ErrorResponse
// @Failure 500 {object} responses.ErrorResponse
// @Router /api/v1/notifications/{id} [delete]
func (h *StatusHandler) DeleteNotification(c *fiber.Ctx) error {
	notificationID := c.Params("id")
	if notificationID == "" {
		return utils.HandleValidationError(c, nil, "알림 ID가 필요합니다")
	}

	err := h.statusService.DeleteNotification(notificationID)
	if err != nil {
		return utils.HandleError(c, err, "알림 삭제 실패")
	}

	response := responses.NewSuccessResponse("알림이 성공적으로 삭제되었습니다")
	return c.JSON(response)
}
