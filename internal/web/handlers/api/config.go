// internal/web/handlers/api/config.go
package api

import (
	"fmt"

	"github.com/gofiber/fiber/v2"

	"bus-tracker/config"
	"bus-tracker/internal/utils"
	webUtils "bus-tracker/internal/web/utils"
)

// ConfigHandler 설정 관련 API 핸들러
type ConfigHandler struct {
	config *config.Config
	logger *utils.Logger
}

// NewConfigHandler 설정 핸들러 생성
func NewConfigHandler(cfg *config.Config, logger *utils.Logger) *ConfigHandler {
	return &ConfigHandler{
		config: cfg,
		logger: logger,
	}
}

// GetConfig 설정 조회
// @Summary 시스템 설정 조회
// @Description 현재 시스템 설정 정보를 조회합니다
// @Tags config
// @Accept json
// @Produce json
// @Success 200 {object} responses.DataResponse
// @Router /api/v1/config [get]
func (h *ConfigHandler) GetConfig(c *fiber.Ctx) error {
	configData := map[string]interface{}{
		"api1Config": map[string]interface{}{
			"interval": h.config.API1Config.Interval.String(),
			"baseURL":  h.config.API1Config.BaseURL,
			"routeIDs": h.config.API1Config.RouteIDs,
			"priority": h.config.API1Config.Priority,
		},
		"api2Config": map[string]interface{}{
			"interval": h.config.API2Config.Interval.String(),
			"baseURL":  h.config.API2Config.BaseURL,
			"routeIDs": h.config.API2Config.RouteIDs,
			"priority": h.config.API2Config.Priority,
		},
		"trackingConfig": map[string]interface{}{
			"busDisappearanceTimeout": h.config.BusDisappearanceTimeout.String(),
			"enableTerminalStop":      h.config.EnableTerminalStop,
			"busCleanupInterval":      h.config.BusCleanupInterval.String(),
		},
		"operatingTime": map[string]interface{}{
			"startHour":   h.config.OperatingStartHour,
			"startMinute": h.config.OperatingStartMinute,
			"endHour":     h.config.OperatingEndHour,
			"endMinute":   h.config.OperatingEndMinute,
		},
		"dataProcessing": map[string]interface{}{
			"dataMergeInterval":   h.config.DataMergeInterval.String(),
			"dataRetentionPeriod": h.config.DataRetentionPeriod.String(),
		},
		"elasticsearch": map[string]interface{}{
			"url":       h.config.ElasticsearchURL,
			"indexName": h.config.IndexName,
			"hasAuth":   h.config.ElasticsearchUsername != "",
		},
		"systemInfo": map[string]interface{}{
			"cityCode": h.config.CityCode,
			"mode":     "unified",
		},
	}

	return webUtils.SendSuccessResponse(c, configData, "설정 조회 성공")
}

// UpdateConfig 설정 수정
// @Summary 시스템 설정 수정
// @Description 시스템 설정을 수정합니다 (일부 설정만 지원)
// @Tags config
// @Accept json
// @Produce json
// @Param config body UpdateConfigRequest true "수정할 설정"
// @Success 200 {object} responses.OperationResponse
// @Failure 400 {object} responses.ErrorResponse
// @Router /api/v1/config [put]
func (h *ConfigHandler) UpdateConfig(c *fiber.Ctx) error {
	var req UpdateConfigRequest

	if err := c.BodyParser(&req); err != nil {
		return webUtils.HandleValidationError(c, err, "요청 데이터가 올바르지 않습니다")
	}

	if err := webUtils.ValidateStruct(&req); err != nil {
		return webUtils.HandleValidationError(c, err, "입력 데이터 검증 실패")
	}

	// 현재는 읽기 전용 설정만 지원 (런타임 변경이 안전한 것들만)
	updatedFields := make(map[string]interface{})

	// 향후 런타임 변경 가능한 설정들을 여기에 추가
	// 예: 로그 레벨, 일부 타임아웃 설정 등

	h.logger.Infof("설정 수정 요청 처리 완료")

	result := map[string]interface{}{
		"updated":       updatedFields,
		"message":       "현재 대부분의 설정은 재시작 후 적용됩니다",
		"restartNeeded": true,
		"supportedUpdates": []string{
			"향후 런타임 변경 가능한 설정들이 추가될 예정입니다",
		},
	}

	return webUtils.SendOperationResponse(c, "config_update", result, "설정 수정 요청이 처리되었습니다")
}

// UpdateConfigRequest 설정 수정 요청 구조체
type UpdateConfigRequest struct {
	// 향후 런타임 변경 가능한 설정들을 여기에 추가
	LogLevel string `json:"logLevel,omitempty" validate:"omitempty,oneof=DEBUG INFO WARN ERROR"`
	// 다른 설정들도 필요에 따라 추가
}

// GetOperatingStatus 운영 상태 조회
// @Summary 운영 상태 조회
// @Description 현재 운영 상태와 시간 정보를 조회합니다
// @Tags config
// @Accept json
// @Produce json
// @Success 200 {object} responses.DataResponse
// @Router /api/v1/config/operating-status [get]
func (h *ConfigHandler) GetOperatingStatus(c *fiber.Ctx) error {
	now := c.Context().Time()
	isOperating := h.config.IsOperatingTime(now)

	var nextOperatingTime *string
	if !isOperating {
		next := h.config.GetNextOperatingTime(now)
		nextStr := next.Format("2006-01-02 15:04:05")
		nextOperatingTime = &nextStr
	}

	statusData := map[string]interface{}{
		"currentTime":       now.Format("2006-01-02 15:04:05"),
		"isOperatingTime":   isOperating,
		"nextOperatingTime": nextOperatingTime,
		"operatingSchedule": map[string]interface{}{
			"startTime": h.config.OperatingStartHour*60 + h.config.OperatingStartMinute,
			"endTime":   h.config.OperatingEndHour*60 + h.config.OperatingEndMinute,
			"display":   getOperatingScheduleDisplay(h.config),
		},
		"timezone": "Asia/Seoul",
	}

	return webUtils.SendSuccessResponse(c, statusData, "운영 상태 조회 성공")
}

// getOperatingScheduleDisplay 운영시간 표시 문자열 생성
func getOperatingScheduleDisplay(cfg *config.Config) string {
	startHour := cfg.OperatingStartHour
	startMinute := cfg.OperatingStartMinute
	endHour := cfg.OperatingEndHour
	endMinute := cfg.OperatingEndMinute

	return fmt.Sprintf("%02d:%02d ~ %02d:%02d", startHour, startMinute, endHour, endMinute)
}
