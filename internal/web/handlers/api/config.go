// internal/web/handlers/api/config.go
package api

import (
	"github.com/gofiber/fiber/v2"
	
	"bus-tracker/config"
	"bus-tracker/internal/utils"
	"bus-tracker/internal/web/models/responses"
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