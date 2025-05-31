// internal/web/handlers/api/cache.go
package api

import (
	"github.com/gofiber/fiber/v2"

	"bus-tracker/internal/web/services"
	"bus-tracker/internal/web/utils"
)

// CacheHandler 캐시 관련 API 핸들러
type CacheHandler struct {
	cacheService *services.CacheService
}

// NewCacheHandler 캐시 핸들러 생성
func NewCacheHandler(cacheService *services.CacheService) *CacheHandler {
	return &CacheHandler{
		cacheService: cacheService,
	}
}

// GetStationCache 정류소 캐시 조회
// @Summary 정류소 캐시 조회
// @Description 모든 노선의 정류소 캐시 정보를 조회합니다
// @Tags cache
// @Accept json
// @Produce json
// @Success 200 {object} responses.DataResponse
// @Failure 500 {object} responses.ErrorResponse
// @Router /api/v1/cache/stations [get]
func (h *CacheHandler) GetStationCache(c *fiber.Ctx) error {
	cacheData, err := h.cacheService.GetStationCacheData()
	if err != nil {
		return utils.HandleError(c, err, "정류소 캐시 조회 실패")
	}

	return utils.SendSuccessResponse(c, cacheData, "정류소 캐시 조회 성공")
}

// GetRouteStations 특정 노선 정류소 조회
// @Summary 특정 노선의 정류소 조회
// @Description 지정된 노선의 정류소 정보를 조회합니다
// @Tags cache
// @Accept json
// @Produce json
// @Param routeId path string true "노선 ID"
// @Success 200 {object} responses.DataResponse
// @Failure 400 {object} responses.ErrorResponse
// @Failure 404 {object} responses.ErrorResponse
// @Router /api/v1/cache/stations/{routeId} [get]
func (h *CacheHandler) GetRouteStations(c *fiber.Ctx) error {
	routeId := c.Params("routeId")
	if routeId == "" {
		return utils.HandleValidationError(c, nil, "routeId 파라미터가 필요합니다")
	}

	stationData, err := h.cacheService.GetRouteStationData(routeId)
	if err != nil {
		return utils.HandleError(c, err, "노선 정류소 조회 실패")
	}

	return utils.SendSuccessResponse(c, stationData, "노선 정류소 조회 성공")
}

// ReloadStationCache 정류소 캐시 새로고침
// @Summary 정류소 캐시 새로고침
// @Description 모든 노선의 정류소 캐시를 다시 로드합니다
// @Tags cache
// @Accept json
// @Produce json
// @Success 200 {object} responses.OperationResponse
// @Failure 500 {object} responses.ErrorResponse
// @Router /api/v1/cache/stations/reload [post]
func (h *CacheHandler) ReloadStationCache(c *fiber.Ctx) error {
	result, err := h.cacheService.ReloadStationCache()
	if err != nil {
		return utils.HandleError(c, err, "정류소 캐시 새로고침 실패")
	}

	return utils.SendOperationResponse(c, "cache_reload", result, "정류소 캐시가 성공적으로 새로고침되었습니다")
}

// ClearStationCache 정류소 캐시 삭제
// @Summary 정류소 캐시 삭제
// @Description 모든 정류소 캐시를 삭제합니다
// @Tags cache
// @Accept json
// @Produce json
// @Success 200 {object} responses.OperationResponse
// @Failure 500 {object} responses.ErrorResponse
// @Router /api/v1/cache/stations [delete]
func (h *CacheHandler) ClearStationCache(c *fiber.Ctx) error {
	result, err := h.cacheService.ClearStationCache()
	if err != nil {
		return utils.HandleError(c, err, "정류소 캐시 삭제 실패")
	}

	return utils.SendOperationResponse(c, "cache_clear", result, "정류소 캐시가 성공적으로 삭제되었습니다")
}

// GetCacheStatistics 캐시 통계 조회
// @Summary 캐시 통계 조회
// @Description 캐시의 통계 정보를 조회합니다
// @Tags cache
// @Accept json
// @Produce json
// @Success 200 {object} responses.DataResponse
// @Failure 500 {object} responses.ErrorResponse
// @Router /api/v1/cache/stats [get]
func (h *CacheHandler) GetCacheStatistics(c *fiber.Ctx) error {
	stats, err := h.cacheService.GetCacheStatistics()
	if err != nil {
		return utils.HandleError(c, err, "캐시 통계 조회 실패")
	}

	return utils.SendSuccessResponse(c, stats, "캐시 통계 조회 성공")
}
