// internal/web/handlers/api/tracking.go
package api

import (
	"fmt"

	"github.com/gofiber/fiber/v2"

	"bus-tracker/internal/web/models/responses"
	"bus-tracker/internal/web/services"
	"bus-tracker/internal/web/utils"
)

// TrackingHandler 트래킹 관련 API 핸들러
type TrackingHandler struct {
	trackingService *services.TrackingService
}

// NewTrackingHandler 트래킹 핸들러 생성
func NewTrackingHandler(trackingService *services.TrackingService) *TrackingHandler {
	return &TrackingHandler{
		trackingService: trackingService,
	}
}

// GetTrackedBuses 추적 중인 버스 목록 조회
// @Summary 추적 중인 버스 목록 조회
// @Description 현재 추적 중인 모든 버스의 목록을 조회합니다
// @Tags tracking
// @Accept json
// @Produce json
// @Param includeDetails query boolean false "상세 정보 포함 여부"
// @Success 200 {object} responses.TrackedBusesResponse
// @Failure 500 {object} responses.ErrorResponse
// @Router /api/v1/tracking/buses [get]
func (h *TrackingHandler) GetTrackedBuses(c *fiber.Ctx) error {
	includeDetails := c.QueryBool("includeDetails", false)

	busesData, err := h.trackingService.GetTrackedBuses(includeDetails)
	if err != nil {
		return utils.HandleError(c, err, "추적 버스 목록 조회 실패")
	}

	response := responses.TrackedBusesResponse{
		BaseResponse: responses.NewSuccessResponse("추적 버스 목록 조회 성공"),
		Data:         busesData,
	}

	return c.JSON(response)
}

// GetBusInfo 특정 버스 정보 조회
// @Summary 특정 버스 정보 조회
// @Description 지정된 버스의 상세 정보를 조회합니다
// @Tags tracking
// @Accept json
// @Produce json
// @Param plateNo path string true "차량 번호"
// @Success 200 {object} responses.BusInfoResponse
// @Failure 400 {object} responses.ErrorResponse
// @Failure 404 {object} responses.ErrorResponse
// @Router /api/v1/tracking/buses/{plateNo} [get]
func (h *TrackingHandler) GetBusInfo(c *fiber.Ctx) error {
	plateNo := c.Params("plateNo")
	if plateNo == "" {
		return utils.HandleValidationError(c, nil, "plateNo 파라미터가 필요합니다")
	}

	busData, err := h.trackingService.GetBusInfo(plateNo)
	if err != nil {
		return utils.HandleError(c, err, "버스 정보 조회 실패")
	}

	response := responses.BusInfoResponse{
		BaseResponse: responses.NewSuccessResponse("버스 정보 조회 성공"),
		Data:         busData,
	}

	return c.JSON(response)
}

// RemoveBusTracking 버스 추적 제거
// @Summary 버스 추적 제거
// @Description 지정된 버스의 추적을 중단합니다
// @Tags tracking
// @Accept json
// @Produce json
// @Param plateNo path string true "차량 번호"
// @Success 200 {object} responses.BaseResponse
// @Failure 400 {object} responses.ErrorResponse
// @Failure 404 {object} responses.ErrorResponse
// @Router /api/v1/tracking/buses/{plateNo} [delete]
func (h *TrackingHandler) RemoveBusTracking(c *fiber.Ctx) error {
	plateNo := c.Params("plateNo")
	if plateNo == "" {
		return utils.HandleValidationError(c, nil, "plateNo 파라미터가 필요합니다")
	}

	err := h.trackingService.RemoveBusTracking(plateNo)
	if err != nil {
		return utils.HandleError(c, err, "버스 추적 제거 실패")
	}

	response := responses.NewSuccessResponse(fmt.Sprintf("버스 추적이 제거되었습니다: %s", plateNo))
	return c.JSON(response)
}

// GetBusHistory 버스 이력 조회
// @Summary 버스 이력 조회
// @Description 지정된 버스의 운행 이력을 조회합니다
// @Tags tracking
// @Accept json
// @Produce json
// @Param plateNo path string true "차량 번호"
// @Param limit query int false "조회 개수" default(50)
// @Param hours query int false "조회 시간 범위 (시간)" default(24)
// @Success 200 {object} responses.ListResponse
// @Failure 400 {object} responses.ErrorResponse
// @Router /api/v1/tracking/buses/{plateNo}/history [get]
func (h *TrackingHandler) GetBusHistory(c *fiber.Ctx) error {
	plateNo := c.Params("plateNo")
	if plateNo == "" {
		return utils.HandleValidationError(c, nil, "plateNo 파라미터가 필요합니다")
	}

	limit := c.QueryInt("limit", 50)
	hours := c.QueryInt("hours", 24)

	history, err := h.trackingService.GetBusHistory(plateNo, limit, hours)
	if err != nil {
		return utils.HandleError(c, err, "버스 이력 조회 실패")
	}

	return utils.SendListResponse(c, history, len(history), "버스 이력 조회 성공")
}

// GetTripStatistics 운행 차수 통계 조회
// @Summary 운행 차수 통계 조회
// @Description 일일 운행 차수 통계를 조회합니다
// @Tags tracking
// @Accept json
// @Produce json
// @Success 200 {object} responses.TripStatisticsResponse
// @Failure 500 {object} responses.ErrorResponse
// @Router /api/v1/tracking/trips/statistics [get]
func (h *TrackingHandler) GetTripStatistics(c *fiber.Ctx) error {
	statisticsData, err := h.trackingService.GetTripStatistics()
	if err != nil {
		return utils.HandleError(c, err, "운행 차수 통계 조회 실패")
	}

	response := responses.TripStatisticsResponse{
		BaseResponse: responses.NewSuccessResponse("운행 차수 통계 조회 성공"),
		Data:         statisticsData,
	}

	return c.JSON(response)
}

// ResetTripCounters 운행 차수 카운터 리셋
// @Summary 운행 차수 카운터 리셋
// @Description 모든 차량의 일일 운행 차수 카운터를 리셋합니다
// @Tags tracking
// @Accept json
// @Produce json
// @Success 200 {object} responses.TripCounterResetResponse
// @Failure 500 {object} responses.ErrorResponse
// @Router /api/v1/tracking/trips/reset-counters [post]
func (h *TrackingHandler) ResetTripCounters(c *fiber.Ctx) error {
	resetData, err := h.trackingService.ResetTripCounters()
	if err != nil {
		return utils.HandleError(c, err, "운행 차수 카운터 리셋 실패")
	}

	response := responses.TripCounterResetResponse{
		BaseResponse: responses.NewSuccessResponse("운행 차수 카운터가 성공적으로 리셋되었습니다"),
		Data:         resetData,
	}

	return c.JSON(response)
}

// GetDailyTripStatistics 특정 일자 운행 통계 조회
// @Summary 특정 일자 운행 통계 조회
// @Description 지정된 날짜의 운행 통계를 조회합니다
// @Tags tracking
// @Accept json
// @Produce json
// @Param date path string true "조회 일자 (YYYY-MM-DD)"
// @Success 200 {object} responses.DataResponse
// @Failure 400 {object} responses.ErrorResponse
// @Router /api/v1/tracking/trips/daily/{date} [get]
func (h *TrackingHandler) GetDailyTripStatistics(c *fiber.Ctx) error {
	date := c.Params("date")
	if date == "" {
		return utils.HandleValidationError(c, nil, "date 파라미터가 필요합니다")
	}

	dailyStats, err := h.trackingService.GetDailyTripStatistics(date)
	if err != nil {
		return utils.HandleError(c, err, "일일 운행 통계 조회 실패")
	}

	return utils.SendSuccessResponse(c, dailyStats, fmt.Sprintf("%s 일일 운행 통계 조회 성공", date))
}

// GetActiveRoutes 활성 노선 목록 조회
// @Summary 활성 노선 목록 조회
// @Description 현재 운행 중인 노선 목록을 조회합니다
// @Tags tracking
// @Accept json
// @Produce json
// @Success 200 {object} responses.ListResponse
// @Failure 500 {object} responses.ErrorResponse
// @Router /api/v1/tracking/routes [get]
func (h *TrackingHandler) GetActiveRoutes(c *fiber.Ctx) error {
	routes, err := h.trackingService.GetActiveRoutes()
	if err != nil {
		return utils.HandleError(c, err, "활성 노선 목록 조회 실패")
	}

	return utils.SendListResponse(c, routes, len(routes), "활성 노선 목록 조회 성공")
}

// GetRouteBuses 특정 노선의 버스 목록 조회
// @Summary 특정 노선의 버스 목록 조회
// @Description 지정된 노선에서 운행 중인 버스 목록을 조회합니다
// @Tags tracking
// @Accept json
// @Produce json
// @Param routeId path string true "노선 ID"
// @Success 200 {object} responses.ListResponse
// @Failure 400 {object} responses.ErrorResponse
// @Router /api/v1/tracking/routes/{routeId}/buses [get]
func (h *TrackingHandler) GetRouteBuses(c *fiber.Ctx) error {
	routeId := c.Params("routeId")
	if routeId == "" {
		return utils.HandleValidationError(c, nil, "routeId 파라미터가 필요합니다")
	}

	buses, err := h.trackingService.GetRouteBuses(routeId)
	if err != nil {
		return utils.HandleError(c, err, "노선 버스 목록 조회 실패")
	}

	return utils.SendListResponse(c, buses, len(buses), fmt.Sprintf("노선 %s 버스 목록 조회 성공", routeId))
}

// GetRouteStatistics 특정 노선 통계 조회
// @Summary 특정 노선 통계 조회
// @Description 지정된 노선의 운행 통계를 조회합니다
// @Tags tracking
// @Accept json
// @Produce json
// @Param routeId path string true "노선 ID"
// @Success 200 {object} responses.DataResponse
// @Failure 400 {object} responses.ErrorResponse
// @Router /api/v1/tracking/routes/{routeId}/statistics [get]
func (h *TrackingHandler) GetRouteStatistics(c *fiber.Ctx) error {
	routeId := c.Params("routeId")
	if routeId == "" {
		return utils.HandleValidationError(c, nil, "routeId 파라미터가 필요합니다")
	}

	stats, err := h.trackingService.GetRouteStatistics(routeId)
	if err != nil {
		return utils.HandleError(c, err, "노선 통계 조회 실패")
	}

	return utils.SendSuccessResponse(c, stats, fmt.Sprintf("노선 %s 통계 조회 성공", routeId))
}
