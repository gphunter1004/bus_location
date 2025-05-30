package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"bus-tracker/config"
	"bus-tracker/internal/models"
	"bus-tracker/internal/services/cache"
	"bus-tracker/internal/utils"
)

// API1Client 경기버스정보 API 클라이언트
type API1Client struct {
	APIClientBase
	client       *http.Client
	stationCache *cache.StationCacheService
}

// NewAPI1Client 새로운 API1 클라이언트 생성 (기본 버전)
func NewAPI1Client(cfg *config.Config, logger *utils.Logger) *API1Client {
	return &API1Client{
		APIClientBase: APIClientBase{
			config: cfg,
			logger: logger,
		},
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		stationCache: cache.NewStationCacheService(cfg, logger, "api1"),
	}
}

// NewAPI1ClientWithSharedCache 공유 캐시를 사용하는 API1 클라이언트 생성 (통합 모드용)
func NewAPI1ClientWithSharedCache(cfg *config.Config, logger *utils.Logger, sharedCache *cache.StationCacheService) *API1Client {
	return &API1Client{
		APIClientBase: APIClientBase{
			config: cfg,
			logger: logger,
		},
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		stationCache: sharedCache,
	}
}

// GetAPIType API 타입 반환
func (ac *API1Client) GetAPIType() string {
	return "api1"
}

// validateAPI1RouteID API1 Route ID 형식 검증
func (ac *API1Client) validateAPI1RouteID(routeID string) error {
	if routeID == "" {
		return fmt.Errorf("routeID가 비어있습니다")
	}

	// API1은 숫자만 허용
	for _, char := range routeID {
		if char < '0' || char > '9' {
			return fmt.Errorf("API1 routeID는 숫자만 허용됩니다: '%s'", routeID)
		}
	}

	return nil
}

// FetchBusLocationByRoute 특정 routeId로 버스 데이터를 가져옴
func (ac *API1Client) FetchBusLocationByRoute(routeID string) ([]models.BusLocation, error) {
	// Route ID 형식 검증
	if err := ac.validateAPI1RouteID(routeID); err != nil {
		return nil, fmt.Errorf("Route ID 형식 오류: %v", err)
	}

	// URL 생성
	apiURL := ac.buildAPIURL(routeID)
	ac.logger.Infof("API1 호출 URL: %s", utils.MaskSensitiveURL(apiURL, ac.config.ServiceKey))

	resp, err := ac.client.Get(apiURL)
	if err != nil {
		return nil, fmt.Errorf("API1 호출 실패 (routeId: %s): %v", routeID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API1 응답 오류 (routeId: %s): HTTP %d", routeID, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("API1 응답 읽기 실패 (routeId: %s): %v", routeID, err)
	}

	// 응답 내용 디버깅 (처음 200자만)
	responsePreview := string(body)
	if len(responsePreview) > 200 {
		responsePreview = responsePreview[:200] + "..."
	}
	ac.logger.Infof("API1 응답 (routeId: %s): %s", routeID, responsePreview)

	return ac.parseResponse(body, routeID)
}

// parseResponse API1 응답 파싱
func (ac *API1Client) parseResponse(body []byte, routeID string) ([]models.BusLocation, error) {
	var apiResp models.API1Response
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, fmt.Errorf("API1 JSON 파싱 실패 (routeId: %s): %v", routeID, err)
	}

	// API 응답 성공 여부 확인
	if !apiResp.IsSuccess() {
		return nil, fmt.Errorf("API1 오류 (routeId: %s): %s", routeID, apiResp.GetErrorMessage())
	}

	busLocations := apiResp.GetBusLocationList()

	// 응답에 routeId 정보 추가 및 정류소 정보 보강
	for i := range busLocations {
		if busLocations[i].RouteId == 0 {
			if routeIdInt, err := models.ParseRouteID(routeID); err == nil {
				busLocations[i].RouteId = routeIdInt
			}
		}

		// 정류소 정보 보강 (routeID 사용)
		ac.stationCache.EnrichBusLocationWithStationInfo(&busLocations[i], routeID)
	}

	return busLocations, nil
}

// FetchAllBusLocations 모든 routeId에 대해 버스 데이터를 병렬로 가져옴
func (ac *API1Client) FetchAllBusLocations(routeIDs []string) ([]models.BusLocation, error) {
	if len(routeIDs) == 0 {
		return nil, fmt.Errorf("routeIDs가 비어있습니다")
	}

	// Route ID 형식 사전 검증
	for _, routeID := range routeIDs {
		if err := ac.validateAPI1RouteID(routeID); err != nil {
			ac.logger.Errorf("Route ID 형식 검증 실패: %v", err)
			return nil, fmt.Errorf("Route ID 형식 검증 실패: %v", err)
		}
	}

	// 채널과 WaitGroup을 사용한 병렬 처리
	type routeResult struct {
		routeID      string
		busLocations []models.BusLocation
		error        error
	}

	resultChan := make(chan routeResult, len(routeIDs))
	var wg sync.WaitGroup

	// 각 routeId에 대해 병렬로 API 호출
	for _, routeID := range routeIDs {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()

			busLocations, err := ac.FetchBusLocationByRoute(id)
			resultChan <- routeResult{
				routeID:      id,
				busLocations: busLocations,
				error:        err,
			}
		}(routeID)
	}

	// 모든 고루틴이 완료될 때까지 대기
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// 결과 수집
	var allBusLocations []models.BusLocation
	errorCount := 0
	var lastError error

	for result := range resultChan {
		if result.error != nil {
			ac.logger.Errorf("API1 노선 %s 호출 실패: %v", result.routeID, result.error)
			errorCount++
			lastError = result.error
			continue
		}

		if len(result.busLocations) > 0 {
			allBusLocations = append(allBusLocations, result.busLocations...)
		}
	}

	if errorCount == len(routeIDs) {
		return nil, fmt.Errorf("모든 노선의 API1 호출이 실패했습니다. 마지막 오류: %v", lastError)
	}

	return allBusLocations, nil
}

// buildAPIURL API1용 URL 생성
func (ac *API1Client) buildAPIURL(routeID string) string {
	params := []string{
		"serviceKey=" + ac.config.ServiceKey,
		"format=json",
		"routeId=" + routeID,
	}

	baseURL := ac.config.APIBaseURL
	if len(params) > 0 {
		if utils.Contains(baseURL, "?") {
			return baseURL + "&" + utils.JoinStrings(params, "&")
		}
		return baseURL + "?" + utils.JoinStrings(params, "&")
	}
	return baseURL
}

// LoadStationCache API1 정류소 정보 캐시 로드
func (ac *API1Client) LoadStationCache(routeIDs []string) error {
	if len(routeIDs) == 0 {
		return fmt.Errorf("routeIDs가 비어있습니다")
	}

	// Route ID 형식 검증
	for _, routeID := range routeIDs {
		if err := ac.validateAPI1RouteID(routeID); err != nil {
			return fmt.Errorf("정류소 캐시 로딩 중 Route ID 검증 실패: %v", err)
		}
	}

	return ac.stationCache.LoadStationCache(routeIDs)
}

// GetCacheStatistics 캐시 통계 반환 (API1 전용)
func (ac *API1Client) GetCacheStatistics() (int, int) {
	return ac.stationCache.GetCacheStatistics()
}

// GetRouteStationCount 특정 노선의 전체 정류소 개수 반환 (API1 전용)
func (ac *API1Client) GetRouteStationCount(routeID string) int {
	return ac.stationCache.GetRouteStationCount(routeID)
}
