package services

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"bus-tracker/config"
	"bus-tracker/models"
	"bus-tracker/utils"
)

// API2Client 공공데이터포털 버스위치정보 API 클라이언트
type API2Client struct {
	APIClientBase
	client       *http.Client
	stationCache *StationCacheService
}

// NewAPI2Client 새로운 API2 클라이언트 생성 (기본 버전)
func NewAPI2Client(cfg *config.Config, logger *utils.Logger) *API2Client {
	return &API2Client{
		APIClientBase: APIClientBase{
			config: cfg,
			logger: logger,
		},
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		stationCache: NewStationCacheService(cfg, logger, "api2"),
	}
}

// NewAPI2ClientWithSharedCache 공유 캐시를 사용하는 API2 클라이언트 생성 (통합 모드용)
func NewAPI2ClientWithSharedCache(cfg *config.Config, logger *utils.Logger, sharedCache *StationCacheService) *API2Client {
	logger.Infof("🔗 API2 클라이언트 생성 - 통합 캐시 공유 모드")
	return &API2Client{
		APIClientBase: APIClientBase{
			config: cfg,
			logger: logger,
		},
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		stationCache: sharedCache, // 🔧 통합 캐시 사용
	}
}

// GetAPIType API 타입 반환
func (ac *API2Client) GetAPIType() string {
	return "api2"
}

// LoadStationCache 정류소 정보 캐시 로드 (API2 전용)
func (ac *API2Client) LoadStationCache(routeIDs []string) error {
	ac.logger.Infof("🏗️ API2 정류소 캐시 로딩 시작 - 노선: %v", routeIDs)

	// Route ID 형식 검증
	for _, routeID := range routeIDs {
		if err := ac.validateAPI2RouteID(routeID); err != nil {
			return fmt.Errorf("정류소 캐시 로딩 중 Route ID 검증 실패: %v", err)
		}
	}

	return ac.stationCache.LoadStationCache(routeIDs)
}

// GetCacheStatistics 캐시 통계 반환 (API2 전용)
func (ac *API2Client) GetCacheStatistics() (int, int) {
	return ac.stationCache.GetCacheStatistics()
}

// GetRouteStationCount 특정 노선의 전체 정류소 개수 반환 (API2 전용)
func (ac *API2Client) GetRouteStationCount(routeID string) int {
	return ac.stationCache.GetRouteStationCount(routeID)
}

// validateAPI2RouteID API2 Route ID 형식 검증
func (ac *API2Client) validateAPI2RouteID(routeID string) error {
	if routeID == "" {
		return fmt.Errorf("routeID가 비어있습니다")
	}

	// API2는 GGB로 시작해야 함
	if !strings.HasPrefix(routeID, "GGB") {
		return fmt.Errorf("API2 routeID는 'GGB'로 시작해야 합니다: '%s'", routeID)
	}

	if len(routeID) < 4 {
		return fmt.Errorf("API2 routeID가 너무 짧습니다: '%s' (최소 4자 이상)", routeID)
	}

	// GGB 이후 부분이 숫자인지 확인
	numericPart := routeID[3:]
	for _, char := range numericPart {
		if char < '0' || char > '9' {
			return fmt.Errorf("API2 routeID의 GGB 이후 부분은 숫자여야 합니다: '%s' (잘못된 문자: %c)", routeID, char)
		}
	}

	ac.logger.Infof("✅ API2 Route ID 검증 통과: '%s'", routeID)
	return nil
}

// FetchBusLocationByRoute 특정 routeId로 버스 데이터를 가져옴
func (ac *API2Client) FetchBusLocationByRoute(routeID string) ([]models.BusLocation, error) {
	// Route ID 형식 검증 (API2: GGB + 숫자)
	if err := ac.validateAPI2RouteID(routeID); err != nil {
		return nil, fmt.Errorf("Route ID 형식 오류: %v", err)
	}

	// URL 생성
	apiURL := ac.buildAPIURL(routeID)

	ac.logger.Infof("🚌 API2 호출 시작 - 노선: %s", routeID)
	ac.logger.Infof("📡 요청 URL: %s", maskSensitiveURL(apiURL, ac.config.ServiceKey))

	resp, err := ac.client.Get(apiURL)
	if err != nil {
		return nil, fmt.Errorf("API2 호출 실패 (routeId: %s): %v", routeID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API2 응답 오류 (routeId: %s): HTTP %d", routeID, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("API2 응답 읽기 실패 (routeId: %s): %v", routeID, err)
	}

	// 응답 내용 디버깅 (처음 500자만)
	responsePreview := string(body)
	if len(responsePreview) > 500 {
		responsePreview = responsePreview[:500] + "..."
	}
	ac.logger.Infof("📄 API2 응답 (routeId: %s): %s", routeID, responsePreview)

	return ac.parseResponse(body, routeID)
}

// parseResponse API2 응답 파싱
func (ac *API2Client) parseResponse(body []byte, routeID string) ([]models.BusLocation, error) {
	var apiResp models.API2Response
	if err := json.Unmarshal(body, &apiResp); err != nil {
		ac.logger.Errorf("❌ API2 JSON 파싱 실패 (routeId: %s): %v", routeID, err)
		ac.logger.Errorf("파싱 실패한 원본 응답: %s", string(body))
		return nil, fmt.Errorf("API2 JSON 파싱 실패 (routeId: %s): %v", routeID, err)
	}

	// API 응답 성공 여부 확인
	if !apiResp.IsSuccess() {
		ac.logger.Warnf("⚠️ API2 오류 응답 (routeId: %s): %s", routeID, apiResp.GetErrorMessage())
		return nil, fmt.Errorf("API2 오류 (routeId: %s): %s", routeID, apiResp.GetErrorMessage())
	}

	busItems := apiResp.GetBusLocationItemList()
	ac.logger.Infof("✅ API2 응답 성공 (routeId: %s): %s, 버스 데이터 수: %d",
		routeID, apiResp.Response.Header.ResultMsg, len(busItems))

	// API2BusLocationItem을 BusLocation으로 변환
	var busLocations []models.BusLocation
	for _, item := range busItems {
		busLocation := item.ConvertToBusLocation()

		// 정류소 정보 보강 (동일한 routeID 사용 - 중요!)
		ac.stationCache.EnrichBusLocationWithStationInfo(&busLocation, routeID)

		// 혹시 전체 정류소 개수가 설정되지 않은 경우 다시 설정
		if busLocation.TotalStations == 0 {
			busLocation.TotalStations = ac.stationCache.GetRouteStationCount(routeID)
		}

		busLocations = append(busLocations, busLocation)
	}

	ac.logger.Infof("🔄 API2 데이터 변환 완료 - %d대 버스 정보", len(busLocations))

	return busLocations, nil
}

// FetchAllBusLocations 모든 routeId에 대해 버스 데이터를 병렬로 가져옴
func (ac *API2Client) FetchAllBusLocations(routeIDs []string) ([]models.BusLocation, error) {
	if len(routeIDs) == 0 {
		return nil, fmt.Errorf("routeIDs가 비어있습니다")
	}

	ac.logger.Infof("🚀 총 %d개 노선에 대해 API2 호출 시작", len(routeIDs))
	ac.logger.Infof("🔢 API2 노선 목록: %v", routeIDs)

	// Route ID 형식 사전 검증
	for _, routeID := range routeIDs {
		if err := ac.validateAPI2RouteID(routeID); err != nil {
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
	successCount := 0
	errorCount := 0

	for result := range resultChan {
		if result.error != nil {
			ac.logger.Errorf("❌ 노선 %s API2 호출 실패: %v", result.routeID, result.error)
			errorCount++
			continue
		}

		if len(result.busLocations) > 0 {
			allBusLocations = append(allBusLocations, result.busLocations...)
			ac.logger.Infof("✅ 노선 %s: %d대 버스 데이터 수신", result.routeID, len(result.busLocations))

			// 모든 버스 데이터 로깅 (정류소 정보 포함)
			for i, bus := range result.busLocations {
				if bus.NodeNm != "" {
					ac.logger.Infof("  🚌 버스 %d/%d - 차량번호: %s, 정류장: %s (%s), 순서: %d/%d, GPS: (%.6f, %.6f)",
						i+1, len(result.busLocations), bus.PlateNo, bus.NodeNm, bus.NodeId,
						bus.NodeOrd, bus.TotalStations, bus.GpsLati, bus.GpsLong)
				} else {
					ac.logger.Infof("  🚌 버스 %d/%d - 차량번호: %s, 정류장ID: %d, 정류장순서: %d/%d, GPS: (%.6f, %.6f)",
						i+1, len(result.busLocations), bus.PlateNo, bus.StationId,
						bus.StationSeq, bus.TotalStations, bus.GpsLati, bus.GpsLong)
				}
			}
			successCount++
		} else {
			ac.logger.Warnf("⚠️ 노선 %s: 데이터 없음", result.routeID)
		}
	}

	ac.logger.Infof("📊 API2 호출 완료 - 성공: %d개 노선, 실패: %d개 노선, 총 데이터: %d대",
		successCount, errorCount, len(allBusLocations))

	if errorCount == len(routeIDs) {
		return nil, fmt.Errorf("모든 노선의 API2 호출이 실패했습니다")
	}

	return allBusLocations, nil
}

// buildAPIURL API2용 URL 생성
func (ac *API2Client) buildAPIURL(routeID string) string {
	params := []string{
		"serviceKey=" + ac.config.ServiceKey,
		"cityCode=" + ac.config.CityCode,
		"routeId=" + routeID,
		"_type=json",
		"numOfRows=100",
	}

	baseURL := ac.config.APIBaseURL
	if len(params) > 0 {
		if contains(baseURL, "?") {
			return baseURL + "&" + joinStrings(params, "&")
		}
		return baseURL + "?" + joinStrings(params, "&")
	}
	return baseURL
}
