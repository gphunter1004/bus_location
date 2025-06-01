package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"bus-tracker/config"
	"bus-tracker/internal/models"
	"bus-tracker/internal/services/cache"
	"bus-tracker/internal/utils"
)

// API2Client 공공데이터포털 버스위치정보 API 클라이언트 (통합 모드 전용)
type API2Client struct {
	APIClientBase
	client       *http.Client
	stationCache cache.StationCacheInterface // 🔄 인터페이스로 변경
}

// NewAPI2ClientWithSharedCache 공유 캐시를 사용하는 API2 클라이언트 생성 (통합 모드용)
func NewAPI2ClientWithSharedCache(cfg *config.Config, logger *utils.Logger, sharedCache cache.StationCacheInterface) *API2Client {
	return &API2Client{
		APIClientBase: APIClientBase{
			config: cfg,
			logger: logger,
		},
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		stationCache: sharedCache, // 🔄 인터페이스로 받음
	}
}

// GetAPIType API 타입 반환
func (ac *API2Client) GetAPIType() string {
	return "api2"
}

// LoadStationCache 정류소 정보 캐시 로드 (API2 전용)
func (ac *API2Client) LoadStationCache(routeIDs []string) error {
	// Route ID 형식 검증
	for _, routeID := range routeIDs {
		if err := ac.validateAPI2RouteID(routeID); err != nil {
			return fmt.Errorf("정류소 캐시 로딩 중 Route ID 검증 실패: %v", err)
		}
	}

	// 인터페이스 메서드 호출
	return ac.stationCache.LoadStationCache(routeIDs)
}

// GetCacheStatistics 캐시 통계 반환 (API2 전용)
func (ac *API2Client) GetCacheStatistics() (int, int) {
	// 인터페이스 메서드 호출
	return ac.stationCache.GetCacheStatistics()
}

// GetRouteStationCount 특정 노선의 전체 정류소 개수 반환 (API2 전용)
func (ac *API2Client) GetRouteStationCount(routeID string) int {
	// 인터페이스 메서드 호출
	return ac.stationCache.GetRouteStationCount(routeID)
}

// validateAPI2RouteID API2 Route ID 형식 검증
func (ac *API2Client) validateAPI2RouteID(routeID string) error {
	if routeID == "" {
		return fmt.Errorf("routeID가 비어있습니다")
	}

	// API2는 GGB로 시작해야 함
	if !utils.String.HasPrefix(routeID, "GGB") {
		return fmt.Errorf("API2 routeID는 'GGB'로 시작해야 합니다: '%s'", routeID)
	}

	if len(routeID) < 4 {
		return fmt.Errorf("API2 routeID가 너무 짧습니다: '%s'", routeID)
	}

	// GGB 이후 부분이 숫자인지 확인
	numericPart := routeID[3:]
	for _, char := range numericPart {
		if char < '0' || char > '9' {
			return fmt.Errorf("API2 routeID의 GGB 이후 부분은 숫자여야 합니다: '%s'", routeID)
		}
	}

	return nil
}

// FetchBusLocationByRoute 특정 routeId로 버스 데이터를 가져옴
func (ac *API2Client) FetchBusLocationByRoute(routeID string) ([]models.BusLocation, error) {
	// Route ID 형식 검증
	if err := ac.validateAPI2RouteID(routeID); err != nil {
		return nil, fmt.Errorf("Route ID 형식 오류: %v", err)
	}

	// URL 생성
	apiURL := ac.buildAPIURL(routeID)
	ac.logger.Infof("API2 호출 URL: %s", utils.String.MaskSensitiveURL(apiURL, ac.config.ServiceKey))

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
	ac.logger.Infof("API2 응답 (routeId: %s): %s", routeID, responsePreview)

	return ac.parseResponse(body, routeID)
}

// parseResponse API2 응답 파싱 (검증 강화 및 에러 처리 개선)
func (ac *API2Client) parseResponse(body []byte, routeID string) ([]models.BusLocation, error) {
	// 응답이 비어있는 경우 처리
	if len(body) == 0 {
		ac.logger.Warnf("API2 빈 응답 (routeId: %s)", routeID)
		return []models.BusLocation{}, nil
	}

	// JSON 파싱 전 응답 구조 확인
	var rawResponse map[string]interface{}
	if err := json.Unmarshal(body, &rawResponse); err != nil {
		ac.logger.Errorf("API2 JSON 사전 파싱 실패 (routeId: %s): %v", routeID, err)
		ac.logger.Errorf("응답 내용: %s", string(body))
		return nil, fmt.Errorf("API2 JSON 사전 파싱 실패 (routeId: %s): %v", routeID, err)
	}

	// 응답 구조 로깅 (디버깅용)
	ac.logger.Debugf("API2 응답 구조 (routeId: %s): response 키 존재 여부: %v", routeID, rawResponse["response"] != nil)

	var apiResp models.API2Response
	if err := json.Unmarshal(body, &apiResp); err != nil {
		ac.logger.Errorf("API2 JSON 파싱 실패 (routeId: %s): %v", routeID, err)

		// 상세한 파싱 오류 정보 제공
		if strings.Contains(err.Error(), "cannot unmarshal") {
			ac.logger.Errorf("파싱 오류 상세: items.item 구조가 예상과 다름")

			// items.item 구조 확인
			if response, ok := rawResponse["response"].(map[string]interface{}); ok {
				if body, ok := response["body"].(map[string]interface{}); ok {
					if items, ok := body["items"].(map[string]interface{}); ok {
						if item, exists := items["item"]; exists {
							ac.logger.Errorf("items.item 타입: %T", item)
							if itemStr, err := json.Marshal(item); err == nil {
								ac.logger.Errorf("items.item 내용: %s", string(itemStr))
							}
						}
					}
				}
			}
		}

		return nil, fmt.Errorf("API2 JSON 파싱 실패 (routeId: %s): %v", routeID, err)
	}

	// API 응답 성공 여부 확인
	if !apiResp.IsSuccess() {
		ac.logger.Warnf("API2 응답 오류 (routeId: %s): %s", routeID, apiResp.GetErrorMessage())
		return nil, fmt.Errorf("API2 오류 (routeId: %s): %s", routeID, apiResp.GetErrorMessage())
	}

	busItems := apiResp.GetBusLocationItemList()
	ac.logger.Infof("API2 파싱 완료 (routeId: %s): %d개 버스 아이템", routeID, len(busItems))

	// 데이터 품질 검증
	validItems := []models.API2BusLocationItem{}

	for i, item := range busItems {
		// 필수 필드 검증
		if item.VehicleNo == "" {
			ac.logger.Warnf("버스 %d: 차량번호 없음, 건너뛰기", i)
			continue
		}

		// 정류장 정보 상태 확인 (디버깅용)
		hasStationInfo := item.NodeId != "" || item.NodeOrd > 0
		if !hasStationInfo {
			ac.logger.Infof("정류장 정보 없는 버스 - 차량번호: %s, NodeId: '%s', NodeOrd: %d, GPS: (%.6f, %.6f)",
				item.VehicleNo, item.NodeId, item.NodeOrd, item.GpsLati, item.GpsLong)
		}

		// GPS 좌표 유효성 검증 (한국 좌표 범위)
		if item.GpsLati < 33.0 || item.GpsLati > 38.0 || item.GpsLong < 124.0 || item.GpsLong > 132.0 {
			ac.logger.Warnf("버스 %s: GPS 좌표 이상 (%.6f, %.6f)", item.VehicleNo, item.GpsLati, item.GpsLong)
			// GPS 좌표가 이상해도 데이터는 유지 (정류장 정보가 있으면)
		}

		// 노선 정보 검증
		if item.RouteNm == 0 {
			ac.logger.Warnf("버스 %s: 노선번호 없음", item.VehicleNo)
		}

		validItems = append(validItems, item)
	}

	ac.logger.Infof("API2 검증 완료 (routeId: %s): %d/%d개 유효 버스", routeID, len(validItems), len(busItems))

	// API2BusLocationItem을 BusLocation으로 변환
	var busLocations []models.BusLocation
	for _, item := range validItems {
		busLocation := item.ConvertToBusLocation()

		// 정류소 정보 보강 (routeID 사용) - 인터페이스 메서드 호출
		ac.stationCache.EnrichBusLocationWithStationInfo(&busLocation, routeID)

		// 전체 정류소 개수가 설정되지 않은 경우 설정
		if busLocation.TotalStations == 0 {
			busLocation.TotalStations = ac.stationCache.GetRouteStationCount(routeID)
		}

		busLocations = append(busLocations, busLocation)
	}

	ac.logger.Infof("API2 변환 완료 (routeId: %s): %d개 버스 위치", routeID, len(busLocations))

	return busLocations, nil
}

// FetchAllBusLocations 모든 routeId에 대해 버스 데이터를 병렬로 가져옴
func (ac *API2Client) FetchAllBusLocations(routeIDs []string) ([]models.BusLocation, error) {
	if len(routeIDs) == 0 {
		return nil, fmt.Errorf("routeIDs가 비어있습니다")
	}

	// Route ID 형식 사전 검증
	for _, routeID := range routeIDs {
		if err := ac.validateAPI2RouteID(routeID); err != nil {
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
			ac.logger.Errorf("API2 노선 %s 호출 실패: %v", result.routeID, result.error)
			errorCount++
			lastError = result.error
			continue
		}

		if len(result.busLocations) > 0 {
			allBusLocations = append(allBusLocations, result.busLocations...)
			ac.logger.Infof("API2 노선 %s: %d개 버스 데이터 수집", result.routeID, len(result.busLocations))
		} else {
			ac.logger.Infof("API2 노선 %s: 운행 중인 버스 없음", result.routeID)
		}
	}

	// 전체 실패 시에만 에러 반환
	if errorCount == len(routeIDs) {
		return nil, fmt.Errorf("모든 노선의 API2 호출이 실패했습니다. 마지막 오류: %v", lastError)
	}

	// 부분 실패 시 경고 로그
	if errorCount > 0 {
		ac.logger.Warnf("API2 호출 부분 실패: %d/%d 노선 실패, 총 %d개 버스 데이터 수집",
			errorCount, len(routeIDs), len(allBusLocations))
	} else {
		ac.logger.Infof("API2 호출 성공: 모든 노선에서 총 %d개 버스 데이터 수집", len(allBusLocations))
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

	baseURL := ac.config.API2Config.BaseURL
	if len(params) > 0 {
		if utils.String.Contains(baseURL, "?") {
			return baseURL + "&" + utils.String.Join(params, "&")
		}
		return baseURL + "?" + utils.String.Join(params, "&")
	}
	return baseURL
}
