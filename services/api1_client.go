package services

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"bus-tracker/config"
	"bus-tracker/models"
	"bus-tracker/utils"
)

// API1Client 경기버스정보 API 클라이언트
type API1Client struct {
	APIClientBase
	client       *http.Client
	stationCache *StationCacheService
}

// NewAPI1Client 새로운 API1 클라이언트 생성
func NewAPI1Client(cfg *config.Config, logger *utils.Logger) *API1Client {
	return &API1Client{
		APIClientBase: APIClientBase{
			config: cfg,
			logger: logger,
		},
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		stationCache: NewStationCacheService(cfg, logger, "api1"),
	}
}

// GetAPIType API 타입 반환
func (ac *API1Client) GetAPIType() string {
	return "api1"
}

// FetchBusLocationByRoute 특정 routeId로 버스 데이터를 가져옴
func (ac *API1Client) FetchBusLocationByRoute(routeID string) ([]models.BusLocation, error) {
	// URL 생성
	apiURL := ac.buildAPIURL(routeID)

	ac.logger.Infof("API1 호출: %s", maskSensitiveURL(apiURL, ac.config.ServiceKey))

	resp, err := ac.client.Get(apiURL)
	if err != nil {
		return nil, fmt.Errorf("API1 호출 실패 (routeId: %s): %v", routeID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API1 응답 오류 (routeId: %s): %d", routeID, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("API1 응답 읽기 실패 (routeId: %s): %v", routeID, err)
	}

	// 응답 내용 디버깅 (처음 300자만)
	responsePreview := string(body)
	if len(responsePreview) > 300 {
		responsePreview = responsePreview[:300] + "..."
	}
	ac.logger.Infof("API1 응답 (routeId: %s): %s", routeID, responsePreview)

	return ac.parseResponse(body, routeID)
}

// parseResponse API1 응답 파싱
func (ac *API1Client) parseResponse(body []byte, routeID string) ([]models.BusLocation, error) {
	var apiResp models.API1Response
	if err := json.Unmarshal(body, &apiResp); err != nil {
		ac.logger.Errorf("API1 JSON 파싱 실패 (routeId: %s): %v", routeID, err)
		ac.logger.Errorf("원본 응답: %s", string(body))
		return nil, fmt.Errorf("API1 JSON 파싱 실패 (routeId: %s): %v", routeID, err)
	}

	// API 응답 성공 여부 확인
	if !apiResp.IsSuccess() {
		ac.logger.Warnf("API1 오류 응답 (routeId: %s): %s", routeID, apiResp.GetErrorMessage())
		return nil, fmt.Errorf("API1 오류 (routeId: %s): %s", routeID, apiResp.GetErrorMessage())
	}

	busLocations := apiResp.GetBusLocationList()
	ac.logger.Infof("API1 응답 성공 (routeId: %s): %s, 버스 수: %d",
		routeID, apiResp.Response.MsgHeader.ResultMessage, len(busLocations))

	// 응답에 routeId 정보 추가 (필요시)
	for i := range busLocations {
		if busLocations[i].RouteId == 0 {
			if routeIdInt, err := models.ParseRouteID(routeID); err == nil {
				busLocations[i].RouteId = routeIdInt
			}
		}

		// 정류소 정보 보강 (routeID를 직접 전달)
		ac.stationCache.EnrichBusLocationWithStationInfo(&busLocations[i], routeID)
	}

	return busLocations, nil
}

// FetchAllBusLocations 모든 routeId에 대해 버스 데이터를 병렬로 가져옴
func (ac *API1Client) FetchAllBusLocations(routeIDs []string) ([]models.BusLocation, error) {
	if len(routeIDs) == 0 {
		return nil, fmt.Errorf("routeIDs가 비어있습니다")
	}

	ac.logger.Infof("총 %d개 노선에 대해 API1 호출 시작", len(routeIDs))

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
			ac.logger.Errorf("노선 %s API1 호출 실패: %v", result.routeID, result.error)
			errorCount++
			continue
		}

		if len(result.busLocations) > 0 {
			allBusLocations = append(allBusLocations, result.busLocations...)
			ac.logger.Infof("노선 %s: %d개 데이터 수신", result.routeID, len(result.busLocations))

			// 모든 버스 데이터 로깅 (정류소 정보 포함)
			for i, bus := range result.busLocations {
				if bus.NodeNm != "" {
					// 정류소 정보가 보강된 경우
					ac.logger.Infof("  버스 %d/%d - 차량번호: %s, 차량ID: %d, 정류장: %s (%d), 순서: %d/%d, 혼잡도: %d",
						i+1, len(result.busLocations), bus.PlateNo, bus.VehId, bus.NodeNm, bus.StationId,
						bus.StationSeq, bus.TotalStations, bus.Crowded)
				} else {
					// 정류소 정보가 없는 경우
					ac.logger.Infof("  버스 %d/%d - 차량번호: %s, 차량ID: %d, 정류장ID: %d, 순서: %d/%d, 혼잡도: %d",
						i+1, len(result.busLocations), bus.PlateNo, bus.VehId, bus.StationId,
						bus.StationSeq, bus.TotalStations, bus.Crowded)
				}
			}
			successCount++
		} else {
			ac.logger.Warnf("노선 %s: 데이터 없음", result.routeID)
		}
	}

	ac.logger.Infof("API1 호출 완료 - 성공: %d개 노선, 실패: %d개 노선, 총 데이터: %d개",
		successCount, errorCount, len(allBusLocations))

	if errorCount == len(routeIDs) {
		return nil, fmt.Errorf("모든 노선의 API1 호출이 실패했습니다")
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
		if contains(baseURL, "?") {
			return baseURL + "&" + joinStrings(params, "&")
		}
		return baseURL + "?" + joinStrings(params, "&")
	}
	return baseURL
}

// LoadStationCache API1 정류소 정보 캐시 로드
func (ac *API1Client) LoadStationCache(routeIDs []string) error {
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
