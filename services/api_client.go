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

// APIClient API 호출을 담당하는 클라이언트
type APIClient struct {
	config *config.Config
	logger *utils.Logger
	client *http.Client
}

// NewAPIClient 새로운 API 클라이언트 생성
func NewAPIClient(cfg *config.Config, logger *utils.Logger) *APIClient {
	return &APIClient{
		config: cfg,
		logger: logger,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// FetchBusLocationByRoute 특정 routeId로 버스 데이터를 가져옴 (API 타입에 따라 다른 처리)
func (ac *APIClient) FetchBusLocationByRoute(routeID string) ([]models.BusLocation, error) {
	// URL 생성
	apiURL := ac.config.BuildAPIURL(routeID)
	
	ac.logger.Infof("API 호출 (%s): %s", ac.config.APIType, ac.maskSensitiveURL(apiURL))
	
	resp, err := ac.client.Get(apiURL)
	if err != nil {
		return nil, fmt.Errorf("API 호출 실패 (routeId: %s): %v", routeID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API 응답 오류 (routeId: %s): %d", routeID, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("응답 읽기 실패 (routeId: %s): %v", routeID, err)
	}

	// 응답 내용 디버깅 (처음 300자만)
	responsePreview := string(body)
	if len(responsePreview) > 300 {
		responsePreview = responsePreview[:300] + "..."
	}
	ac.logger.Infof("API 응답 (routeId: %s): %s", routeID, responsePreview)

	// API 타입에 따라 다른 구조체로 파싱
	if ac.config.APIType == "api2" {
		return ac.parseAPI2Response(body, routeID)
	} else {
		return ac.parseAPI1Response(body, routeID)
	}
}

// parseAPI1Response API1(경기버스정보) 응답 파싱
func (ac *APIClient) parseAPI1Response(body []byte, routeID string) ([]models.BusLocation, error) {
	var apiResp models.API1Response
	if err := json.Unmarshal(body, &apiResp); err != nil {
		ac.logger.Errorf("JSON 파싱 실패 (routeId: %s): %v", routeID, err)
		ac.logger.Errorf("원본 응답: %s", string(body))
		return nil, fmt.Errorf("JSON 파싱 실패 (routeId: %s): %v", routeID, err)
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
	}

	return busLocations, nil
}

// parseAPI2Response API2(공공데이터포털) 응답 파싱
func (ac *APIClient) parseAPI2Response(body []byte, routeID string) ([]models.BusLocation, error) {
	var apiResp models.API2Response
	if err := json.Unmarshal(body, &apiResp); err != nil {
		ac.logger.Errorf("JSON 파싱 실패 (routeId: %s): %v", routeID, err)
		ac.logger.Errorf("원본 응답: %s", string(body))
		return nil, fmt.Errorf("JSON 파싱 실패 (routeId: %s): %v", routeID, err)
	}

	// API 응답 성공 여부 확인
	if !apiResp.IsSuccess() {
		ac.logger.Warnf("API2 오류 응답 (routeId: %s): %s", routeID, apiResp.GetErrorMessage())
		return nil, fmt.Errorf("API2 오류 (routeId: %s): %s", routeID, apiResp.GetErrorMessage())
	}

	busItems := apiResp.GetBusLocationItemList()
	ac.logger.Infof("API2 응답 성공 (routeId: %s): %s, 버스 데이터 수: %d", 
		routeID, apiResp.Response.Header.ResultMsg, len(busItems))

	// API2BusLocationItem을 BusLocation으로 변환
	var busLocations []models.BusLocation
	for _, item := range busItems {
		busLocation := item.ConvertToBusLocation()
		busLocations = append(busLocations, busLocation)
	}

	ac.logger.Infof("API2 데이터 변환 완료 - %d개 버스 정보", len(busLocations))

	return busLocations, nil
}

// FetchAllBusLocations 모든 routeId에 대해 버스 데이터를 병렬로 가져옴
func (ac *APIClient) FetchAllBusLocations(routeIDs []string) ([]models.BusLocation, error) {
	if len(routeIDs) == 0 {
		return nil, fmt.Errorf("routeIDs가 비어있습니다")
	}

	ac.logger.Infof("총 %d개 노선에 대해 %s API 호출 시작", len(routeIDs), ac.config.APIType)

	// 채널과 WaitGroup을 사용한 병렬 처리
	type routeResult struct {
		routeID     string
		busLocations []models.BusLocation
		error       error
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
				routeID:     id,
				busLocations: busLocations,
				error:       err,
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
			ac.logger.Errorf("노선 %s API 호출 실패: %v", result.routeID, result.error)
			errorCount++
			continue
		}

		if len(result.busLocations) > 0 {
			allBusLocations = append(allBusLocations, result.busLocations...)
			ac.logger.Infof("노선 %s: %d개 데이터 수신", result.routeID, len(result.busLocations))
			
			// 첫 번째 데이터 샘플 로깅
			firstBus := result.busLocations[0]
			if ac.config.APIType == "api2" {
				// API2는 버스 위치 정보
				ac.logger.Infof("  샘플 버스 - 차량번호: %s, 정류장ID: %d, 정류장순서: %d, 잔여좌석: %d", 
					firstBus.PlateNo, firstBus.StationId, firstBus.StationSeq, firstBus.RemainSeatCnt)
			} else {
				// API1은 버스 위치 정보 위주
				ac.logger.Infof("  샘플 버스 - 차량번호: %s, 차량ID: %d, 정류장: %d, 혼잡도: %d", 
					firstBus.PlateNo, firstBus.VehId, firstBus.StationId, firstBus.Crowded)
			}
			successCount++
		} else {
			ac.logger.Warnf("노선 %s: 데이터 없음", result.routeID)
		}
	}

	ac.logger.Infof("API 호출 완료 - 성공: %d개 노선, 실패: %d개 노선, 총 데이터: %d개", 
		successCount, errorCount, len(allBusLocations))

	if errorCount == len(routeIDs) {
		return nil, fmt.Errorf("모든 노선의 API 호출이 실패했습니다")
	}

	return allBusLocations, nil
}

// maskSensitiveURL serviceKey가 포함된 URL에서 민감한 정보 마스킹
func (ac *APIClient) maskSensitiveURL(url string) string {
	// serviceKey 파라미터를 찾아서 마스킹
	if len(ac.config.ServiceKey) > 10 {
		masked := ac.config.ServiceKey[:6] + "***" + ac.config.ServiceKey[len(ac.config.ServiceKey)-4:]
		return fmt.Sprintf("URL: %s... (serviceKey: %s)", url[:80], masked)
	}
	return url[:80] + "..."
}