package services

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"bus-tracker/config"
	"bus-tracker/models"
	"bus-tracker/utils"
)

// StationCacheService 정류소 정보 캐시 서비스
type StationCacheService struct {
	config       *config.Config
	logger       *utils.Logger
	client       *http.Client
	stationCache map[string]map[int]models.StationCache // routeId -> nodeOrd(API2) 또는 stationSeq(API1) -> StationCache
	apiType      string                                 // "api1" 또는 "api2"
	mutex        sync.RWMutex
}

// NewStationCacheService 새로운 정류소 캐시 서비스 생성
func NewStationCacheService(cfg *config.Config, logger *utils.Logger, apiType string) *StationCacheService {
	return &StationCacheService{
		config: cfg,
		logger: logger,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		stationCache: make(map[string]map[int]models.StationCache),
		apiType:      apiType,
	}
}

// LoadStationCache 모든 노선의 정류소 정보를 미리 로드
func (scs *StationCacheService) LoadStationCache(routeIDs []string) error {
	scs.logger.Infof("정류소 정보 캐시 로딩 시작 - 총 %d개 노선 (%s)", len(routeIDs), scs.apiType)

	var wg sync.WaitGroup
	errorChan := make(chan error, len(routeIDs))
	successCount := 0

	for _, routeID := range routeIDs {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()

			if err := scs.loadStationCacheForRoute(id); err != nil {
				errorChan <- fmt.Errorf("노선 %s 정류소 정보 로드 실패: %v", id, err)
			} else {
				successCount++
			}
		}(routeID)
	}

	// 모든 고루틴 완료 대기
	go func() {
		wg.Wait()
		close(errorChan)
	}()

	// 에러 수집
	var errors []error
	for err := range errorChan {
		errors = append(errors, err)
		scs.logger.Error(err.Error())
	}

	scs.logger.Infof("정류소 정보 캐시 로딩 완료 - 성공: %d개, 실패: %d개", successCount, len(errors))

	// 전체 캐시 통계 출력
	scs.printCacheStatistics()

	if len(errors) > 0 && successCount == 0 {
		return fmt.Errorf("모든 노선의 정류소 정보 로드가 실패했습니다")
	}

	return nil
}

// loadStationCacheForRoute 특정 노선의 정류소 정보 로드
func (scs *StationCacheService) loadStationCacheForRoute(routeID string) error {
	// API 타입에 따라 다른 처리
	if scs.apiType == "api1" {
		return scs.loadAPI1StationCache(routeID)
	} else {
		return scs.loadAPI2StationCache(routeID)
	}
}

// loadAPI1StationCache API1용 정류소 정보 로드
func (scs *StationCacheService) loadAPI1StationCache(routeID string) error {
	// API URL 생성
	apiURL := scs.buildAPI1StationInfoURL(routeID)

	scs.logger.Infof("API1 정류소 정보 API 호출 (routeId: %s): %s", routeID,
		maskSensitiveURL(apiURL, scs.config.ServiceKey))

	// API 호출
	resp, err := scs.client.Get(apiURL)
	if err != nil {
		return fmt.Errorf("API 호출 실패: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("API 응답 오류: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("응답 읽기 실패: %v", err)
	}

	// 응답 내용 디버깅 (처음 500자만)
	responsePreview := string(body)
	if len(responsePreview) > 500 {
		responsePreview = responsePreview[:500] + "..."
	}
	scs.logger.Infof("API1 정류소 정보 API 응답 (routeId: %s): %s", routeID, responsePreview)

	// JSON 파싱
	var stationResp models.API1StationInfoResponse
	if err := json.Unmarshal(body, &stationResp); err != nil {
		scs.logger.Errorf("API1 정류소 정보 JSON 파싱 실패 (routeId: %s): %v", routeID, err)
		scs.logger.Errorf("원본 응답: %s", string(body))
		return fmt.Errorf("JSON 파싱 실패: %v", err)
	}

	// API 응답 성공 여부 확인
	if !stationResp.IsSuccess() {
		scs.logger.Warnf("API1 정류소 정보 API 오류 응답 (routeId: %s): %s", routeID, stationResp.GetErrorMessage())
		return fmt.Errorf("API 오류: %s", stationResp.GetErrorMessage())
	}

	stations := stationResp.GetStationInfoList()
	scs.logger.Infof("API1 노선 %s: %d개 정류소 정보 수신", routeID, len(stations))

	// 캐시에 저장 (API1은 stationSeq 기준)
	scs.mutex.Lock()
	defer scs.mutex.Unlock()

	if scs.stationCache[routeID] == nil {
		scs.stationCache[routeID] = make(map[int]models.StationCache)
	}

	for _, station := range stations {
		stationCache := station.ToStationCache()
		scs.stationCache[routeID][station.StationSeq] = stationCache

		// 첫 번째와 마지막 정류소 정보 로깅
		if station.StationSeq == 1 || station.StationSeq == len(stations) {
			scs.logger.Infof("  정류소 %d: %s (ID: %d)", station.StationSeq, station.StationName, station.StationId)
		}
	}

	// 캐시 저장 확인 로깅
	scs.logger.Infof("API1 노선 %s 정류소 캐시 저장 완료 - %d개 정류소", routeID, len(stations))

	return nil
}

// loadAPI2StationCache API2용 정류소 정보 로드
func (scs *StationCacheService) loadAPI2StationCache(routeID string) error {
	// API URL 생성
	apiURL := scs.buildAPI2StationInfoURL(routeID)

	scs.logger.Infof("API2 정류소 정보 API 호출 (routeId: %s): %s", routeID,
		maskSensitiveURL(apiURL, scs.config.ServiceKey))

	// API 호출
	resp, err := scs.client.Get(apiURL)
	if err != nil {
		return fmt.Errorf("API 호출 실패: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("API 응답 오류: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("응답 읽기 실패: %v", err)
	}

	// 응답 내용 디버깅 (처음 500자만)
	responsePreview := string(body)
	if len(responsePreview) > 500 {
		responsePreview = responsePreview[:500] + "..."
	}
	scs.logger.Infof("API2 정류소 정보 API 응답 (routeId: %s): %s", routeID, responsePreview)

	// JSON 파싱
	var stationResp models.StationInfoResponse
	if err := json.Unmarshal(body, &stationResp); err != nil {
		scs.logger.Errorf("API2 정류소 정보 JSON 파싱 실패 (routeId: %s): %v", routeID, err)
		scs.logger.Errorf("원본 응답: %s", string(body))
		return fmt.Errorf("JSON 파싱 실패: %v", err)
	}

	// API 응답 성공 여부 확인
	if !stationResp.IsSuccess() {
		scs.logger.Warnf("API2 정류소 정보 API 오류 응답 (routeId: %s): %s", routeID, stationResp.GetErrorMessage())
		return fmt.Errorf("API 오류: %s", stationResp.GetErrorMessage())
	}

	stations := stationResp.GetStationInfoList()
	scs.logger.Infof("API2 노선 %s: %d개 정류소 정보 수신", routeID, len(stations))

	// 캐시에 저장 (API2는 nodeOrd 기준)
	scs.mutex.Lock()
	defer scs.mutex.Unlock()

	if scs.stationCache[routeID] == nil {
		scs.stationCache[routeID] = make(map[int]models.StationCache)
	}

	for _, station := range stations {
		stationCache := station.ToStationCache()
		scs.stationCache[routeID][station.NodeOrd] = stationCache

		// 첫 번째와 마지막 정류소 정보 로깅
		if station.NodeOrd == 1 || station.NodeOrd == len(stations) {
			scs.logger.Infof("  정류소 %d: %s (%s)", station.NodeOrd, station.NodeNm, station.NodeId)
		}
	}

	// 캐시 저장 확인 로깅
	scs.logger.Infof("API2 노선 %s 정류소 캐시 저장 완료 - %d개 정류소", routeID, len(stations))

	return nil
}

// GetStationInfo 키로 정류소 정보 조회 (API1: stationSeq, API2: nodeOrd)
func (scs *StationCacheService) GetStationInfo(routeID string, key int) (models.StationCache, bool) {
	scs.mutex.RLock()
	defer scs.mutex.RUnlock()

	routeStations, routeExists := scs.stationCache[routeID]
	if !routeExists {
		return models.StationCache{}, false
	}

	station, stationExists := routeStations[key]
	return station, stationExists
}

// GetRouteStationCount 특정 노선의 전체 정류소 개수 반환
func (scs *StationCacheService) GetRouteStationCount(routeID string) int {
	scs.mutex.RLock()
	defer scs.mutex.RUnlock()

	if stations, exists := scs.stationCache[routeID]; exists {
		return len(stations)
	}
	return 0
}

// EnrichBusLocationWithStationInfo 버스 위치 정보에 정류소 정보 보강
func (scs *StationCacheService) EnrichBusLocationWithStationInfo(busLocation *models.BusLocation, routeID string) {
	var lookupKey int

	// API 타입에 따라 다른 키 사용
	if scs.apiType == "api1" {
		// API1: StationSeq 기준 (BusLocation의 StationSeq 사용)
		lookupKey = busLocation.StationSeq
	} else {
		// API2: NodeOrd 기준
		lookupKey = busLocation.NodeOrd

		// NodeOrd가 0이면 보강하지 않음
		if busLocation.NodeOrd <= 0 {
			scs.logger.Warnf("순서가 유효하지 않음 - 노선: %s, 순서: %d, 차량번호: %s",
				routeID, busLocation.NodeOrd, busLocation.PlateNo)
			return
		}

		// 이미 정류소 정보가 있는지 확인 (버스 위치 API에서 이미 제공된 경우)
		if busLocation.NodeNm != "" && busLocation.NodeId != "" {
			// 정류소 정보가 이미 있어도 전체 정류소 개수는 설정해야 함
			busLocation.TotalStations = scs.GetRouteStationCount(routeID)

			scs.logger.Infof("정류소 정보 이미 존재 - 노선: %s, 순서: %d/%d → %s (%s) [캐시 불필요]",
				routeID, busLocation.NodeOrd, busLocation.TotalStations, busLocation.NodeNm, busLocation.NodeId)
			return
		}
	}

	// routeID를 직접 사용 (API 호출에서 사용한 routeId와 동일)
	if stationInfo, exists := scs.GetStationInfo(routeID, lookupKey); exists {
		// 정류소 정보 보강
		busLocation.NodeId = stationInfo.NodeId
		busLocation.NodeNm = stationInfo.NodeNm
		busLocation.NodeNo = stationInfo.NodeNo // 이미 int이므로 직접 대입

		// API별로 순서 필드 설정
		if scs.apiType == "api1" {
			// API1: StationSeq가 기본이므로 NodeOrd에도 복사
			busLocation.NodeOrd = stationInfo.NodeOrd // StationSeq 값
		} else {
			// API2: NodeOrd는 이미 설정되어 있음
		}

		// StationId도 NodeId에서 추출해서 설정 (정류장 변경 추적용)
		if scs.apiType == "api1" {
			// API1: NodeId가 이미 StationId 숫자 문자열이므로 직접 변환
			if stationIdInt, err := strconv.ParseInt(stationInfo.NodeId, 10, 64); err == nil {
				busLocation.StationId = stationIdInt
			}
		} else {
			// API2: NodeId에서 숫자 부분 추출 (예: "GGB233003132" -> 233003132)
			if stationInfo.NodeId != "" && len(stationInfo.NodeId) > 3 {
				if stationIdInt, err := strconv.ParseInt(stationInfo.NodeId[3:], 10, 64); err == nil {
					busLocation.StationId = stationIdInt
				}
			}
		}

		// GPS 정보가 있으면 추가 (이미 float64이므로 직접 사용)
		if stationInfo.GPSLat != 0 && stationInfo.GPSLong != 0 {
			busLocation.GpsLati = stationInfo.GPSLat
			busLocation.GpsLong = stationInfo.GPSLong
		}

		// 전체 정류소 개수 설정
		busLocation.TotalStations = scs.GetRouteStationCount(routeID)

		// 전체 정류소 개수 정보 추가
		scs.logger.Infof("정류소 정보 보강 완료 - 노선: %s, 순서: %d/%d → %s (%s) [캐시 사용]",
			routeID, lookupKey, busLocation.TotalStations, stationInfo.NodeNm, stationInfo.NodeId)
	} else {
		scs.logger.Warnf("정류소 정보 없음 - 노선: %s, 순서: %d (캐시에서 찾을 수 없음)",
			routeID, lookupKey)

		// 해당 노선의 캐시된 정류소 순서들 출력 (디버깅용)
		scs.debugRouteStations(routeID)
	}
}

// buildAPI1StationInfoURL API1용 정류소 정보 API URL 생성
func (scs *StationCacheService) buildAPI1StationInfoURL(routeID string) string {
	baseURL := "https://apis.data.go.kr/6410000/busrouteservice/v2/getBusRouteStationListv2"

	params := []string{
		"serviceKey=" + scs.config.ServiceKey,
		"routeId=" + routeID,
		"format=json",
	}

	return baseURL + "?" + joinStrings(params, "&")
}

// buildAPI2StationInfoURL API2용 정류소 정보 API URL 생성
func (scs *StationCacheService) buildAPI2StationInfoURL(routeID string) string {
	baseURL := "http://apis.data.go.kr/1613000/BusRouteInfoInqireService/getRouteAcctoThrghSttnList"

	params := []string{
		"serviceKey=" + scs.config.ServiceKey,
		"cityCode=" + scs.config.CityCode,
		"routeId=" + routeID,
		"_type=json",
		"numOfRows=100",
	}

	return baseURL + "?" + joinStrings(params, "&")
}

// printCacheStatistics 캐시 통계 출력
func (scs *StationCacheService) printCacheStatistics() {
	scs.mutex.RLock()
	defer scs.mutex.RUnlock()

	totalStations := 0
	scs.logger.Infof("=== %s 정류소 캐시 통계 ===", scs.apiType)

	for routeID, stations := range scs.stationCache {
		stationCount := len(stations)
		totalStations += stationCount
		scs.logger.Infof("노선 %s: %d개 정류소", routeID, stationCount)
	}

	scs.logger.Infof("총 %d개 노선, %d개 정류소 정보 캐시됨", len(scs.stationCache), totalStations)
	scs.logger.Info("========================")
}

// GetCacheStatistics 캐시 통계 반환
func (scs *StationCacheService) GetCacheStatistics() (int, int) {
	scs.mutex.RLock()
	defer scs.mutex.RUnlock()

	totalStations := 0
	for _, stations := range scs.stationCache {
		totalStations += len(stations)
	}

	return len(scs.stationCache), totalStations
}

// debugRouteStations 특정 노선의 캐시된 정류소 정보 디버깅 출력
func (scs *StationCacheService) debugRouteStations(routeID string) {
	scs.mutex.RLock()
	defer scs.mutex.RUnlock()

	if stations, exists := scs.stationCache[routeID]; exists {
		scs.logger.Infof("노선 %s의 캐시된 정류소 순서들:", routeID)

		// 순서대로 정렬해서 출력 (처음 10개만)
		var orders []int
		for ord := range stations {
			orders = append(orders, ord)
			if len(orders) >= 10 { // 처음 10개만
				break
			}
		}

		for _, ord := range orders {
			station := stations[ord]
			scs.logger.Infof("  순서 %d: %s (%s)", ord, station.NodeNm, station.NodeId)
		}

		if len(stations) > 10 {
			scs.logger.Infof("  ... 총 %d개 정류소 (처음 10개만 표시)", len(stations))
		}
	} else {
		scs.logger.Warnf("노선 %s의 캐시 정보가 없습니다", routeID)
	}
}
