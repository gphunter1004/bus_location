// services/unified_station_cache.go - API2 기준 통합 캐시
package services

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"bus-tracker/config"
	"bus-tracker/models"
	"bus-tracker/utils"
)

// UnifiedStationCacheService API2 기준 통합 정류소 캐시 서비스
type UnifiedStationCacheService struct {
	config       *config.Config
	logger       *utils.Logger
	client       *http.Client
	stationCache map[string]map[int]models.StationCache // routeId -> stationSeq(통합) -> StationCache
	mutex        sync.RWMutex
}

// NewUnifiedStationCacheService 새로운 통합 정류소 캐시 서비스 생성
func NewUnifiedStationCacheService(cfg *config.Config, logger *utils.Logger) *UnifiedStationCacheService {
	return &UnifiedStationCacheService{
		config: cfg,
		logger: logger,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		stationCache: make(map[string]map[int]models.StationCache),
	}
}

// LoadUnifiedStationCache API2를 우선으로 통합 정류소 캐시 로드
func (uscs *UnifiedStationCacheService) LoadUnifiedStationCache(api1RouteIDs, api2RouteIDs []string) error {
	uscs.logger.Info("=== 통합 정류소 캐시 로딩 시작 (API2 우선) ===")

	var allRouteIDs []string
	var wg sync.WaitGroup
	errorChan := make(chan error, len(api1RouteIDs)+len(api2RouteIDs))
	successCount := 0

	// API2 노선들을 먼저 로드 (우선순위)
	for _, routeID := range api2RouteIDs {
		allRouteIDs = append(allRouteIDs, routeID)
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			if err := uscs.loadAPI2StationCache(id); err != nil {
				errorChan <- fmt.Errorf("API2 노선 %s 정류소 정보 로드 실패: %v", id, err)
			} else {
				successCount++
			}
		}(routeID)
	}

	// API1 노선들 로드 (API2에 없는 노선만)
	for _, routeID := range api1RouteIDs {
		// 이미 API2에서 로드된 노선인지 확인
		if !containsRoute(api2RouteIDs, routeID) {
			allRouteIDs = append(allRouteIDs, routeID)
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				if err := uscs.loadAPI1StationCache(id); err != nil {
					errorChan <- fmt.Errorf("API1 노선 %s 정류소 정보 로드 실패: %v", id, err)
				} else {
					successCount++
				}
			}(routeID)
		} else {
			uscs.logger.Infof("노선 %s: API2에서 이미 로드됨, API1 로드 건너뜀", routeID)
		}
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
		uscs.logger.Error(err.Error())
	}

	uscs.logger.Infof("통합 정류소 캐시 로딩 완료 - 성공: %d개, 실패: %d개", successCount, len(errors))
	uscs.printUnifiedCacheStatistics()

	if len(errors) > 0 && successCount == 0 {
		return fmt.Errorf("모든 노선의 정류소 정보 로드가 실패했습니다")
	}

	return nil
}

// loadAPI2StationCache API2용 정류소 정보 로드 (우선)
func (uscs *UnifiedStationCacheService) loadAPI2StationCache(routeID string) error {
	// API URL 생성
	apiURL := uscs.buildAPI2StationInfoURL(routeID)

	uscs.logger.Infof("API2 정류소 정보 로드 (routeId: %s): %s", routeID,
		maskSensitiveURL(apiURL, uscs.config.ServiceKey))

	// API 호출
	resp, err := uscs.client.Get(apiURL)
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

	// JSON 파싱
	var stationResp models.StationInfoResponse
	if err := json.Unmarshal(body, &stationResp); err != nil {
		uscs.logger.Errorf("API2 정류소 정보 JSON 파싱 실패 (routeId: %s): %v", routeID, err)
		return fmt.Errorf("JSON 파싱 실패: %v", err)
	}

	// API 응답 성공 여부 확인
	if !stationResp.IsSuccess() {
		uscs.logger.Warnf("API2 정류소 정보 API 오류 응답 (routeId: %s): %s", routeID, stationResp.GetErrorMessage())
		return fmt.Errorf("API 오류: %s", stationResp.GetErrorMessage())
	}

	stations := stationResp.GetStationInfoList()
	uscs.logger.Infof("API2 노선 %s: %d개 정류소 정보 수신", routeID, len(stations))

	// 캐시에 저장 (NodeOrd를 통합 StationSeq로 사용)
	uscs.mutex.Lock()
	defer uscs.mutex.Unlock()

	if uscs.stationCache[routeID] == nil {
		uscs.stationCache[routeID] = make(map[int]models.StationCache)
	}

	for _, station := range stations {
		stationCache := station.ToStationCache()
		// NodeOrd를 통합 키로 사용 (StationSeq와 동일한 개념)
		uscs.stationCache[routeID][station.NodeOrd] = stationCache

		// 첫 번째와 마지막 정류소 정보 로깅
		if station.NodeOrd == 1 || station.NodeOrd == len(stations) {
			uscs.logger.Infof("  [API2] 정류소 %d: %s (%s) GPS:(%.6f,%.6f)",
				station.NodeOrd, station.NodeNm, station.NodeId, station.GPSLat, station.GPSLong)
		}
	}

	uscs.logger.Infof("API2 노선 %s 통합 캐시 저장 완료 - %d개 정류소", routeID, len(stations))
	return nil
}

// loadAPI1StationCache API1용 정류소 정보 로드 (보조)
func (uscs *UnifiedStationCacheService) loadAPI1StationCache(routeID string) error {
	// API URL 생성
	apiURL := uscs.buildAPI1StationInfoURL(routeID)

	uscs.logger.Infof("API1 정류소 정보 로드 (routeId: %s): %s", routeID,
		maskSensitiveURL(apiURL, uscs.config.ServiceKey))

	// API 호출
	resp, err := uscs.client.Get(apiURL)
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

	// JSON 파싱
	var stationResp models.API1StationInfoResponse
	if err := json.Unmarshal(body, &stationResp); err != nil {
		uscs.logger.Errorf("API1 정류소 정보 JSON 파싱 실패 (routeId: %s): %v", routeID, err)
		return fmt.Errorf("JSON 파싱 실패: %v", err)
	}

	// API 응답 성공 여부 확인
	if !stationResp.IsSuccess() {
		uscs.logger.Warnf("API1 정류소 정보 API 오류 응답 (routeId: %s): %s", routeID, stationResp.GetErrorMessage())
		return fmt.Errorf("API 오류: %s", stationResp.GetErrorMessage())
	}

	stations := stationResp.GetStationInfoList()
	uscs.logger.Infof("API1 노선 %s: %d개 정류소 정보 수신", routeID, len(stations))

	// 캐시에 저장 (StationSeq를 통합 키로 사용)
	uscs.mutex.Lock()
	defer uscs.mutex.Unlock()

	if uscs.stationCache[routeID] == nil {
		uscs.stationCache[routeID] = make(map[int]models.StationCache)
	}

	for _, station := range stations {
		stationCache := station.ToStationCache()
		// StationSeq를 통합 키로 사용
		uscs.stationCache[routeID][station.StationSeq] = stationCache

		// 첫 번째와 마지막 정류소 정보 로깅
		if station.StationSeq == 1 || station.StationSeq == len(stations) {
			uscs.logger.Infof("  [API1] 정류소 %d: %s (ID: %d) GPS:(%.6f,%.6f)",
				station.StationSeq, station.StationName, station.StationId, station.Y, station.X)
		}
	}

	uscs.logger.Infof("API1 노선 %s 통합 캐시 저장 완료 - %d개 정류소", routeID, len(stations))
	return nil
}

// GetStationInfo 통합 키로 정류소 정보 조회 (StationSeq/NodeOrd 통합)
func (uscs *UnifiedStationCacheService) GetStationInfo(routeID string, stationSeq int) (models.StationCache, bool) {
	uscs.mutex.RLock()
	defer uscs.mutex.RUnlock()

	routeStations, routeExists := uscs.stationCache[routeID]
	if !routeExists {
		return models.StationCache{}, false
	}

	station, stationExists := routeStations[stationSeq]
	return station, stationExists
}

// GetRouteStationCount 특정 노선의 전체 정류소 개수 반환
func (uscs *UnifiedStationCacheService) GetRouteStationCount(routeID string) int {
	uscs.mutex.RLock()
	defer uscs.mutex.RUnlock()

	if stations, exists := uscs.stationCache[routeID]; exists {
		return len(stations)
	}
	return 0
}

// EnrichBusLocationWithStationInfo 버스 위치 정보에 정류소 정보 보강 (통합 버전)
func (uscs *UnifiedStationCacheService) EnrichBusLocationWithStationInfo(busLocation *models.BusLocation, routeID string, stationSeq int) {
	if stationSeq <= 0 {
		uscs.logger.Warnf("유효하지 않은 정류장 순서 - 노선: %s, 순서: %d", routeID, stationSeq)
		return
	}

	// 통합 캐시에서 정류장 정보 조회
	if stationInfo, exists := uscs.GetStationInfo(routeID, stationSeq); exists {
		// 정류장 정보 보강
		busLocation.NodeId = stationInfo.NodeId
		busLocation.NodeNm = stationInfo.NodeNm
		busLocation.NodeNo = stationInfo.NodeNo
		busLocation.NodeOrd = stationSeq    // 통합된 순서 사용
		busLocation.StationSeq = stationSeq // 통합된 순서 사용

		// StationId 설정 (NodeId에서 추출)
		if stationInfo.NodeId != "" {
			if len(stationInfo.NodeId) > 3 && stationInfo.NodeId[:3] == "GGB" {
				// API2 형식: "GGB233003132" -> 233003132
				if stationIdInt, err := strconv.ParseInt(stationInfo.NodeId[3:], 10, 64); err == nil {
					busLocation.StationId = stationIdInt
				}
			} else {
				// API1 형식: 숫자 문자열 직접 변환
				if stationIdInt, err := strconv.ParseInt(stationInfo.NodeId, 10, 64); err == nil {
					busLocation.StationId = stationIdInt
				}
			}
		}

		// GPS 정보 설정
		if stationInfo.GPSLat != 0 && stationInfo.GPSLong != 0 {
			busLocation.GpsLati = stationInfo.GPSLat
			busLocation.GpsLong = stationInfo.GPSLong
		}

		// 전체 정류소 개수 설정
		busLocation.TotalStations = uscs.GetRouteStationCount(routeID)

		uscs.logger.Infof("통합 캐시 정류소 보강 - 노선: %s, 순서: %d/%d → %s (%s) GPS:(%.6f,%.6f)",
			routeID, stationSeq, busLocation.TotalStations, stationInfo.NodeNm, stationInfo.NodeId,
			stationInfo.GPSLat, stationInfo.GPSLong)
	} else {
		uscs.logger.Warnf("통합 캐시에서 정류소 정보 없음 - 노선: %s, 순서: %d", routeID, stationSeq)
		uscs.debugRouteStations(routeID)
	}
}

// buildAPI1StationInfoURL API1용 정류소 정보 API URL 생성
func (uscs *UnifiedStationCacheService) buildAPI1StationInfoURL(routeID string) string {
	baseURL := "https://apis.data.go.kr/6410000/busrouteservice/v2/getBusRouteStationListv2"
	params := []string{
		"serviceKey=" + uscs.config.ServiceKey,
		"routeId=" + routeID,
		"format=json",
	}
	return baseURL + "?" + strings.Join(params, "&")
}

// buildAPI2StationInfoURL API2용 정류소 정보 API URL 생성
func (uscs *UnifiedStationCacheService) buildAPI2StationInfoURL(routeID string) string {
	baseURL := "http://apis.data.go.kr/1613000/BusRouteInfoInqireService/getRouteAcctoThrghSttnList"
	params := []string{
		"serviceKey=" + uscs.config.ServiceKey,
		"cityCode=" + uscs.config.CityCode,
		"routeId=" + routeID,
		"_type=json",
		"numOfRows=100",
	}
	return baseURL + "?" + strings.Join(params, "&")
}

// printUnifiedCacheStatistics 통합 캐시 통계 출력
func (uscs *UnifiedStationCacheService) printUnifiedCacheStatistics() {
	uscs.mutex.RLock()
	defer uscs.mutex.RUnlock()

	totalStations := 0
	uscs.logger.Info("=== 통합 정류소 캐시 통계 ===")

	for routeID, stations := range uscs.stationCache {
		stationCount := len(stations)
		totalStations += stationCount
		uscs.logger.Infof("노선 %s: %d개 정류소 (통합 캐시)", routeID, stationCount)
	}

	uscs.logger.Infof("총 %d개 노선, %d개 정류소 정보 통합 캐시됨", len(uscs.stationCache), totalStations)
	uscs.logger.Info("캐시 우선순위: API2 (GPS+상세정보) > API1 (기본정보)")
	uscs.logger.Info("===============================")
}

// GetCacheStatistics 캐시 통계 반환
func (uscs *UnifiedStationCacheService) GetCacheStatistics() (int, int) {
	uscs.mutex.RLock()
	defer uscs.mutex.RUnlock()

	totalStations := 0
	for _, stations := range uscs.stationCache {
		totalStations += len(stations)
	}

	return len(uscs.stationCache), totalStations
}

// debugRouteStations 특정 노선의 캐시된 정류소 정보 디버깅 출력
func (uscs *UnifiedStationCacheService) debugRouteStations(routeID string) {
	uscs.mutex.RLock()
	defer uscs.mutex.RUnlock()

	if stations, exists := uscs.stationCache[routeID]; exists {
		uscs.logger.Infof("노선 %s의 통합 캐시 정류소 순서들:", routeID)

		// 순서대로 정렬해서 출력 (처음 10개만)
		count := 0
		for seq := 1; seq <= 100 && count < 10; seq++ {
			if station, exists := stations[seq]; exists {
				uscs.logger.Infof("  순서 %d: %s (%s) GPS:(%.6f,%.6f)",
					seq, station.NodeNm, station.NodeId, station.GPSLat, station.GPSLong)
				count++
			}
		}

		if len(stations) > 10 {
			uscs.logger.Infof("  ... 총 %d개 정류소 (처음 %d개만 표시)", len(stations), count)
		}
	} else {
		uscs.logger.Warnf("노선 %s의 통합 캐시 정보가 없습니다", routeID)
	}
}

// containsRoute 문자열 슬라이스에 특정 문자열이 포함되어 있는지 확인
func containsRoute(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
