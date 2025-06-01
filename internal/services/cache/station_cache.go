// internal/services/cache/station_cache_v2.go - 개선된 캐시 구조
package cache

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"bus-tracker/config"
	"bus-tracker/internal/models"
	"bus-tracker/internal/utils"
)

// StationCacheServiceV2 개선된 정류소 캐시 서비스
// 구조: RouteID -> StationOrder -> StationData
type StationCacheServiceV2 struct {
	config *config.Config
	logger *utils.Logger
	client *http.Client
	// 🆕 개선된 캐시 구조: map[RouteID]map[StationOrder]StationData
	stationCache   map[string]map[int]models.StationCache // RouteID -> StationOrder -> StationData
	apiType        string                                 // "api1" 또는 "api2"
	mutex          sync.RWMutex
	routeConverter *RouteIDConverter
}

// NewStationCacheServiceV2 새로운 개선된 정류소 캐시 서비스 생성
func NewStationCacheServiceV2(cfg *config.Config, logger *utils.Logger, apiType string) *StationCacheServiceV2 {
	return &StationCacheServiceV2{
		config: cfg,
		logger: logger,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		stationCache:   make(map[string]map[int]models.StationCache), // RouteID -> StationOrder -> Data
		apiType:        apiType,
		routeConverter: NewRouteIDConverter(),
	}
}

// GetStationByRouteAndOrder 노선ID와 정류장 순서로 정류소 정보 조회
func (scs *StationCacheServiceV2) GetStationByRouteAndOrder(routeID string, stationOrder int) (models.StationCache, bool) {
	// 통합 키로 변환
	unifiedRouteID := scs.routeConverter.ToUnifiedKey(routeID)

	scs.mutex.RLock()
	defer scs.mutex.RUnlock()

	// 노선 존재 확인
	routeStations, routeExists := scs.stationCache[unifiedRouteID]
	if !routeExists {
		scs.logger.Debugf("노선 캐시 없음: %s (통합키: %s)", routeID, unifiedRouteID)
		return models.StationCache{}, false
	}

	// 정류장 순서로 조회
	station, stationExists := routeStations[stationOrder]
	if !stationExists {
		scs.logger.Debugf("정류장 캐시 없음: 노선=%s, 순서=%d (가능한 순서: %v)",
			routeID, stationOrder, scs.getAvailableOrders(routeStations))
		return models.StationCache{}, false
	}

	scs.logger.Debugf("캐시 히트: 노선=%s, 순서=%d -> %s", routeID, stationOrder, station.NodeNm)
	return station, true
}

// GetRouteStations 특정 노선의 모든 정류소 조회 (순서대로 정렬)
func (scs *StationCacheServiceV2) GetRouteStations(routeID string) ([]models.StationCache, bool) {
	unifiedRouteID := scs.routeConverter.ToUnifiedKey(routeID)

	scs.mutex.RLock()
	defer scs.mutex.RUnlock()

	routeStations, exists := scs.stationCache[unifiedRouteID]
	if !exists {
		return nil, false
	}

	// 순서대로 정렬하여 반환
	var stations []models.StationCache
	for order := 1; order <= len(routeStations)+10; order++ { // 여유분 추가
		if station, found := routeStations[order]; found {
			stations = append(stations, station)
		}
	}

	return stations, len(stations) > 0
}

// GetRouteStationCount 특정 노선의 전체 정류소 개수 반환
func (scs *StationCacheServiceV2) GetRouteStationCount(routeID string) int {
	unifiedRouteID := scs.routeConverter.ToUnifiedKey(routeID)

	scs.mutex.RLock()
	defer scs.mutex.RUnlock()

	if stations, exists := scs.stationCache[unifiedRouteID]; exists {
		return len(stations)
	}
	return 0
}

// AddStationToCache 캐시에 정류소 추가
func (scs *StationCacheServiceV2) AddStationToCache(routeID string, stationOrder int, station models.StationCache) {
	unifiedRouteID := scs.routeConverter.ToUnifiedKey(routeID)

	scs.mutex.Lock()
	defer scs.mutex.Unlock()

	// 노선이 없으면 생성
	if scs.stationCache[unifiedRouteID] == nil {
		scs.stationCache[unifiedRouteID] = make(map[int]models.StationCache)
	}

	// 정류소 추가
	scs.stationCache[unifiedRouteID][stationOrder] = station
	scs.logger.Debugf("캐시 추가: 노선=%s, 순서=%d, 정류장=%s", routeID, stationOrder, station.NodeNm)
}

// EnrichBusLocationWithStationInfo 버스 위치 정보에 정류소 정보 보강
func (scs *StationCacheServiceV2) EnrichBusLocationWithStationInfo(busLocation *models.BusLocation, routeID string) {
	// 우선순위: NodeOrd -> StationSeq
	var stationOrder int
	if busLocation.NodeOrd > 0 {
		stationOrder = busLocation.NodeOrd
	} else if busLocation.StationSeq > 0 {
		stationOrder = busLocation.StationSeq
	} else {
		// 순서 정보가 없으면 전체 정류소 개수만 설정
		busLocation.TotalStations = scs.GetRouteStationCount(routeID)
		scs.logger.Debugf("순서 정보 없음: 차량=%s, 노선=%s", busLocation.PlateNo, routeID)
		return
	}

	// 캐시에서 정류소 정보 조회
	if station, found := scs.GetStationByRouteAndOrder(routeID, stationOrder); found {
		// 정류소 정보 보강
		if busLocation.NodeNm == "" {
			busLocation.NodeNm = station.NodeNm
		}
		if busLocation.NodeId == "" {
			busLocation.NodeId = station.NodeId
		}
		if busLocation.NodeNo == 0 {
			busLocation.NodeNo = station.NodeNo
		}
		if busLocation.GpsLati == 0 && station.GPSLat != 0 {
			busLocation.GpsLati = station.GPSLat
		}
		if busLocation.GpsLong == 0 && station.GPSLong != 0 {
			busLocation.GpsLong = station.GPSLong
		}

		// StationId 보강
		if busLocation.StationId == 0 && station.StationId > 0 {
			busLocation.StationId = station.StationId
		}

		busLocation.TotalStations = scs.GetRouteStationCount(routeID)

		scs.logger.Debugf("정류소 정보 보강 완료: 차량=%s, 노선=%s, 순서=%d -> %s",
			busLocation.PlateNo, routeID, stationOrder, station.NodeNm)
	} else {
		// 캐시 미스
		busLocation.TotalStations = scs.GetRouteStationCount(routeID)
		scs.logger.Debugf("캐시 미스: 차량=%s, 노선=%s, 순서=%d", busLocation.PlateNo, routeID, stationOrder)
	}
}

// LoadStationCache 모든 노선의 정류소 정보를 미리 로드
func (scs *StationCacheServiceV2) LoadStationCache(routeIDs []string) error {
	// 중복 제거: 통합 키 기준으로 고유한 노선만 처리
	uniqueRoutes := make(map[string]string) // unifiedKey -> originalRouteID
	for _, routeID := range routeIDs {
		if err := scs.routeConverter.ValidateRouteID(routeID, scs.apiType); err != nil {
			return err
		}

		_, unifiedKey := scs.routeConverter.GetOriginalAndUnified(routeID)

		// 이미 같은 통합 키가 있는지 확인
		if _, exists := uniqueRoutes[unifiedKey]; !exists {
			uniqueRoutes[unifiedKey] = routeID
		}
	}

	// API2 우선으로 정류소 정보 로드
	var wg sync.WaitGroup
	successCount := 0
	var successMutex sync.Mutex

	for unifiedKey, routeID := range uniqueRoutes {
		wg.Add(1)
		go func(key, id string) {
			defer wg.Done()

			// 이미 캐시가 있는지 확인
			scs.mutex.RLock()
			_, cacheExists := scs.stationCache[key]
			scs.mutex.RUnlock()

			if cacheExists {
				successMutex.Lock()
				successCount++
				successMutex.Unlock()
				return
			}

			// 캐시 로딩 시도
			var err error
			for attempt := 1; attempt <= 3; attempt++ {
				if err = scs.loadStationCacheForRoute(id); err == nil {
					successMutex.Lock()
					successCount++
					successMutex.Unlock()
					return
				}
				if attempt < 3 {
					time.Sleep(time.Duration(attempt) * time.Second)
				}
			}
		}(unifiedKey, routeID)
	}

	wg.Wait()

	if successCount == 0 {
		return fmt.Errorf("모든 노선의 정류소 정보 로드가 실패했습니다")
	}

	scs.logger.Infof("정류소 캐시 로드 완료: %d/%d 노선 성공", successCount, len(uniqueRoutes))
	return nil
}

// loadStationCacheForRoute 특정 노선의 정류소 정보 로드
func (scs *StationCacheServiceV2) loadStationCacheForRoute(routeID string) error {
	// API2 우선으로 처리
	if scs.apiType == "api2" || scs.apiType == "unified" {
		return scs.loadAPI2StationCache(routeID)
	} else {
		return scs.loadAPI1StationCache(routeID)
	}
}

// loadAPI2StationCache API2용 정류소 정보 로드
func (scs *StationCacheServiceV2) loadAPI2StationCache(routeID string) error {
	api2RouteID := scs.routeConverter.ToAPI2Format(routeID)
	unifiedKey := scs.routeConverter.ToUnifiedKey(routeID)

	apiURL := scs.buildAPI2StationInfoURL(api2RouteID)

	resp, err := scs.client.Get(apiURL)
	if err != nil {
		return fmt.Errorf("API 호출 실패: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("API 응답 오류: HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("응답 읽기 실패: %v", err)
	}

	var stationResp models.StationInfoResponse
	if err := json.Unmarshal(body, &stationResp); err != nil {
		return fmt.Errorf("JSON 파싱 실패: %v", err)
	}

	if !stationResp.IsSuccess() {
		return fmt.Errorf("API 오류 (코드: %s): %s", stationResp.Response.Header.ResultCode, stationResp.GetErrorMessage())
	}

	stations := stationResp.GetStationInfoList()
	if len(stations) == 0 {
		return fmt.Errorf("노선 %s에 대한 정류소 정보가 없습니다", api2RouteID)
	}

	// 🆕 개선된 캐시 구조로 저장: RouteID -> StationOrder -> StationData
	scs.mutex.Lock()
	defer scs.mutex.Unlock()

	if scs.stationCache[unifiedKey] == nil {
		scs.stationCache[unifiedKey] = make(map[int]models.StationCache)
	}

	for _, station := range stations {
		stationCache := station.ToStationCache()
		// NodeOrd를 정류장 순서로 사용
		if station.NodeOrd > 0 {
			scs.stationCache[unifiedKey][station.NodeOrd] = stationCache
		}
	}

	scs.logger.Infof("API2 정류소 캐시 로드 완료: 노선=%s, 정류소=%d개", routeID, len(stations))
	return nil
}

// loadAPI1StationCache API1용 정류소 정보 로드
func (scs *StationCacheServiceV2) loadAPI1StationCache(routeID string) error {
	api1RouteID := scs.routeConverter.ToAPI1Format(routeID)
	unifiedKey := scs.routeConverter.ToUnifiedKey(routeID)

	apiURL := scs.buildAPI1StationInfoURL(api1RouteID)

	resp, err := scs.client.Get(apiURL)
	if err != nil {
		return fmt.Errorf("API 호출 실패: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("API 응답 오류: HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("응답 읽기 실패: %v", err)
	}

	var stationResp models.API1StationInfoResponse
	if err := json.Unmarshal(body, &stationResp); err != nil {
		return fmt.Errorf("JSON 파싱 실패: %v", err)
	}

	if !stationResp.IsSuccess() {
		return fmt.Errorf("API 오류 (코드: %d): %s", stationResp.Response.MsgHeader.ResultCode, stationResp.GetErrorMessage())
	}

	stations := stationResp.GetStationInfoList()
	if len(stations) == 0 {
		return fmt.Errorf("노선 %s에 대한 정류소 정보가 없습니다", api1RouteID)
	}

	// 🆕 개선된 캐시 구조로 저장: RouteID -> StationOrder -> StationData
	scs.mutex.Lock()
	defer scs.mutex.Unlock()

	if scs.stationCache[unifiedKey] == nil {
		scs.stationCache[unifiedKey] = make(map[int]models.StationCache)
	}

	// API2 데이터가 없는 경우에만 API1 데이터 사용
	for _, station := range stations {
		stationCache := station.ToStationCache()

		// API1의 StationSeq를 정류장 순서로 사용
		stationSeq := station.StationSeq
		if stationSeq > 0 {
			// API2 데이터가 없는 경우에만 API1 데이터 사용
			if _, exists := scs.stationCache[unifiedKey][stationSeq]; !exists {
				scs.stationCache[unifiedKey][stationSeq] = stationCache
			}
		}
	}

	scs.logger.Infof("API1 정류소 캐시 로드 완료: 노선=%s, 정류소=%d개", routeID, len(stations))
	return nil
}

// GetCacheStatistics 캐시 통계 반환
func (scs *StationCacheServiceV2) GetCacheStatistics() (routeCount, stationCount int) {
	scs.mutex.RLock()
	defer scs.mutex.RUnlock()

	routeCount = len(scs.stationCache)
	for _, stations := range scs.stationCache {
		stationCount += len(stations)
	}

	return routeCount, stationCount
}

// PrintCacheStatus 캐시 상태 출력
func (scs *StationCacheServiceV2) PrintCacheStatus() {
	routeCount, stationCount := scs.GetCacheStatistics()

	scs.logger.Infof("📊 정류소 캐시 상태 - 노선: %d개, 정류소: %d개", routeCount, stationCount)

	// 상세 정보 (DEBUG 레벨)
	scs.mutex.RLock()
	for routeID, stations := range scs.stationCache {
		if len(stations) > 0 {
			orders := scs.getAvailableOrders(stations)
			scs.logger.Debugf("   노선 %s: %d개 정류소 (순서: %v)", routeID, len(stations), orders[:min(5, len(orders))])
		}
	}
	scs.mutex.RUnlock()
}

// getAvailableOrders 사용 가능한 정류장 순서 목록 반환 (디버깅용)
func (scs *StationCacheServiceV2) getAvailableOrders(stations map[int]models.StationCache) []int {
	var orders []int
	for order := range stations {
		orders = append(orders, order)
	}
	return orders
}

// API URL 생성 헬퍼 함수들 (기존과 동일)
func (scs *StationCacheServiceV2) buildAPI1StationInfoURL(routeID string) string {
	baseURL := "https://apis.data.go.kr/6410000/busrouteservice/v2/getBusRouteStationListv2"
	params := []string{
		"serviceKey=" + scs.config.ServiceKey,
		"routeId=" + routeID,
		"format=json",
	}
	return baseURL + "?" + utils.String.Join(params, "&")
}

func (scs *StationCacheServiceV2) buildAPI2StationInfoURL(routeID string) string {
	baseURL := "http://apis.data.go.kr/1613000/BusRouteInfoInqireService/getRouteAcctoThrghSttnList"
	params := []string{
		"serviceKey=" + scs.config.ServiceKey,
		"cityCode=" + scs.config.CityCode,
		"routeId=" + routeID,
		"_type=json",
		"numOfRows=200",
	}
	return baseURL + "?" + utils.String.Join(params, "&")
}

// min 함수
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
