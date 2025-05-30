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

// StationCacheService 정류소 정보 캐시 서비스 (StationId 기반)
type StationCacheService struct {
	config         *config.Config
	logger         *utils.Logger
	client         *http.Client
	stationCache   map[string]map[int]models.StationCache // unifiedKey -> nodeOrd/stationSeq -> StationCache
	apiType        string                                 // "api1" 또는 "api2"
	mutex          sync.RWMutex
	routeConverter *RouteIDConverter
}

// NewStationCacheService 새로운 정류소 캐시 서비스 생성
func NewStationCacheService(cfg *config.Config, logger *utils.Logger, apiType string) *StationCacheService {
	return &StationCacheService{
		config: cfg,
		logger: logger,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		stationCache:   make(map[string]map[int]models.StationCache),
		apiType:        apiType,
		routeConverter: NewRouteIDConverter(),
	}
}

// LoadStationCache 모든 노선의 정류소 정보를 미리 로드 (API2 우선)
func (scs *StationCacheService) LoadStationCache(routeIDs []string) error {
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

	return nil
}

// loadStationCacheForRoute 특정 노선의 정류소 정보 로드
func (scs *StationCacheService) loadStationCacheForRoute(routeID string) error {
	// API2 우선으로 처리
	if scs.apiType == "api2" || scs.apiType == "unified" {
		return scs.loadAPI2StationCache(routeID)
	} else {
		return scs.loadAPI1StationCache(routeID)
	}
}

// loadAPI2StationCache API2용 정류소 정보 로드 (우선)
func (scs *StationCacheService) loadAPI2StationCache(routeID string) error {
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

	// 통합 키로 캐시에 저장 (API2 정보 우선)
	scs.mutex.Lock()
	defer scs.mutex.Unlock()

	if scs.stationCache[unifiedKey] == nil {
		scs.stationCache[unifiedKey] = make(map[int]models.StationCache)
	}

	for _, station := range stations {
		stationCache := station.ToStationCache()
		// NodeOrd를 키로 사용 (API2에서는 NodeOrd가 정류장 순서)
		if station.NodeOrd > 0 {
			scs.stationCache[unifiedKey][station.NodeOrd] = stationCache
		}
	}

	return nil
}

// loadAPI1StationCache API1용 정류소 정보 로드 (보조)
func (scs *StationCacheService) loadAPI1StationCache(routeID string) error {
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

	// 통합 키로 캐시에 저장
	scs.mutex.Lock()
	defer scs.mutex.Unlock()

	if scs.stationCache[unifiedKey] == nil {
		scs.stationCache[unifiedKey] = make(map[int]models.StationCache)
	}

	// API2 데이터가 없는 경우에만 API1 데이터 사용
	for _, station := range stations {
		stationCache := station.ToStationCache()

		// API1의 StationSeq를 키로 사용
		stationSeq := station.StationSeq
		if stationSeq > 0 {
			// API2 데이터가 없는 경우에만 API1 데이터 사용
			if _, exists := scs.stationCache[unifiedKey][stationSeq]; !exists {
				scs.stationCache[unifiedKey][stationSeq] = stationCache
			} else {
				// API2에서 제공하지 않는 정보만 업데이트
				existing := scs.stationCache[unifiedKey][stationSeq]
				// API1에서만 제공되는 정보가 있다면 여기서 업데이트
				// 현재는 API2가 더 완전한 정보를 제공하므로 덮어쓰지 않음
				scs.stationCache[unifiedKey][stationSeq] = existing
			}
		}
	}

	return nil
}

// GetStationInfo 통합 키로 정류소 정보 조회 (NodeOrd/StationSeq 기반)
func (scs *StationCacheService) GetStationInfo(routeNmOrId string, nodeOrdOrSeq int) (models.StationCache, bool) {
	unifiedKey := scs.routeConverter.ToUnifiedKey(routeNmOrId)

	scs.mutex.RLock()
	defer scs.mutex.RUnlock()

	routeStations, routeExists := scs.stationCache[unifiedKey]
	if !routeExists {
		return models.StationCache{}, false
	}

	station, stationExists := routeStations[nodeOrdOrSeq]
	return station, stationExists
}

// GetRouteStationCount 특정 노선의 전체 정류소 개수 반환
func (scs *StationCacheService) GetRouteStationCount(routeNmOrId string) int {
	unifiedKey := scs.routeConverter.ToUnifiedKey(routeNmOrId)

	scs.mutex.RLock()
	defer scs.mutex.RUnlock()

	if stations, exists := scs.stationCache[unifiedKey]; exists {
		return len(stations)
	}
	return 0
}

// EnrichBusLocationWithStationInfo 버스 위치 정보에 정류소 정보 보강 (NodeOrd/StationSeq 기반)
func (scs *StationCacheService) EnrichBusLocationWithStationInfo(busLocation *models.BusLocation, routeNmOrId string) {
	// NodeOrd 또는 StationSeq를 키로 사용
	var lookupKey int
	if busLocation.NodeOrd > 0 {
		lookupKey = busLocation.NodeOrd
	} else if busLocation.StationSeq > 0 {
		lookupKey = busLocation.StationSeq
	} else {
		busLocation.TotalStations = scs.GetRouteStationCount(routeNmOrId)
		return
	}

	// API2에서 이미 정류소 정보가 있는 경우
	if busLocation.NodeNm != "" && busLocation.NodeId != "" {
		busLocation.TotalStations = scs.GetRouteStationCount(routeNmOrId)
		return
	}

	// NodeOrd/StationSeq로 정류소 정보 조회
	if stationInfo, exists := scs.GetStationInfo(routeNmOrId, lookupKey); exists {
		// 정류소 정보 보강
		busLocation.NodeId = stationInfo.NodeId
		busLocation.NodeNm = stationInfo.NodeNm
		busLocation.NodeNo = stationInfo.NodeNo
		busLocation.NodeOrd = stationInfo.NodeOrd

		// StationId 설정 (NodeId에서 추출하거나 기존 값 유지)
		if stationInfo.StationId > 0 {
			busLocation.StationId = stationInfo.StationId
		}

		// GPS 정보 설정 (API2에서 제공)
		if stationInfo.GPSLat != 0 && stationInfo.GPSLong != 0 {
			busLocation.GpsLati = stationInfo.GPSLat
			busLocation.GpsLong = stationInfo.GPSLong
		}

		busLocation.TotalStations = scs.GetRouteStationCount(routeNmOrId)
	} else {
		// 캐시에서 정류소 정보를 찾지 못한 경우 로깅
		scs.mutex.RLock()
		routeStations, routeExists := scs.stationCache[scs.routeConverter.ToUnifiedKey(routeNmOrId)]
		scs.mutex.RUnlock()

		if !routeExists {
			scs.logger.Infof("캐시 미스 - 차량: %s, 노선: %s 캐시 자체가 없음", busLocation.PlateNo, routeNmOrId)
		} else {
			scs.logger.Infof("캐시 미스 - 차량: %s, 노선: %s, LookupKey: %d (캐시에는 %d개 정류소, 첫 몇개 키: %v)",
				busLocation.PlateNo, routeNmOrId, lookupKey, len(routeStations), scs.getFirstFewKeys(routeStations, 5))
		}
		busLocation.TotalStations = scs.GetRouteStationCount(routeNmOrId)
	}
}

// getFirstFewKeys 캐시의 처음 몇 개 키를 반환 (디버깅용)
func (scs *StationCacheService) getFirstFewKeys(stations map[int]models.StationCache, limit int) []int {
	keys := make([]int, 0, limit)
	count := 0
	for key := range stations {
		if count >= limit {
			break
		}
		keys = append(keys, key)
		count++
	}
	return keys
}

// buildAPI1StationInfoURL API1용 정류소 정보 API URL 생성
func (scs *StationCacheService) buildAPI1StationInfoURL(routeID string) string {
	baseURL := "https://apis.data.go.kr/6410000/busrouteservice/v2/getBusRouteStationListv2"
	params := []string{
		"serviceKey=" + scs.config.ServiceKey,
		"routeId=" + routeID,
		"format=json",
	}
	return baseURL + "?" + utils.JoinStrings(params, "&")
}

// buildAPI2StationInfoURL API2용 정류소 정보 API URL 생성
func (scs *StationCacheService) buildAPI2StationInfoURL(routeID string) string {
	baseURL := "http://apis.data.go.kr/1613000/BusRouteInfoInqireService/getRouteAcctoThrghSttnList"
	params := []string{
		"serviceKey=" + scs.config.ServiceKey,
		"cityCode=" + scs.config.CityCode,
		"routeId=" + routeID,
		"_type=json",
		"numOfRows=200",
	}
	return baseURL + "?" + utils.JoinStrings(params, "&")
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
