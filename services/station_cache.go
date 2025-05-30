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

// StationCacheService 정류소 정보 캐시 서비스 (통합 키 기반)
type StationCacheService struct {
	config         *config.Config
	logger         *utils.Logger
	client         *http.Client
	stationCache   map[string]map[int]models.StationCache // unifiedKey -> stationSeq -> StationCache
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

// LoadStationCache 모든 노선의 정류소 정보를 미리 로드
// services/station_cache.go의 LoadStationCache 메서드 수정

// LoadStationCache 모든 노선의 정류소 정보를 미리 로드 (중복 제거)
func (scs *StationCacheService) LoadStationCache(routeIDs []string) error {
	scs.logger.Infof("🚀 정류소 정보 캐시 로딩 시작 - 총 %d개 노선 (%s)", len(routeIDs), scs.apiType)
	scs.logger.Infof("📋 입력된 Route IDs: %v", routeIDs)

	// 🔧 중복 제거: 통합 키 기준으로 고유한 노선만 처리
	uniqueRoutes := make(map[string]string) // unifiedKey -> originalRouteID
	for i, routeID := range routeIDs {
		if err := scs.routeConverter.ValidateRouteID(routeID, scs.apiType); err != nil {
			scs.logger.Errorf("❌ Route ID 형식 오류 [%d]: %v", i+1, err)
			return err
		}

		_, unifiedKey := scs.routeConverter.GetOriginalAndUnified(routeID)

		// 이미 같은 통합 키가 있는지 확인
		if existingRoute, exists := uniqueRoutes[unifiedKey]; exists {
			scs.logger.Infof("🔄 Route ID 중복 제거 [%d]: '%s' → 통합키: '%s' (이미 '%s'로 처리됨)",
				i+1, routeID, unifiedKey, existingRoute)
			continue
		}

		uniqueRoutes[unifiedKey] = routeID
		scs.logger.Infof("🔄 Route ID 변환 [%d]: '%s' → 통합키: '%s'", i+1, routeID, unifiedKey)
	}

	// 고유한 노선들로만 처리
	var wg sync.WaitGroup
	errorChan := make(chan error, len(uniqueRoutes))
	successCount := 0
	var successMutex sync.Mutex

	i := 0
	for unifiedKey, routeID := range uniqueRoutes {
		i++
		wg.Add(1)
		go func(index int, key, id string) {
			defer wg.Done()

			scs.logger.Infof("🔄 [%d/%d] 노선 %s 캐시 로딩 시작", index, len(uniqueRoutes), id)

			// 🔧 이미 캐시가 있는지 확인
			scs.mutex.RLock()
			_, cacheExists := scs.stationCache[key]
			scs.mutex.RUnlock()

			if cacheExists {
				scs.logger.Infof("✅ [%d/%d] 노선 %s 캐시 이미 존재함 (통합키: %s)",
					index, len(uniqueRoutes), id, key)
				successMutex.Lock()
				successCount++
				successMutex.Unlock()
				return
			}

			// 캐시 로딩 시도 (최대 3회 재시도)
			var err error
			for attempt := 1; attempt <= 3; attempt++ {
				if err = scs.loadStationCacheForRoute(id); err == nil {
					successMutex.Lock()
					successCount++
					successMutex.Unlock()
					scs.logger.Infof("✅ [%d/%d] 노선 %s 캐시 로딩 성공", index, len(uniqueRoutes), id)
					return
				}
				scs.logger.Warnf("⚠️ [%d/%d] 노선 %s 캐시 로딩 실패 (시도 %d/3): %v",
					index, len(uniqueRoutes), id, attempt, err)
				if attempt < 3 {
					time.Sleep(time.Duration(attempt) * time.Second)
				}
			}
			errorChan <- fmt.Errorf("노선 %s 정류소 정보 로드 최종 실패: %v", id, err)
		}(i, unifiedKey, routeID)
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

	scs.logger.Infof("📊 정류소 정보 캐시 로딩 완료 - 성공: %d개, 실패: %d개", successCount, len(errors))

	// 전체 캐시 통계 출력
	scs.printCacheStatistics()

	// 일부라도 성공했다면 계속 진행
	if successCount > 0 {
		if len(errors) > 0 {
			scs.logger.Warnf("⚠️ 일부 노선의 정류소 정보가 로드되지 않았습니다. 캐시 없는 노선은 기본 정보만 제공됩니다.")
		}
		return nil
	}

	if len(errors) > 0 {
		return fmt.Errorf("모든 노선의 정류소 정보 로드가 실패했습니다. 첫 번째 오류: %v", errors[0])
	}

	return nil
}

// loadStationCacheForRoute 특정 노선의 정류소 정보 로드
func (scs *StationCacheService) loadStationCacheForRoute(routeID string) error {
	scs.logger.Infof("🔄 노선 %s 정류소 캐시 로딩 시작 (%s)", routeID, scs.apiType)

	// API 타입에 따라 다른 처리
	if scs.apiType == "api1" {
		return scs.loadAPI1StationCache(routeID)
	} else {
		return scs.loadAPI2StationCache(routeID)
	}
}

// loadAPI1StationCache API1용 정류소 정보 로드
func (scs *StationCacheService) loadAPI1StationCache(routeID string) error {
	// API1 형식으로 변환
	api1RouteID := scs.routeConverter.ToAPI1Format(routeID)
	unifiedKey := scs.routeConverter.ToUnifiedKey(routeID)

	scs.logger.Infof("🔄 API1 정류소 로딩 - 원본: %s, API1형식: %s, 통합키: %s", routeID, api1RouteID, unifiedKey)

	// API URL 생성
	apiURL := scs.buildAPI1StationInfoURL(api1RouteID)

	scs.logger.Infof("📡 API1 정류소 정보 API 호출")
	scs.logger.Infof("🔗 요청 URL: %s", maskSensitiveURL(apiURL, scs.config.ServiceKey))

	// API 호출
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

	// 응답 내용 디버깅
	responsePreview := string(body)
	if len(responsePreview) > 500 {
		responsePreview = responsePreview[:500] + "..."
	}
	scs.logger.Infof("📄 API1 정류소 정보 API 응답: %s", responsePreview)

	// JSON 파싱
	var stationResp models.API1StationInfoResponse
	if err := json.Unmarshal(body, &stationResp); err != nil {
		scs.logger.Errorf("❌ API1 정류소 정보 JSON 파싱 실패: %v", err)
		scs.logger.Errorf("파싱 실패한 원본 응답: %s", string(body))
		return fmt.Errorf("JSON 파싱 실패: %v", err)
	}

	// API 응답 성공 여부 확인
	if !stationResp.IsSuccess() {
		scs.logger.Errorf("❌ API1 정류소 정보 API 오류: 코드=%d, 메시지=%s",
			stationResp.Response.MsgHeader.ResultCode, stationResp.GetErrorMessage())
		return fmt.Errorf("API 오류 (코드: %d): %s", stationResp.Response.MsgHeader.ResultCode, stationResp.GetErrorMessage())
	}

	stations := stationResp.GetStationInfoList()
	scs.logger.Infof("📊 API1 노선 %s: %d개 정류소 정보 수신", api1RouteID, len(stations))

	if len(stations) == 0 {
		scs.logger.Warnf("⚠️ API1 노선 %s: 정류소 정보가 비어있습니다", api1RouteID)
		return fmt.Errorf("노선 %s에 대한 정류소 정보가 없습니다", api1RouteID)
	}

	// 통합 키로 캐시에 저장
	scs.mutex.Lock()
	defer scs.mutex.Unlock()

	if scs.stationCache[unifiedKey] == nil {
		scs.stationCache[unifiedKey] = make(map[int]models.StationCache)
	}

	for _, station := range stations {
		stationCache := station.ToStationCache()
		scs.stationCache[unifiedKey][station.StationSeq] = stationCache

		// 샘플 로깅
		if station.StationSeq <= 3 || station.StationSeq == len(stations) {
			scs.logger.Infof("  📍 [API1] 정류소 %d: %s (ID: %d) 좌표:(%.6f, %.6f)",
				station.StationSeq, station.StationName, station.StationId, station.Y, station.X)
		}
	}

	scs.logger.Infof("✅ API1 노선 정류소 캐시 저장 완료 - 통합키: %s, %d개 정류소", unifiedKey, len(stations))
	return nil
}

// loadAPI2StationCache API2용 정류소 정보 로드
func (scs *StationCacheService) loadAPI2StationCache(routeID string) error {
	// API2 형식으로 변환
	api2RouteID := scs.routeConverter.ToAPI2Format(routeID)
	unifiedKey := scs.routeConverter.ToUnifiedKey(routeID)

	scs.logger.Infof("🔄 API2 정류소 로딩 - 원본: %s, API2형식: %s, 통합키: %s", routeID, api2RouteID, unifiedKey)

	// API URL 생성
	apiURL := scs.buildAPI2StationInfoURL(api2RouteID)

	scs.logger.Infof("📡 API2 정류소 정보 API 호출")
	scs.logger.Infof("🔗 요청 URL: %s", maskSensitiveURL(apiURL, scs.config.ServiceKey))

	// API 호출
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

	// 응답 내용 디버깅
	responsePreview := string(body)
	if len(responsePreview) > 500 {
		responsePreview = responsePreview[:500] + "..."
	}
	scs.logger.Infof("📄 API2 정류소 정보 API 응답: %s", responsePreview)

	// JSON 파싱
	var stationResp models.StationInfoResponse
	if err := json.Unmarshal(body, &stationResp); err != nil {
		scs.logger.Errorf("❌ API2 정류소 정보 JSON 파싱 실패: %v", err)
		scs.logger.Errorf("파싱 실패한 원본 응답: %s", string(body))
		return fmt.Errorf("JSON 파싱 실패: %v", err)
	}

	// API 응답 성공 여부 확인
	if !stationResp.IsSuccess() {
		scs.logger.Errorf("❌ API2 정류소 정보 API 오류: 코드=%s, 메시지=%s",
			stationResp.Response.Header.ResultCode, stationResp.GetErrorMessage())
		return fmt.Errorf("API 오류 (코드: %s): %s", stationResp.Response.Header.ResultCode, stationResp.GetErrorMessage())
	}

	stations := stationResp.GetStationInfoList()
	scs.logger.Infof("📊 API2 노선 %s: %d개 정류소 정보 수신", api2RouteID, len(stations))

	if len(stations) == 0 {
		scs.logger.Warnf("⚠️ API2 노선 %s: 정류소 정보가 비어있습니다", api2RouteID)
		return fmt.Errorf("노선 %s에 대한 정류소 정보가 없습니다", api2RouteID)
	}

	// 통합 키로 캐시에 저장
	scs.mutex.Lock()
	defer scs.mutex.Unlock()

	if scs.stationCache[unifiedKey] == nil {
		scs.stationCache[unifiedKey] = make(map[int]models.StationCache)
	}

	for _, station := range stations {
		stationCache := station.ToStationCache()
		scs.stationCache[unifiedKey][station.NodeOrd] = stationCache

		// 샘플 로깅
		if station.NodeOrd <= 3 || station.NodeOrd == len(stations) {
			scs.logger.Infof("  📍 [API2] 정류소 %d: %s (%s) GPS:(%.6f, %.6f)",
				station.NodeOrd, station.NodeNm, station.NodeId, station.GPSLat, station.GPSLong)
		}
	}

	scs.logger.Infof("✅ API2 노선 정류소 캐시 저장 완료 - 통합키: %s, %d개 정류소", unifiedKey, len(stations))
	return nil
}

// GetStationInfo 통합 키로 정류소 정보 조회
func (scs *StationCacheService) GetStationInfo(routeID string, stationSeq int) (models.StationCache, bool) {
	unifiedKey := scs.routeConverter.ToUnifiedKey(routeID)

	scs.mutex.RLock()
	defer scs.mutex.RUnlock()

	routeStations, routeExists := scs.stationCache[unifiedKey]
	if !routeExists {
		return models.StationCache{}, false
	}

	station, stationExists := routeStations[stationSeq]
	return station, stationExists
}

// GetRouteStationCount 특정 노선의 전체 정류소 개수 반환
func (scs *StationCacheService) GetRouteStationCount(routeID string) int {
	unifiedKey := scs.routeConverter.ToUnifiedKey(routeID)

	scs.mutex.RLock()
	defer scs.mutex.RUnlock()

	if stations, exists := scs.stationCache[unifiedKey]; exists {
		return len(stations)
	}
	return 0
}

// EnrichBusLocationWithStationInfo 버스 위치 정보에 정류소 정보 보강
// services/station_cache.go의 EnrichBusLocationWithStationInfo 메서드 수정

// EnrichBusLocationWithStationInfo 버스 위치 정보에 정류소 정보 보강
// services/station_cache.go의 EnrichBusLocationWithStationInfo 메서드 수정

// EnrichBusLocationWithStationInfo 버스 위치 정보에 정류소 정보 보강
func (scs *StationCacheService) EnrichBusLocationWithStationInfo(busLocation *models.BusLocation, routeID string) {
	var lookupKey int
	unifiedRouteKey := scs.routeConverter.ToUnifiedKey(routeID)

	// API 타입에 따라 다른 키 사용
	if scs.apiType == "api1" {
		lookupKey = busLocation.StationSeq
		if lookupKey <= 0 {
			scs.logger.Warnf("⚠️ API1 정류소 순서가 유효하지 않음 - 노선: %s, StationSeq: %d, 차량번호: %s",
				routeID, lookupKey, busLocation.PlateNo)
			// 전체 정류소 개수만이라도 설정
			busLocation.TotalStations = scs.GetRouteStationCount(routeID)
			return
		}
		scs.logger.Infof("🔍 API1 정류소 캐시 조회 - 원본노선: '%s', 통합키: '%s', StationSeq: %d", routeID, unifiedRouteKey, lookupKey)
	} else {
		lookupKey = busLocation.NodeOrd

		// 🔧 NodeOrd가 0인 경우 특별 처리
		if lookupKey <= 0 {
			// StationSeq를 대신 사용 시도
			if busLocation.StationSeq > 0 {
				lookupKey = busLocation.StationSeq
				scs.logger.Infof("⚡ API2 NodeOrd=0 대체 - 차량번호: %s, StationSeq: %d 사용",
					busLocation.PlateNo, lookupKey)
			} else {
				scs.logger.Warnf("⚠️ API2 정류소 순서가 유효하지 않음 - 노선: %s, NodeOrd: %d, StationSeq: %d, 차량번호: %s",
					routeID, busLocation.NodeOrd, busLocation.StationSeq, busLocation.PlateNo)
				// 전체 정류소 개수만이라도 설정
				busLocation.TotalStations = scs.GetRouteStationCount(routeID)
				return
			}
		}

		// API2에서 이미 정류소 정보가 있는지 확인
		if busLocation.NodeNm != "" && busLocation.NodeId != "" {
			busLocation.TotalStations = scs.GetRouteStationCount(routeID)

			// 🔧 NodeOrd 보정 (lookupKey가 대체값인 경우)
			if busLocation.NodeOrd <= 0 {
				busLocation.NodeOrd = lookupKey
				scs.logger.Infof("🔧 NodeOrd 보정 완료 - 차량번호: %s, NodeOrd: %d",
					busLocation.PlateNo, lookupKey)
			}

			scs.logger.Infof("ℹ️ 정류소 정보 이미 존재 - 통합키: %s, 순서: %d/%d → %s (%s) [캐시 불필요]",
				unifiedRouteKey, lookupKey, busLocation.TotalStations, busLocation.NodeNm, busLocation.NodeId)
			return
		}
		scs.logger.Infof("🔍 API2 정류소 캐시 조회 - 원본노선: '%s', 통합키: '%s', NodeOrd: %d", routeID, unifiedRouteKey, lookupKey)
	}

	// 통합 키로 정류소 정보 조회
	if stationInfo, exists := scs.GetStationInfo(routeID, lookupKey); exists {
		// 정류소 정보 보강
		busLocation.NodeId = stationInfo.NodeId
		busLocation.NodeNm = stationInfo.NodeNm
		busLocation.NodeNo = stationInfo.NodeNo

		// API별 순서 필드 설정
		if scs.apiType == "api1" {
			busLocation.NodeOrd = stationInfo.NodeOrd
		} else {
			// 🔧 API2에서 NodeOrd가 0이었다면 보정
			if busLocation.NodeOrd <= 0 {
				busLocation.NodeOrd = lookupKey
			}
		}

		// StationId 설정
		if scs.apiType == "api1" {
			if stationIdInt, err := strconv.ParseInt(stationInfo.NodeId, 10, 64); err == nil {
				busLocation.StationId = stationIdInt
			}
		} else {
			if stationInfo.NodeId != "" && len(stationInfo.NodeId) > 3 {
				if stationIdInt, err := strconv.ParseInt(stationInfo.NodeId[3:], 10, 64); err == nil {
					busLocation.StationId = stationIdInt
				}
			}
		}

		// GPS 정보 설정
		if stationInfo.GPSLat != 0 && stationInfo.GPSLong != 0 {
			busLocation.GpsLati = stationInfo.GPSLat
			busLocation.GpsLong = stationInfo.GPSLong
		}

		// 전체 정류소 개수 설정 (중요!)
		busLocation.TotalStations = scs.GetRouteStationCount(routeID)

		scs.logger.Infof("✅ 정류소 정보 보강 완료 - 통합키: %s, 순서: %d/%d → %s (%s) [캐시 사용]",
			unifiedRouteKey, lookupKey, busLocation.TotalStations, stationInfo.NodeNm, stationInfo.NodeId)
	} else {
		// 전체 정류소 개수만이라도 설정
		busLocation.TotalStations = scs.GetRouteStationCount(routeID)

		scs.logger.Errorf("❌ 정류소 정보 없음 - 통합키: %s, 순서: %d (캐시에서 찾을 수 없음), 전체: %d개",
			unifiedRouteKey, lookupKey, busLocation.TotalStations)

		// 캐시 상태 확인 (디버깅용)
		scs.logCacheStatus(unifiedRouteKey)
	}
}

// logCacheStatus 캐시 상태 로깅
func (scs *StationCacheService) logCacheStatus(unifiedRouteKey string) {
	scs.mutex.RLock()
	defer scs.mutex.RUnlock()

	if routeStations, exists := scs.stationCache[unifiedRouteKey]; exists {
		scs.logger.Infof("📊 캐시 상태 - 통합키 '%s': %d개 정류소 캐시됨", unifiedRouteKey, len(routeStations))

		var minSeq, maxSeq int = 999, 0
		for seq := range routeStations {
			if seq < minSeq {
				minSeq = seq
			}
			if seq > maxSeq {
				maxSeq = seq
			}
		}
		scs.logger.Infof("   🔢 순서 범위: %d ~ %d", minSeq, maxSeq)
	} else {
		scs.logger.Errorf("📊 캐시 상태 - 통합키 '%s': ❌ 캐시 없음!", unifiedRouteKey)

		var cachedRoutes []string
		for cachedRouteKey := range scs.stationCache {
			cachedRoutes = append(cachedRoutes, cachedRouteKey)
		}
		scs.logger.Errorf("   현재 캐시된 통합키들: %v", cachedRoutes)
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
		"numOfRows=200",
	}
	return baseURL + "?" + joinStrings(params, "&")
}

// printCacheStatistics 캐시 통계 출력
func (scs *StationCacheService) printCacheStatistics() {
	scs.mutex.RLock()
	defer scs.mutex.RUnlock()

	totalStations := 0
	scs.logger.Infof("=== %s 정류소 캐시 통계 (통합 키 기반) ===", scs.apiType)

	for unifiedKey, stations := range scs.stationCache {
		stationCount := len(stations)
		totalStations += stationCount

		var minSeq, maxSeq int = 999, 0
		for seq := range stations {
			if seq < minSeq {
				minSeq = seq
			}
			if seq > maxSeq {
				maxSeq = seq
			}
		}

		scs.logger.Infof("통합키 %s: %d개 정류소 (순서: %d~%d)", unifiedKey, stationCount, minSeq, maxSeq)
	}

	scs.logger.Infof("총 %d개 통합키, %d개 정류소 정보 캐시됨", len(scs.stationCache), totalStations)
	scs.logger.Info("===============================")
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
func (scs *StationCacheService) debugRouteStations(unifiedRouteKey string) {
	scs.mutex.RLock()
	defer scs.mutex.RUnlock()

	if stations, exists := scs.stationCache[unifiedRouteKey]; exists {
		scs.logger.Infof("🔍 통합키 %s의 캐시된 정류소 순서들 (총 %d개):", unifiedRouteKey, len(stations))

		// 순서대로 정렬해서 출력 (처음 15개만)
		count := 0
		for seq := 1; seq <= 100 && count < 15; seq++ {
			if station, exists := stations[seq]; exists {
				scs.logger.Infof("  📍 순서 %d: %s (%s)", seq, station.NodeNm, station.NodeId)
				count++
			}
		}

		if len(stations) > 15 {
			scs.logger.Infof("  ... 총 %d개 정류소 중 처음 %d개만 표시", len(stations), count)
		}
	} else {
		scs.logger.Errorf("❌ 통합키 %s의 캐시 정보가 없습니다", unifiedRouteKey)
	}
}
