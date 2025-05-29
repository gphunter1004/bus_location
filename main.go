package main

import (
	"log"
	"time"

	"bus-tracker/config"
	"bus-tracker/services"
	"bus-tracker/utils"
)

func main() {
	// 설정 로드
	cfg := config.LoadConfig()

	// 로거 초기화
	logger := utils.NewLogger()

	// 설정 정보 로깅
	logger.Infof("API 타입: %s", cfg.APIType)
	logger.Infof("API 베이스 URL: %s", cfg.APIBaseURL)
	if cfg.APIType == "api2" {
		logger.Infof("도시 코드: %s", cfg.CityCode)
	}
	logger.Infof("Service Key: %s***", cfg.ServiceKey[:6]) // 보안을 위해 일부만 표시
	logger.Infof("모니터링할 노선 수: %d개", len(cfg.RouteIDs))
	for i, routeID := range cfg.RouteIDs {
		logger.Infof("  노선 %d: %s", i+1, routeID)
	}
	logger.Infof("호출 간격: %v", cfg.Interval)
	logger.Infof("운영 시간: %02d:%02d ~ %02d:%02d (중단: %02d:%02d ~ %02d:%02d)",
		cfg.OperatingStartHour, cfg.OperatingStartMinute, cfg.OperatingEndHour, cfg.OperatingEndMinute,
		cfg.OperatingEndHour, cfg.OperatingEndMinute, cfg.OperatingStartHour, cfg.OperatingStartMinute)
	logger.Infof("버스 정리 주기: %v", cfg.BusCleanupInterval)
	logger.Infof("버스 미목격 제한 시간: %v", cfg.BusTimeoutDuration)

	// 서비스 초기화 - 팩토리 패턴으로 API 클라이언트 생성
	busTracker := services.NewBusTracker()
	apiClient := services.NewBusAPIClient(cfg, logger) // 인터페이스 사용
	esService := services.NewElasticsearchService(cfg, logger)

	// API 클라이언트 타입 로깅
	logger.Infof("초기화된 API 클라이언트: %s", apiClient.GetAPIType())

	// API 클라이언트별 정류소 정보 캐시 로드
	logger.Infof("%s 사용 - 정류소 정보 캐시 로딩 시작", apiClient.GetAPIType())
	startTime := time.Now()

	if err := apiClient.LoadStationCache(cfg.RouteIDs); err != nil {
		logger.Errorf("정류소 정보 캐시 로드 실패: %v", err)
		logger.Warn("정류소 정보 없이 계속 진행합니다 (API 응답에 정류소 정보가 포함될 수 있음)")
	} else {
		duration := time.Since(startTime)
		logger.Infof("정류소 정보 캐시 로드 완료 - 소요시간: %v", duration)
	}

	// 캐시 통계 출력 (API별로 처리)
	if api1Client, ok := apiClient.(*services.API1Client); ok {
		routeCount, stationCount := api1Client.GetCacheStatistics()
		logger.Infof("API1 캐시 통계 - 노선: %d개, 정류소: %d개", routeCount, stationCount)
	} else if api2Client, ok := apiClient.(*services.API2Client); ok {
		routeCount, stationCount := api2Client.GetCacheStatistics()
		logger.Infof("API2 캐시 통계 - 노선: %d개, 정류소: %d개", routeCount, stationCount)
	}

	// Elasticsearch 연결 테스트
	if err := esService.TestConnection(); err != nil {
		log.Fatalf("Elasticsearch 연결 실패: %v", err)
	}
	logger.Info("Elasticsearch 연결 성공")

	// 타이머 생성
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	// 버스 정리 타이머 생성 (환경변수 기반)
	cleanupTicker := time.NewTicker(cfg.BusCleanupInterval)
	defer cleanupTicker.Stop()

	logger.Info("버스 정보 API 모니터링 시작")
	if cfg.APIType == "api2" {
		logger.Info("API2 모드 - 공공데이터포털 버스위치정보")
	} else {
		logger.Info("API1 모드 - 경기도 버스위치정보 v2")
	}
	logger.Info("정류장 변경된 버스만 Elasticsearch로 전송됩니다")

	// 첫 번째 호출 (운영 시간 체크 후)
	currentTime := time.Now()
	logger.Infof("현재 시간: %s", currentTime.Format("2006-01-02 15:04:05"))
	logger.Infof("운영 시간 체크 결과: %t", cfg.IsOperatingTime(currentTime))

	if cfg.IsOperatingTime(currentTime) {
		logger.Info("운영 시간 중 - API 호출 시작")
		processAPICall(apiClient, esService, busTracker, logger, cfg)
	} else {
		nextOperatingTime := cfg.GetNextOperatingTime(currentTime)
		logger.Infof("현재 운행 중단 시간입니다. 다음 운행 시작: %s",
			nextOperatingTime.Format("2006-01-02 15:04:05"))
	}

	// 타이머로 반복 실행
	for {
		select {
		case <-ticker.C:
			currentTime := time.Now()
			logger.Infof("시간 체크: %s, 운영 중: %t",
				currentTime.Format("15:04:05"), cfg.IsOperatingTime(currentTime))

			if cfg.IsOperatingTime(currentTime) {
				processAPICall(apiClient, esService, busTracker, logger, cfg)
			} else {
				nextOperatingTime := cfg.GetNextOperatingTime(currentTime)
				logger.Infof("운행 중단 시간. 다음 운행 시작: %s",
					nextOperatingTime.Format("2006-01-02 15:04:05"))
			}

		case <-cleanupTicker.C:
			// 구간 운행 버스 정리 (환경변수 기반 시간)
			removedCount := busTracker.CleanupMissingBuses(cfg.BusTimeoutDuration, logger)
			if removedCount > 0 {
				logger.Infof("정리 완료 - 현재 추적 중인 버스: %d대", busTracker.GetTrackedBusCount())
			}
		}
	}
}

func processAPICall(apiClient services.BusAPIClient, esService *services.ElasticsearchService,
	busTracker *services.BusTracker, logger *utils.Logger, cfg *config.Config) {

	logger.Infof("=== 버스 위치 API 호출 시작 (%s, %s) ===",
		time.Now().Format("15:04:05"), apiClient.GetAPIType())

	// 모든 노선에 대해 API 호출
	allBusLocations, err := apiClient.FetchAllBusLocations(cfg.RouteIDs)
	if err != nil {
		logger.Errorf("API 호출 오류: %v", err)
		return
	}

	// 전체 버스 위치 정보가 비어있는지 확인
	if len(allBusLocations) == 0 {
		logger.Warn("모든 노선에서 버스 위치 정보가 없습니다")
		return
	}

	logger.Infof("전체 수신된 버스 위치 정보: %d대", len(allBusLocations))
	logger.Infof("현재 추적 중인 버스: %d대", busTracker.GetTrackedBusCount())

	// 노선별 통계 출력
	routeStats := make(map[int64]int)
	for _, bus := range allBusLocations {
		routeStats[bus.RouteId]++
	}

	for routeId, count := range routeStats {
		logger.Infof("노선 %d: %d대", routeId, count)
	}

	// 정류장이 변경된 버스만 필터링
	changedBuses := busTracker.FilterChangedStations(allBusLocations, logger)

	// 변경된 버스가 없으면 Elasticsearch 전송 생략
	if len(changedBuses) == 0 {
		logger.Info("정류장 변경된 버스가 없어 전송을 생략합니다")
		logger.Info("=== 처리 완료 ===")
		return
	}

	// 변경된 버스 정보만 벌크로 Elasticsearch에 전송
	startTime := time.Now()
	if err := esService.BulkSendBusLocations(cfg.IndexName, changedBuses); err != nil {
		logger.Errorf("벌크 전송 중 오류 발생: %v", err)
		return
	}

	duration := time.Since(startTime)
	logger.Infof("벌크 전송 완료 - 처리 시간: %v", duration)
	logger.Info("=== 처리 완료 ===")
}
