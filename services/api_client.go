package services

import (
	"bus-tracker/config"
	"bus-tracker/models"
	"bus-tracker/utils"
)

// BusAPIClient 버스 API 클라이언트 공통 인터페이스
type BusAPIClient interface {
	// FetchBusLocationByRoute 특정 routeId로 버스 데이터를 가져옴
	FetchBusLocationByRoute(routeID string) ([]models.BusLocation, error)

	// FetchAllBusLocations 모든 routeId에 대해 버스 데이터를 병렬로 가져옴
	FetchAllBusLocations(routeIDs []string) ([]models.BusLocation, error)

	// GetAPIType API 타입 반환
	GetAPIType() string

	// LoadStationCache 정류소 정보 캐시 로드 (API2 전용, API1은 빈 구현)
	LoadStationCache(routeIDs []string) error
}

// NewBusAPIClient API 타입에 따라 적절한 클라이언트 생성 (팩토리 패턴)
func NewBusAPIClient(cfg *config.Config, logger *utils.Logger) BusAPIClient {
	switch cfg.APIType {
	case "api2":
		return NewAPI2Client(cfg, logger)
	default:
		return NewAPI1Client(cfg, logger)
	}
}

// APIClientBase 공통 베이스 구조체 (공통 필드와 메서드)
type APIClientBase struct {
	config *config.Config
	logger *utils.Logger
}
