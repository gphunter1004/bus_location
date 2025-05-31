// internal/web/models/responses/tracking.go
package responses

import "time"

// TrackedBusesResponse 추적 버스 목록 응답
type TrackedBusesResponse struct {
	BaseResponse
	Data TrackedBusesData `json:"data"`
}

// TrackedBusesData 추적 버스 데이터
type TrackedBusesData struct {
	TotalTracked  int               `json:"totalTracked"`
	OperatingDate string            `json:"operatingDate"`
	DailySummary  DailySummary      `json:"dailySummary"`
	Buses         []BusTrackingInfo `json:"buses,omitempty"`
}

// DailySummary 일일 요약 정보
type DailySummary struct {
	ActiveBusesWithTrips int     `json:"activeBusesWithTrips"`
	TotalDailyTrips      int     `json:"totalDailyTrips"`
	MaxDailyTrips        int     `json:"maxDailyTrips"`
	AverageDailyTrips    float64 `json:"averageDailyTrips"`
}

// BusTrackingInfo 버스 추적 정보
type BusTrackingInfo struct {
	PlateNo          string    `json:"plateNo"`
	RouteNm          string    `json:"routeNm"`
	LastPosition     int64     `json:"lastPosition"`
	PreviousPosition int64     `json:"previousPosition"`
	LastSeenTime     time.Time `json:"lastSeenTime"`
	TripNumber       int       `json:"tripNumber"`
	IsTerminated     bool      `json:"isTerminated"`
	TotalStations    int       `json:"totalStations"`
	TripStartTime    time.Time `json:"tripStartTime,omitempty"`
}

// BusInfoResponse 특정 버스 정보 응답
type BusInfoResponse struct {
	BaseResponse
	Data BusInfoData `json:"data"`
}

// BusInfoData 버스 정보 데이터
type BusInfoData struct {
	BusInfo        BusTrackingInfo `json:"busInfo"`
	OperatingDate  string          `json:"operatingDate"`
	TripStartTime  time.Time       `json:"tripStartTime"`
	TripDuration   string          `json:"tripDuration"`
	DailyTripCount int             `json:"dailyTripCount"`
	RouteInfo      RouteInfo       `json:"routeInfo,omitempty"`
}

// RouteInfo 노선 정보
type RouteInfo struct {
	RouteID       string `json:"routeId"`
	RouteName     string `json:"routeName"`
	TotalStations int    `json:"totalStations"`
	RouteType     string `json:"routeType"`
}

// TripStatisticsResponse 운행 차수 통계 응답
type TripStatisticsResponse struct {
	BaseResponse
	Data TripStatisticsData `json:"data"`
}

// TripStatisticsData 운행 차수 통계 데이터
type TripStatisticsData struct {
	OperatingDate string                `json:"operatingDate"`
	LastResetTime time.Time             `json:"lastResetTime"`
	Summary       TripStatisticsSummary `json:"summary"`
	Statistics    map[string]int        `json:"statistics"`
	TopPerformers []VehiclePerformance  `json:"topPerformers,omitempty"`
}

// TripStatisticsSummary 운행 차수 통계 요약
type TripStatisticsSummary struct {
	ActiveVehicles int     `json:"activeVehicles"`
	TotalTrips     int     `json:"totalTrips"`
	MaxTrips       int     `json:"maxTrips"`
	AverageTrips   float64 `json:"averageTrips"`
	MinTrips       int     `json:"minTrips"`
}

// VehiclePerformance 차량 성과 정보
type VehiclePerformance struct {
	PlateNo    string  `json:"plateNo"`
	RouteNm    string  `json:"routeNm"`
	TripCount  int     `json:"tripCount"`
	Efficiency float64 `json:"efficiency,omitempty"`
}

// TripCounterResetResponse 운행 차수 리셋 응답
type TripCounterResetResponse struct {
	BaseResponse
	Data TripCounterResetData `json:"data"`
}

// TripCounterResetData 운행 차수 리셋 데이터
type TripCounterResetData struct {
	OldDate            string         `json:"oldDate"`
	NewDate            string         `json:"newDate"`
	ResetTime          time.Time      `json:"resetTime"`
	AffectedVehicles   int            `json:"affectedVehicles"`
	PreviousStatistics map[string]int `json:"previousStatistics,omitempty"`
}
