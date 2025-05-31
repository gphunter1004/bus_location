// internal/web/models/responses/status.go
package responses

import "time"

// StatusResponse 시스템 상태 응답
type StatusResponse struct {
	BaseResponse
	Data StatusData `json:"data"`
}

// StatusData 상태 데이터
type StatusData struct {
	Status              string        `json:"status"`
	Uptime              string        `json:"uptime"`
	Version             string        `json:"version"`
	Mode                string        `json:"mode"`
	OperatingTime       bool          `json:"operatingTime"`
	OrchestratorRunning bool          `json:"orchestratorRunning"`
	Config              ConfigInfo    `json:"config"`
	OperatingInfo       OperatingInfo `json:"operatingInfo"`
}

// ConfigInfo 설정 정보
type ConfigInfo struct {
	API1Routes              int    `json:"api1Routes"`
	API2Routes              int    `json:"api2Routes"`
	BusDisappearanceTimeout string `json:"busDisappearanceTimeout"`
	EnableTerminalStop      bool   `json:"enableTerminalStop"`
	OperatingSchedule       string `json:"operatingSchedule"`
}

// OperatingInfo 운영 정보
type OperatingInfo struct {
	CurrentDate       string     `json:"currentDate"`
	LastCounterReset  time.Time  `json:"lastCounterReset"`
	NextOperatingTime *time.Time `json:"nextOperatingTime,omitempty"`
}

// StatisticsResponse 통계 응답
type StatisticsResponse struct {
	BaseResponse
	Data StatisticsData `json:"data"`
}

// StatisticsData 통계 데이터
type StatisticsData struct {
	BusStatistics   BusStatistics   `json:"busStatistics"`
	CacheStatistics CacheStatistics `json:"cacheStatistics"`
	SystemMetrics   SystemMetrics   `json:"systemMetrics"`
}

// BusStatistics 버스 통계
type BusStatistics struct {
	TotalBuses   int `json:"totalBuses"`
	API1Only     int `json:"api1Only"`
	API2Only     int `json:"api2Only"`
	Both         int `json:"both"`
	TrackedBuses int `json:"trackedBuses"`
}

// CacheStatistics 캐시 통계
type CacheStatistics struct {
	Routes   int `json:"routes"`
	Stations int `json:"stations"`
}

// SystemMetrics 시스템 메트릭
type SystemMetrics struct {
	MemoryUsage string `json:"memoryUsage,omitempty"`
	CPUUsage    string `json:"cpuUsage,omitempty"`
	Uptime      string `json:"uptime"`
}

// HealthCheckResponse 헬스체크 응답
type HealthCheckResponse struct {
	BaseResponse
	Data HealthCheckData `json:"data"`
}

// HealthCheckData 헬스체크 데이터
type HealthCheckData struct {
	Status        string            `json:"status"`
	Checks        map[string]string `json:"checks"`
	Statistics    BusStatistics     `json:"statistics"`
	Configuration struct {
		API1Routes int `json:"api1Routes"`
		API2Routes int `json:"api2Routes"`
	} `json:"configuration"`
}
