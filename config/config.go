package config

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

// APIConfig 개별 API 설정
type APIConfig struct {
	Enabled  bool          `json:"enabled"`  // API 활성화 여부
	Interval time.Duration `json:"interval"` // 호출 주기
	BaseURL  string        `json:"baseURL"`  // API URL
	RouteIDs []string      `json:"routeIDs"` // 대상 노선
	Priority int           `json:"priority"` // 우선순위 (낮을수록 우선)
}

// Config 애플리케이션 설정 구조체 (모든 모드 지원)
type Config struct {
	// 모드 설정
	Mode string `json:"mode"` // "api1", "api2", "unified" 중 선택

	// 기본 인증 정보
	ServiceKey string
	CityCode   string

	// Elasticsearch 설정
	ElasticsearchURL      string
	ElasticsearchUsername string
	ElasticsearchPassword string
	IndexName             string

	// 레거시 모드용 설정 (하위 호환성)
	APIBaseURL string
	RouteIDs   []string
	Interval   time.Duration
	APIType    string // 하위 호환성을 위해 유지

	// 운영 시간 설정
	OperatingStartHour   int // 운영 시작 시간 (24시간 형식)
	OperatingStartMinute int // 운영 시작 분
	OperatingEndHour     int // 운영 종료 시간 (24시간 형식)
	OperatingEndMinute   int // 운영 종료 분

	// 버스 트래킹 설정
	BusCleanupInterval time.Duration // 버스 정리 작업 주기 (기본: 5분)
	BusTimeoutDuration time.Duration // 버스 미목격 제한 시간 (기본: 60분)

	// 통합 처리 설정
	DataMergeInterval   time.Duration `json:"dataMergeInterval"`   // 데이터 통합 주기 (기본: 10초)
	DataRetentionPeriod time.Duration `json:"dataRetentionPeriod"` // 메모리 데이터 보존 기간 (기본: 5분)

	// API별 설정
	API1Config APIConfig `json:"api1Config"`
	API2Config APIConfig `json:"api2Config"`
}

// LoadConfig 환경변수 또는 기본값으로 설정을 로드
func LoadConfig() *Config {
	// .env 파일 로드 시도 (선택사항)
	if err := godotenv.Load(); err != nil {
		log.Println(".env 파일을 찾을 수 없습니다. 시스템 환경변수를 사용합니다.")
	} else {
		log.Println(".env 파일을 성공적으로 로드했습니다.")
	}

	// MODE 환경변수로 동작 모드 결정 (기본값: api1)
	mode := getEnv("MODE", "api1")

	// 하위 호환성: API_TYPE이 설정되어 있으면 MODE보다 우선
	if apiType := getEnv("API_TYPE", ""); apiType != "" {
		mode = apiType
	}

	cfg := &Config{
		// 모드 설정
		Mode: mode,

		// 기본 인증 정보
		ServiceKey: getEnv("SERVICE_KEY", ""),
		CityCode:   getEnv("CITY_CODE", "31240"),

		// Elasticsearch 설정
		ElasticsearchURL:      getEnv("ELASTICSEARCH_URL", "http://localhost:9200"),
		ElasticsearchUsername: getEnv("ELASTICSEARCH_USERNAME", ""),
		ElasticsearchPassword: getEnv("ELASTICSEARCH_PASSWORD", ""),
		IndexName:             getEnv("INDEX_NAME", "bus-locations"),

		// 하위 호환성을 위한 레거시 설정
		APIType:    mode, // Mode와 동일하게 설정
		APIBaseURL: getEnv("API_BASE_URL", ""),
		RouteIDs:   getRouteIDList("ROUTE_IDS"),
		Interval:   getDuration("INTERVAL_SECONDS", 30),

		// 운영 시간 기본값: 04:55 ~ 01:00
		OperatingStartHour:   getIntEnv("OPERATING_START_HOUR", 4),
		OperatingStartMinute: getIntEnv("OPERATING_START_MINUTE", 55),
		OperatingEndHour:     getIntEnv("OPERATING_END_HOUR", 1),
		OperatingEndMinute:   getIntEnv("OPERATING_END_MINUTE", 0),

		// 버스 트래킹 설정 (🔧 수정된 부분)
		BusCleanupInterval: getDurationMinutes("BUS_CLEANUP_INTERVAL_MINUTES", 5), // 5분
		BusTimeoutDuration: getDurationMinutes("BUS_TIMEOUT_MINUTES", 60),         // 60분 (1시간)

		// 통합 처리 설정 (🔧 수정된 부분)
		DataMergeInterval:   getDuration("DATA_MERGE_INTERVAL_SECONDS", 10),  // 10초
		DataRetentionPeriod: getDurationMinutes("DATA_RETENTION_MINUTES", 5), // 5분

		// API1 설정 (경기도 버스위치정보 v2)
		API1Config: APIConfig{
			Enabled:  getBoolEnv("API1_ENABLED", mode == "api1" || mode == "unified"),
			Interval: getDuration("API1_INTERVAL_SECONDS", 30),
			BaseURL:  getEnv("API1_BASE_URL", "https://apis.data.go.kr/6410000/buslocationservice/v2/getBusLocationListv2"),
			RouteIDs: getRouteIDList("API1_ROUTE_IDS"),
			Priority: getIntEnv("API1_PRIORITY", 1),
		},

		// API2 설정 (공공데이터포털 버스위치정보)
		API2Config: APIConfig{
			Enabled:  getBoolEnv("API2_ENABLED", mode == "api2" || mode == "unified"),
			Interval: getDuration("API2_INTERVAL_SECONDS", 45),
			BaseURL:  getEnv("API2_BASE_URL", "http://apis.data.go.kr/1613000/BusLcInfoInqireService/getRouteAcctoBusLcList"),
			RouteIDs: getRouteIDList("API2_ROUTE_IDS"),
			Priority: getIntEnv("API2_PRIORITY", 2),
		},
	}

	// 모드별 기본 설정 적용
	cfg.applyModeDefaults()

	// 하위 호환성을 위한 RouteIDs 설정
	if len(cfg.RouteIDs) == 0 {
		if cfg.Mode == "api1" && len(cfg.API1Config.RouteIDs) > 0 {
			cfg.RouteIDs = cfg.API1Config.RouteIDs
		} else if cfg.Mode == "api2" && len(cfg.API2Config.RouteIDs) > 0 {
			cfg.RouteIDs = cfg.API2Config.RouteIDs
		}
	}

	// 레거시 모드의 APIBaseURL 설정
	if cfg.APIBaseURL == "" {
		if cfg.Mode == "api1" {
			cfg.APIBaseURL = cfg.API1Config.BaseURL
		} else if cfg.Mode == "api2" {
			cfg.APIBaseURL = cfg.API2Config.BaseURL
		}
	}

	// 설정 검증
	if err := cfg.Validate(); err != nil {
		log.Fatalf("설정 검증 실패: %v", err)
	}

	return cfg
}

// applyModeDefaults 모드별 기본 설정 적용
func (c *Config) applyModeDefaults() {
	switch c.Mode {
	case "api1":
		// API1 모드: API1만 활성화
		c.API1Config.Enabled = true
		c.API2Config.Enabled = false

		// 레거시 모드용 기본 노선 설정
		if len(c.API1Config.RouteIDs) == 0 {
			c.API1Config.RouteIDs = []string{"233000266"}
		}

	case "api2":
		// API2 모드: API2만 활성화
		c.API1Config.Enabled = false
		c.API2Config.Enabled = true

		// 레거시 모드용 기본 노선 설정
		if len(c.API2Config.RouteIDs) == 0 {
			c.API2Config.RouteIDs = []string{"GGB233000266"}
		}

	case "unified":
		// 통합 모드: 환경변수에 따라 결정 (기본값: 둘 다 활성화)
		if !getBoolEnvExists("API1_ENABLED") {
			c.API1Config.Enabled = true
		}
		if !getBoolEnvExists("API2_ENABLED") {
			c.API2Config.Enabled = true
		}

		// 최소 하나는 활성화되어야 함
		if !c.API1Config.Enabled && !c.API2Config.Enabled {
			log.Println("경고: 통합 모드에서 API1과 API2가 모두 비활성화되어 있습니다. API1을 활성화합니다.")
			c.API1Config.Enabled = true
		}

	default:
		log.Printf("경고: 알 수 없는 모드 '%s'. API1 모드로 설정합니다.", c.Mode)
		c.Mode = "api1"
		c.applyModeDefaults()
	}
}

// Validate 설정 유효성 검증
func (c *Config) Validate() error {
	if c.ServiceKey == "" {
		return fmt.Errorf("SERVICE_KEY가 설정되지 않았습니다. 환경변수를 확인해주세요")
	}

	switch c.Mode {
	case "api1":
		if len(c.API1Config.RouteIDs) == 0 {
			return fmt.Errorf("API1 모드에서 API1_ROUTE_IDS가 설정되지 않았습니다")
		}
	case "api2":
		if len(c.API2Config.RouteIDs) == 0 {
			return fmt.Errorf("API2 모드에서 API2_ROUTE_IDS가 설정되지 않았습니다")
		}
	case "unified":
		if c.API1Config.Enabled && len(c.API1Config.RouteIDs) == 0 {
			return fmt.Errorf("API1이 활성화되었지만 API1_ROUTE_IDS가 설정되지 않았습니다")
		}
		if c.API2Config.Enabled && len(c.API2Config.RouteIDs) == 0 {
			return fmt.Errorf("API2가 활성화되었지만 API2_ROUTE_IDS가 설정되지 않았습니다")
		}
		if !c.API1Config.Enabled && !c.API2Config.Enabled {
			return fmt.Errorf("통합 모드에서 최소 하나의 API는 활성화되어야 합니다")
		}
	}

	return nil
}

// IsAPI1Mode API1 모드인지 확인
func (c *Config) IsAPI1Mode() bool {
	return c.Mode == "api1"
}

// IsAPI2Mode API2 모드인지 확인
func (c *Config) IsAPI2Mode() bool {
	return c.Mode == "api2"
}

// IsUnifiedMode 통합 모드인지 확인
func (c *Config) IsUnifiedMode() bool {
	return c.Mode == "unified"
}

// GetModeDescription 모드 설명 반환
func (c *Config) GetModeDescription() string {
	switch c.Mode {
	case "api1":
		return "API1 모드 (경기도 버스위치정보 v2)"
	case "api2":
		return "API2 모드 (공공데이터포털 버스위치정보)"
	case "unified":
		return "통합 모드 (API1 + API2)"
	default:
		return "알 수 없는 모드"
	}
}

// getEnv 환경변수 값을 가져오거나 기본값 반환
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getIntEnv 환경변수에서 정수값을 가져오거나 기본값 반환
func getIntEnv(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
		log.Printf("환경변수 %s 값이 올바르지 않습니다. 기본값 %d를 사용합니다.", key, defaultValue)
	}
	return defaultValue
}

// getBoolEnv 환경변수에서 불린값을 가져오거나 기본값 반환
func getBoolEnv(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
		log.Printf("환경변수 %s 값이 올바르지 않습니다. 기본값 %t를 사용합니다.", key, defaultValue)
	}
	return defaultValue
}

// getBoolEnvExists 환경변수가 존재하는지 확인
func getBoolEnvExists(key string) bool {
	_, exists := os.LookupEnv(key)
	return exists
}

// getRouteIDList 환경변수에서 RouteID 리스트 파싱
func getRouteIDList(key string) []string {
	routeIDsEnv := getEnv(key, "")

	if routeIDsEnv == "" {
		return []string{}
	}

	// 쉼표로 분리하고 공백 제거
	routeIDs := strings.Split(routeIDsEnv, ",")
	var cleanRouteIDs []string

	for _, id := range routeIDs {
		cleanID := strings.TrimSpace(id)
		if cleanID != "" {
			cleanRouteIDs = append(cleanRouteIDs, cleanID)
		}
	}

	return cleanRouteIDs
}

// getDuration 환경변수에서 duration 파싱
func getDuration(key string, defaultSeconds int) time.Duration {
	if value := os.Getenv(key); value != "" {
		if seconds, err := strconv.Atoi(value); err == nil {
			return time.Duration(seconds) * time.Second
		}
		log.Printf("환경변수 %s 값이 올바르지 않습니다. 기본값 %d초를 사용합니다.", key, defaultSeconds)
	}
	return time.Duration(defaultSeconds) * time.Second
}

func getDurationMinutes(key string, defaultMinutes int) time.Duration {
	if value := os.Getenv(key); value != "" {
		if minutes, err := strconv.Atoi(value); err == nil {
			return time.Duration(minutes) * time.Minute
		}
		log.Printf("환경변수 %s 값이 올바르지 않습니다. 기본값 %d분을 사용합니다.", key, defaultMinutes)
	}
	return time.Duration(defaultMinutes) * time.Minute
}

// IsOperatingTime 현재 시간이 운영 시간인지 확인
func (c *Config) IsOperatingTime(currentTime time.Time) bool {
	hour := currentTime.Hour()
	minute := currentTime.Minute()

	startHour := c.OperatingStartHour
	startMinute := c.OperatingStartMinute
	endHour := c.OperatingEndHour
	endMinute := c.OperatingEndMinute

	// 현재 시간을 분 단위로 변환 (하루 = 1440분)
	currentMinutes := hour*60 + minute
	startMinutes := startHour*60 + startMinute
	endMinutes := endHour*60 + endMinute

	// 시작 시간이 종료 시간보다 작은 경우 (예: 06:00 ~ 10:00)
	if startMinutes < endMinutes {
		return currentMinutes >= startMinutes && currentMinutes < endMinutes
	}

	// 시작 시간이 종료 시간보다 큰 경우 (예: 22:00 ~ 06:00, 자정을 넘어가는 경우)
	if startMinutes > endMinutes {
		return currentMinutes >= startMinutes || currentMinutes < endMinutes
	}

	// 시작 시간과 종료 시간이 같은 경우 (24시간 운영)
	return true
}

// GetNextOperatingTime 다음 운영 시작 시간 반환
func (c *Config) GetNextOperatingTime(currentTime time.Time) time.Time {
	startHour := c.OperatingStartHour
	startMinute := c.OperatingStartMinute

	// 당일 시작 시간 계산
	todayStart := time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day(),
		startHour, startMinute, 0, 0, currentTime.Location())

	// 현재 시간이 당일 시작 시간 이전이면 당일 시작 시간 반환
	if currentTime.Before(todayStart) {
		return todayStart
	}

	// 그렇지 않으면 다음날 시작 시간 반환
	nextDay := currentTime.AddDate(0, 0, 1)
	return time.Date(nextDay.Year(), nextDay.Month(), nextDay.Day(),
		startHour, startMinute, 0, 0, currentTime.Location())
}

// PrintConfig 현재 설정을 출력 (디버깅용)
func (c *Config) PrintConfig() {
	log.Println("=== 버스 트래커 설정 ===")
	log.Printf("동작 모드: %s", c.GetModeDescription())

	if len(c.ServiceKey) >= 10 {
		log.Printf("Service Key: %s***", c.ServiceKey[:10])
	} else {
		log.Printf("Service Key: %s***", c.ServiceKey)
	}

	log.Printf("City Code: %s", c.CityCode)
	log.Printf("Elasticsearch URL: %s", c.ElasticsearchURL)
	log.Printf("Index Name: %s", c.IndexName)

	if c.IsUnifiedMode() {
		// 통합 모드 설정 출력
		log.Printf("=== 통합 모드 설정 ===")
		log.Printf("데이터 통합 주기: %v", c.DataMergeInterval)
		log.Printf("데이터 보존 기간: %v", c.DataRetentionPeriod)

		// API1 설정
		log.Printf("API1 - 활성화: %t, 주기: %v, 노선수: %d개",
			c.API1Config.Enabled, c.API1Config.Interval, len(c.API1Config.RouteIDs))
		if c.API1Config.Enabled && len(c.API1Config.RouteIDs) > 0 {
			log.Printf("  API1 노선들: %v", c.API1Config.RouteIDs)
		}

		// API2 설정
		log.Printf("API2 - 활성화: %t, 주기: %v, 노선수: %d개",
			c.API2Config.Enabled, c.API2Config.Interval, len(c.API2Config.RouteIDs))
		if c.API2Config.Enabled && len(c.API2Config.RouteIDs) > 0 {
			log.Printf("  API2 노선들: %v", c.API2Config.RouteIDs)
		}
	} else {
		// 단일 API 모드 설정 출력
		log.Printf("=== %s 설정 ===", c.GetModeDescription())
		log.Printf("API Base URL: %s", c.APIBaseURL)
		log.Printf("Route IDs: %v", c.RouteIDs)
		log.Printf("Interval: %v", c.Interval)
	}

	// 공통 설정
	log.Printf("운영 시간: %02d:%02d ~ %02d:%02d",
		c.OperatingStartHour, c.OperatingStartMinute, c.OperatingEndHour, c.OperatingEndMinute)
	log.Printf("버스 정리 주기: %v", c.BusCleanupInterval)
	log.Printf("버스 타임아웃: %v", c.BusTimeoutDuration)
	log.Println("========================")
}
