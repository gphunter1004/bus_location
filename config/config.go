package config

import (
	"fmt"
	"log"
	"time"

	"bus-tracker/internal/utils"

	"github.com/joho/godotenv"
)

// APIConfig 개별 API 설정
type APIConfig struct {
	Interval time.Duration `json:"interval"` // 호출 주기
	BaseURL  string        `json:"baseURL"`  // API URL
	RouteIDs []string      `json:"routeIDs"` // 대상 노선
	Priority int           `json:"priority"` // 우선순위 (낮을수록 우선)
}

// RedisConfig Redis 설정 구조체
type RedisConfig struct {
	Addr        string `json:"addr"`        // Redis 주소 (예: localhost:6379)
	Password    string `json:"password"`    // Redis 비밀번호
	DB          int    `json:"db"`          // Redis DB 번호
	MaxRetries  int    `json:"maxRetries"`  // 최대 재시도 횟수
	PoolSize    int    `json:"poolSize"`    // 연결 풀 크기
	IdleTimeout int    `json:"idleTimeout"` // 유휴 연결 타임아웃 (초)
}

// Config 애플리케이션 설정 구조체 (통합 모드 전용)
type Config struct {
	// 기본 인증 정보
	ServiceKey string
	CityCode   string

	// Elasticsearch 설정
	ElasticsearchURL      string
	ElasticsearchUsername string
	ElasticsearchPassword string
	IndexName             string

	// Redis 설정
	Redis RedisConfig

	// 운영 시간 설정
	OperatingStartHour   int // 운영 시작 시간 (24시간 형식)
	OperatingStartMinute int // 운영 시작 분
	OperatingEndHour     int // 운영 종료 시간 (24시간 형식)
	OperatingEndMinute   int // 운영 종료 분

	// 버스 트래킹 종료 조건 설정
	BusCleanupInterval      time.Duration // 버스 정리 작업 주기 (기본: 5분)
	BusDisappearanceTimeout time.Duration // 버스 미목격 종료 시간 (기본: 10분)
	EnableTerminalStop      bool          // 종점 도착 시 종료 활성화 (기본: true)

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

	// 환경변수 디버깅 출력
	debugEnvironmentVariables()

	cfg := &Config{
		// 기본 인증 정보
		ServiceKey: getEnv("SERVICE_KEY", ""),
		CityCode:   getEnv("CITY_CODE", "31240"),

		// Elasticsearch 설정
		ElasticsearchURL:      getEnv("ELASTICSEARCH_URL", "http://localhost:9200"),
		ElasticsearchUsername: getEnv("ELASTICSEARCH_USERNAME", ""),
		ElasticsearchPassword: getEnv("ELASTICSEARCH_PASSWORD", ""),
		IndexName:             getEnv("INDEX_NAME", "bus-locations"),

		// Redis 설정
		Redis: RedisConfig{
			Addr:        getEnv("REDIS_ADDR", "localhost:6379"),
			Password:    getEnv("REDIS_PASSWORD", ""),
			DB:          getIntEnv("REDIS_DB", 0),
			MaxRetries:  getIntEnv("REDIS_MAX_RETRIES", 3),
			PoolSize:    getIntEnv("REDIS_POOL_SIZE", 10),
			IdleTimeout: getIntEnv("REDIS_IDLE_TIMEOUT", 300), // 5분
		},

		// 운영 시간 기본값: 04:55 ~ 01:00
		OperatingStartHour:   getIntEnv("OPERATING_START_HOUR", 4),
		OperatingStartMinute: getIntEnv("OPERATING_START_MINUTE", 55),
		OperatingEndHour:     getIntEnv("OPERATING_END_HOUR", 1),
		OperatingEndMinute:   getIntEnv("OPERATING_END_MINUTE", 0),

		// 버스 트래킹 종료 조건 설정
		BusCleanupInterval:      getDurationMinutes("BUS_CLEANUP_INTERVAL_MINUTES", 5),       // 5분
		BusDisappearanceTimeout: getDurationMinutes("BUS_DISAPPEARANCE_TIMEOUT_MINUTES", 10), // 10분
		EnableTerminalStop:      getBoolEnv("ENABLE_TERMINAL_STOP", true),                    // true

		// 통합 처리 설정
		DataMergeInterval:   getDuration("DATA_MERGE_INTERVAL_SECONDS", 10),  // 10초
		DataRetentionPeriod: getDurationMinutes("DATA_RETENTION_MINUTES", 5), // 5분

		// API1 설정 (경기도 버스위치정보 v2)
		API1Config: APIConfig{
			Interval: getDuration("API1_INTERVAL_SECONDS", 30),
			BaseURL:  getEnv("API1_BASE_URL", "https://apis.data.go.kr/6410000/buslocationservice/v2/getBusLocationListv2"),
			RouteIDs: getRouteIDList("API1_ROUTE_IDS"),
			Priority: getIntEnv("API1_PRIORITY", 1),
		},

		// API2 설정 (공공데이터포털 버스위치정보)
		API2Config: APIConfig{
			Interval: getDuration("API2_INTERVAL_SECONDS", 45),
			BaseURL:  getEnv("API2_BASE_URL", "http://apis.data.go.kr/1613000/BusLcInfoInqireService/getRouteAcctoBusLcList"),
			RouteIDs: getRouteIDList("API2_ROUTE_IDS"),
			Priority: getIntEnv("API2_PRIORITY", 2),
		},
	}

	// 최소 하나의 API에 노선이 설정되어야 함
	if len(cfg.API1Config.RouteIDs) == 0 && len(cfg.API2Config.RouteIDs) == 0 {
		log.Println("경고: API1과 API2 노선이 모두 비어있습니다. API1에 기본 노선을 설정합니다.")
		cfg.API1Config.RouteIDs = []string{"233000266"}
	}

	// 설정 검증
	if err := cfg.Validate(); err != nil {
		log.Fatalf("설정 검증 실패: %v", err)
	}

	return cfg
}

// Validate 설정 유효성 검증
func (c *Config) Validate() error {
	if utils.Validate.IsEmpty(c.ServiceKey) {
		return fmt.Errorf("SERVICE_KEY가 설정되지 않았습니다. 환경변수를 확인해주세요")
	}

	// Redis 설정 검증
	if utils.Validate.IsEmpty(c.Redis.Addr) {
		return fmt.Errorf("REDIS_ADDR이 설정되지 않았습니다")
	}

	if c.Redis.DB < 0 || c.Redis.DB > 15 {
		return fmt.Errorf("REDIS_DB는 0-15 범위여야 합니다 (현재: %d)", c.Redis.DB)
	}

	if c.Redis.PoolSize <= 0 {
		return fmt.Errorf("REDIS_POOL_SIZE는 0보다 커야 합니다 (현재: %d)", c.Redis.PoolSize)
	}

	// 종료 조건 설정 검증
	if c.BusDisappearanceTimeout <= 0 {
		return fmt.Errorf("BUS_DISAPPEARANCE_TIMEOUT_MINUTES는 0보다 커야 합니다 (현재: %v)", c.BusDisappearanceTimeout)
	}

	if c.BusCleanupInterval <= 0 {
		return fmt.Errorf("BUS_CLEANUP_INTERVAL_MINUTES는 0보다 커야 합니다 (현재: %v)", c.BusCleanupInterval)
	}

	// API 설정 검증 - 노선 설정만 확인
	hasAPI1Routes := len(c.API1Config.RouteIDs) > 0
	hasAPI2Routes := len(c.API2Config.RouteIDs) > 0

	if !hasAPI1Routes && !hasAPI2Routes {
		return fmt.Errorf("API1과 API2 모두 노선이 설정되지 않았습니다. 최소 하나의 API에는 노선을 설정해야 합니다")
	}

	// API1 노선 검증
	for _, routeID := range c.API1Config.RouteIDs {
		if !utils.Validate.IsValidRouteID(routeID, "api1") {
			return fmt.Errorf("API1 노선 ID 형식이 올바르지 않습니다: %s", routeID)
		}
	}

	// API2 노선 검증
	for _, routeID := range c.API2Config.RouteIDs {
		if !utils.Validate.IsValidRouteID(routeID, "api2") {
			return fmt.Errorf("API2 노선 ID 형식이 올바르지 않습니다: %s", routeID)
		}
	}

	return nil
}
