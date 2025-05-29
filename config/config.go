package config

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

// Config 애플리케이션 설정 구조체
type Config struct {
	APIBaseURL            string
	ServiceKey            string
	RouteIDs              []string
	ElasticsearchURL      string
	ElasticsearchUsername string
	ElasticsearchPassword string
	IndexName             string
	Interval              time.Duration
	// API 선택 설정
	APIType  string // "api1" 또는 "api2"
	CityCode string // API2용 도시 코드
	// 운영 시간 설정
	OperatingStartHour   int // 운영 시작 시간 (24시간 형식)
	OperatingStartMinute int // 운영 시작 분
	OperatingEndHour     int // 운영 종료 시간 (24시간 형식)
	OperatingEndMinute   int // 운영 종료 분
	// 버스 트래킹 설정
	BusCleanupInterval time.Duration // 버스 정리 작업 주기 (기본: 5분)
	BusTimeoutDuration time.Duration // 버스 미목격 제한 시간 (기본: 60분)
}

// LoadConfig 환경변수 또는 기본값으로 설정을 로드
func LoadConfig() *Config {
	// .env 파일 로드 시도 (선택사항)
	if err := godotenv.Load(); err != nil {
		log.Println(".env 파일을 찾을 수 없습니다. 시스템 환경변수를 사용합니다.")
	} else {
		log.Println(".env 파일을 성공적으로 로드했습니다.")
	}

	cfg := &Config{
		APIBaseURL:            getEnv("API_BASE_URL", "https://apis.data.go.kr/6410000/buslocationservice/v2/getBusLocationListv2"),
		ServiceKey:            getEnv("SERVICE_KEY", ""),
		RouteIDs:              getRouteIDList(),
		ElasticsearchURL:      getEnv("ELASTICSEARCH_URL", "http://localhost:9200"),
		ElasticsearchUsername: getEnv("ELASTICSEARCH_USERNAME", ""),
		ElasticsearchPassword: getEnv("ELASTICSEARCH_PASSWORD", ""),
		IndexName:             getEnv("INDEX_NAME", "bus-locations"),
		Interval:              getDuration("INTERVAL_SECONDS", 30),
		// API 설정
		APIType:  getEnv("API_TYPE", "api1"),   // 기본값: api1
		CityCode: getEnv("CITY_CODE", "31240"), // API2용 도시 코드
		// 운영 시간 기본값: 04:55 ~ 01:00 (새벽 1시부터 4시 55분까지 중단)
		OperatingStartHour:   getIntEnv("OPERATING_START_HOUR", 4),
		OperatingStartMinute: getIntEnv("OPERATING_START_MINUTE", 55),
		OperatingEndHour:     getIntEnv("OPERATING_END_HOUR", 1),
		OperatingEndMinute:   getIntEnv("OPERATING_END_MINUTE", 0),
		// 버스 트래킹 설정
		BusCleanupInterval: getDuration("BUS_CLEANUP_INTERVAL_MINUTES", 5) * time.Minute, // 기본: 5분
		BusTimeoutDuration: getDuration("BUS_TIMEOUT_MINUTES", 60) * time.Minute,         // 기본: 60분
	}

	// 로드된 설정 검증
	if len(cfg.RouteIDs) == 0 {
		log.Fatal("ROUTE_IDS가 설정되지 않았습니다. 환경변수를 확인해주세요.")
	}

	if cfg.ServiceKey == "" {
		log.Fatal("SERVICE_KEY가 설정되지 않았습니다. 환경변수를 확인해주세요.")
	}

	return cfg
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

// getRouteIDList 환경변수에서 RouteID 리스트 파싱
func getRouteIDList() []string {
	routeIDsEnv := getEnv("ROUTE_IDS", "")

	if routeIDsEnv == "" {
		log.Println("ROUTE_IDS 환경변수가 설정되지 않았습니다. 기본값을 사용합니다.")
		return []string{"233000266"}
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
	log.Println("=== 현재 설정 ===")
	log.Printf("API Base URL: %s", c.APIBaseURL)
	log.Printf("Service Key: %s***", c.ServiceKey[:10]) // 보안을 위해 일부만 표시
	log.Printf("Route IDs: %v", c.RouteIDs)
	log.Printf("Elasticsearch URL: %s", c.ElasticsearchURL)
	log.Printf("Index Name: %s", c.IndexName)
	log.Printf("Interval: %v", c.Interval)
	log.Printf("Operating Hours: %02d:%02d ~ %02d:%02d (중단 시간: %02d:%02d ~ %02d:%02d)",
		c.OperatingStartHour, c.OperatingStartMinute, c.OperatingEndHour, c.OperatingEndMinute,
		c.OperatingEndHour, c.OperatingEndMinute, c.OperatingStartHour, c.OperatingStartMinute)
	log.Printf("Bus Cleanup Interval: %v", c.BusCleanupInterval)
	log.Printf("Bus Timeout Duration: %v", c.BusTimeoutDuration)
	log.Println("================")
}
