package config

import (
	"log"
	"time"

	"bus-tracker/internal/utils"
)

// PrintConfig 현재 설정을 출력 (디버깅용)
func (c *Config) PrintConfig() {
	log.Println("=== 버스 트래커 설정 (통합 모드 + Redis) ===")

	// ServiceKey 마스킹
	maskedServiceKey := utils.String.MaskSensitive(c.ServiceKey, 10, 4)
	log.Printf("Service Key: %s", maskedServiceKey)

	log.Printf("City Code: %s", c.CityCode)
	log.Printf("Elasticsearch URL: %s", c.ElasticsearchURL)
	log.Printf("Index Name: %s", c.IndexName)

	// Redis 설정 출력
	c.printRedisConfig()

	// 통합 모드 설정 출력
	c.printUnifiedModeConfig()

	// 버스 트래킹 종료 조건 설정 출력
	c.printTrackingConfig()

	// API 설정 출력
	c.printAPIConfigs()

	// 운영 시간 설정
	c.printOperatingTimeConfig()

	// Elasticsearch 설정
	c.printElasticsearchConfig()

	// 설정 검증 상태
	c.printValidationStatus()

	log.Println("====================================")
}

// printRedisConfig Redis 설정 출력
func (c *Config) printRedisConfig() {
	log.Printf("=== Redis 설정 ===")
	log.Printf("Redis 주소: %s", c.Redis.Addr)
	log.Printf("Redis DB: %d", c.Redis.DB)
	log.Printf("Redis 풀 크기: %d", c.Redis.PoolSize)
	log.Printf("Redis 최대 재시도: %d", c.Redis.MaxRetries)
	log.Printf("Redis 유휴 타임아웃: %d초", c.Redis.IdleTimeout)

	if c.Redis.Password != "" {
		log.Printf("Redis 비밀번호: %s", maskSensitive(c.Redis.Password))
	} else {
		log.Printf("Redis 비밀번호: 없음")
	}
}

// printUnifiedModeConfig 통합 모드 설정 출력
func (c *Config) printUnifiedModeConfig() {
	log.Printf("=== 통합 모드 설정 ===")
	log.Printf("데이터 통합 주기: %v", c.DataMergeInterval)
	log.Printf("데이터 보존 기간: %v", c.DataRetentionPeriod)
}

// printTrackingConfig 버스 트래킹 설정 출력
func (c *Config) printTrackingConfig() {
	log.Printf("=== 버스 트래킹 설정 ===")
	log.Printf("버스 미목격 종료 시간: %v", c.BusDisappearanceTimeout)
	log.Printf("종점 도착 시 종료: %t", c.EnableTerminalStop)
	log.Printf("정리 작업 주기: %v", c.BusCleanupInterval)
}

// printAPIConfigs API 설정들 출력
func (c *Config) printAPIConfigs() {
	// API1 설정
	log.Printf("=== API1 설정 (경기도 버스정보) ===")
	log.Printf("주기: %v, 노선수: %d개", c.API1Config.Interval, len(c.API1Config.RouteIDs))
	if len(c.API1Config.RouteIDs) > 0 {
		log.Printf("노선들: %v", c.API1Config.RouteIDs)
	}
	log.Printf("Base URL: %s", c.API1Config.BaseURL)
	log.Printf("우선순위: %d", c.API1Config.Priority)

	// API2 설정
	log.Printf("=== API2 설정 (공공데이터포털) ===")
	log.Printf("주기: %v, 노선수: %d개", c.API2Config.Interval, len(c.API2Config.RouteIDs))
	if len(c.API2Config.RouteIDs) > 0 {
		log.Printf("노선들: %v", c.API2Config.RouteIDs)
	}
	log.Printf("Base URL: %s", c.API2Config.BaseURL)
	log.Printf("우선순위: %d", c.API2Config.Priority)
}

// printOperatingTimeConfig 운영 시간 설정 출력
func (c *Config) printOperatingTimeConfig() {
	log.Printf("=== 운영 시간 설정 ===")
	log.Printf("운영 시간: %s", c.GetOperatingScheduleString())
	log.Printf("운영 기간: %v", c.GetOperatingDuration())

	// 현재 시간 기준 운영 상태
	isOperating, timeUntilNext := c.IsWithinOperatingHours(time.Now())
	if isOperating {
		log.Printf("현재 상태: 운영 중")
	} else {
		log.Printf("현재 상태: 운영 시간 외 (다음 운영까지: %v)", timeUntilNext)
	}
}

// printElasticsearchConfig Elasticsearch 설정 출력
func (c *Config) printElasticsearchConfig() {
	log.Printf("=== Elasticsearch 설정 ===")
	log.Printf("URL: %s", c.ElasticsearchURL)
	log.Printf("인덱스명: %s", c.IndexName)

	if c.ElasticsearchUsername != "" {
		log.Printf("사용자명: %s", c.ElasticsearchUsername)
		log.Printf("비밀번호: %s", maskSensitive(c.ElasticsearchPassword))
	} else {
		log.Printf("인증: 없음")
	}
}

// printValidationStatus 설정 검증 상태 출력
func (c *Config) printValidationStatus() {
	log.Printf("=== 설정 검증 상태 ===")

	if err := c.Validate(); err != nil {
		log.Printf("검증 결과: ❌ 실패 - %v", err)
	} else {
		log.Printf("검증 결과: ✅ 성공")
	}

	// 추가 정보
	log.Printf("총 설정된 노선 수: %d개", len(c.API1Config.RouteIDs)+len(c.API2Config.RouteIDs))

	hasAPI1 := len(c.API1Config.RouteIDs) > 0
	hasAPI2 := len(c.API2Config.RouteIDs) > 0

	if hasAPI1 && hasAPI2 {
		log.Printf("운영 모드: 통합 모드 (API1 + API2)")
	} else if hasAPI1 {
		log.Printf("운영 모드: API1 전용")
	} else if hasAPI2 {
		log.Printf("운영 모드: API2 전용")
	} else {
		log.Printf("운영 모드: ⚠️ 설정된 노선 없음")
	}
}
