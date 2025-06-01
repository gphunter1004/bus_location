package config

import (
	"log"
	"os"
	"strings"
	"time"

	"bus-tracker/internal/utils"
)

// getEnv 환경변수 값을 가져오거나 기본값 반환
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); !utils.Validate.IsEmpty(value) {
		return utils.String.TrimSpace(value)
	}
	return defaultValue
}

// getIntEnv 환경변수에서 정수값을 가져오거나 기본값 반환
func getIntEnv(key string, defaultValue int) int {
	value := utils.String.TrimSpace(os.Getenv(key))

	// 디버깅: 변환 과정 출력
	log.Printf("DEBUG - getIntEnv('%s'): raw='%s', trimmed='%s'", key, os.Getenv(key), value)

	// 새로운 헬퍼 함수 사용 (성공 여부 확인 가능)
	result, success := utils.Convert.StringToIntWithSuccess(value, defaultValue)

	if value != "" && !success {
		log.Printf("환경변수 %s 값이 올바르지 않습니다 ('%s'). 기본값 %d를 사용합니다.", key, value, defaultValue)
	}

	log.Printf("DEBUG - getIntEnv('%s'): result=%d (성공: %t)", key, result, success)
	return result
}

// getBoolEnv 환경변수에서 불린값을 가져오거나 기본값 반환
func getBoolEnv(key string, defaultValue bool) bool {
	value := utils.String.TrimSpace(os.Getenv(key))

	// 디버깅: 변환 과정 출력
	log.Printf("DEBUG - getBoolEnv('%s'): raw='%s', trimmed='%s'", key, os.Getenv(key), value)

	// 새로운 헬퍼 함수 사용 (성공 여부 확인 가능)
	result, success := utils.Convert.StringToBoolWithSuccess(value, defaultValue)

	if value != "" && !success {
		log.Printf("환경변수 %s 값이 올바르지 않습니다 ('%s'). 기본값 %t를 사용합니다.", key, value, defaultValue)
	}

	log.Printf("DEBUG - getBoolEnv('%s'): result=%t (성공: %t)", key, result, success)
	return result
}

// getRouteIDList 환경변수에서 RouteID 리스트 파싱
func getRouteIDList(key string) []string {
	routeIDsEnv := utils.String.TrimSpace(getEnv(key, ""))

	if utils.Validate.IsEmpty(routeIDsEnv) {
		return []string{}
	}

	// 쉼표로 분리하고 공백 제거
	routeIDs := utils.String.Split(routeIDsEnv, ",")

	// 빈 문자열 제거 및 공백 정리
	var cleanRouteIDs []string
	for _, id := range routeIDs {
		cleanID := utils.String.TrimSpace(id)
		if !utils.Validate.IsEmpty(cleanID) {
			cleanRouteIDs = append(cleanRouteIDs, cleanID)
		}
	}

	// 중복 제거
	return utils.Slice.RemoveDuplicateStrings(cleanRouteIDs)
}

// getDuration 환경변수에서 duration 파싱 (초 단위)
func getDuration(key string, defaultSeconds int) time.Duration {
	value := utils.String.TrimSpace(os.Getenv(key))

	// 디버깅: 변환 과정 출력
	log.Printf("DEBUG - getDuration('%s'): raw='%s', trimmed='%s'", key, os.Getenv(key), value)

	// 정수 변환 시도
	intValue, success := utils.Convert.StringToIntWithSuccess(value, defaultSeconds)

	if value != "" && !success {
		log.Printf("환경변수 %s 값이 올바르지 않습니다 ('%s'). 기본값 %d초를 사용합니다.", key, value, defaultSeconds)
	}

	result := time.Duration(intValue) * time.Second
	log.Printf("DEBUG - getDuration('%s'): result=%v (성공: %t)", key, result, success)
	return result
}

// getDurationMinutes 환경변수에서 분 단위 duration 파싱
func getDurationMinutes(key string, defaultMinutes int) time.Duration {
	value := utils.String.TrimSpace(os.Getenv(key))

	// 디버깅: 변환 과정 출력
	log.Printf("DEBUG - getDurationMinutes('%s'): raw='%s', trimmed='%s'", key, os.Getenv(key), value)

	// 정수 변환 시도
	intValue, success := utils.Convert.StringToIntWithSuccess(value, defaultMinutes)

	if value != "" && !success {
		log.Printf("환경변수 %s 값이 올바르지 않습니다 ('%s'). 기본값 %d분을 사용합니다.", key, value, defaultMinutes)
	}

	result := time.Duration(intValue) * time.Minute
	log.Printf("DEBUG - getDurationMinutes('%s'): result=%v (성공: %t)", key, result, success)
	return result
}

// maskSensitive 민감한 정보 마스킹
func maskSensitive(value string) string {
	if len(value) <= 4 {
		return strings.Repeat("*", len(value))
	}
	return value[:2] + strings.Repeat("*", len(value)-4) + value[len(value)-2:]
}

// debugEnvironmentVariables 환경변수 디버깅 출력
func debugEnvironmentVariables() {
	log.Printf("DEBUG - REDIS_ADDR: '%s'", os.Getenv("REDIS_ADDR"))
	log.Printf("DEBUG - REDIS_PASSWORD: '%s'", maskSensitive(os.Getenv("REDIS_PASSWORD")))
	log.Printf("DEBUG - REDIS_DB: '%s'", os.Getenv("REDIS_DB"))
	log.Printf("DEBUG - OPERATING_START_HOUR: '%s'", os.Getenv("OPERATING_START_HOUR"))
	log.Printf("DEBUG - OPERATING_START_MINUTE: '%s'", os.Getenv("OPERATING_START_MINUTE"))
	log.Printf("DEBUG - OPERATING_END_HOUR: '%s'", os.Getenv("OPERATING_END_HOUR"))
	log.Printf("DEBUG - OPERATING_END_MINUTE: '%s'", os.Getenv("OPERATING_END_MINUTE"))
	log.Printf("DEBUG - BUS_CLEANUP_INTERVAL_MINUTES: '%s'", os.Getenv("BUS_CLEANUP_INTERVAL_MINUTES"))
	log.Printf("DEBUG - BUS_DISAPPEARANCE_TIMEOUT_MINUTES: '%s'", os.Getenv("BUS_DISAPPEARANCE_TIMEOUT_MINUTES"))
	log.Printf("DEBUG - ENABLE_TERMINAL_STOP: '%s'", os.Getenv("ENABLE_TERMINAL_STOP"))
	log.Printf("DEBUG - DATA_MERGE_INTERVAL_SECONDS: '%s'", os.Getenv("DATA_MERGE_INTERVAL_SECONDS"))
	log.Printf("DEBUG - DATA_RETENTION_MINUTES: '%s'", os.Getenv("DATA_RETENTION_MINUTES"))
	log.Printf("DEBUG - API1_INTERVAL_SECONDS: '%s'", os.Getenv("API1_INTERVAL_SECONDS"))
	log.Printf("DEBUG - API2_INTERVAL_SECONDS: '%s'", os.Getenv("API2_INTERVAL_SECONDS"))
}
