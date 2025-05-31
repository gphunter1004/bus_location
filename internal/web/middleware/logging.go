// internal/web/middleware/logging.go
package middleware

import (
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
)

// RequestLogger 요청 로깅 미들웨어
func RequestLogger() fiber.Handler {
	return logger.New(logger.Config{
		Format:     "[${time}] ${status} - ${method} ${path} (${latency}) ${ip} \"${ua}\" ${locals:requestId}\n",
		TimeFormat: "2006-01-02 15:04:05",
		TimeZone:   "Asia/Seoul",
		Done: func(c *fiber.Ctx, logString []byte) {
			// 로그를 파일이나 외부 시스템으로 전송할 수 있습니다
		},
	})
}

// ErrorLogger 에러 로깅 미들웨어
func ErrorLogger() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// 요청 시작 시간 기록
		start := time.Now()

		// 다음 핸들러 실행
		err := c.Next()

		// 에러가 발생한 경우 로깅
		if err != nil {
			duration := time.Since(start)

			// 에러 정보 로깅 (실제 구현에서는 로거 사용)
			logData := map[string]interface{}{
				"timestamp": time.Now(),
				"method":    c.Method(),
				"path":      c.Path(),
				"status":    c.Response().StatusCode(),
				"duration":  duration,
				"error":     err.Error(),
				"ip":        c.IP(),
				"userAgent": c.Get("User-Agent"),
				"requestId": c.Locals("requestId"),
			}

			// 실제 로거로 전송 (예: 구조화된 로깅)
			_ = logData // 임시로 사용하지 않음 표시
		}

		return err
	}
}

// RateLimit 요청 제한 미들웨어 (기본 구현)
func RateLimit() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// 간단한 레이트 리미팅 구현
		// 실제 운영에서는 Redis나 메모리 기반의 레이트 리미터 사용

		// 현재는 패스스루
		return c.Next()
	}
}
