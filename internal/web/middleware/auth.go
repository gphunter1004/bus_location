// internal/web/middleware/auth.go
package middleware

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
)

// APIKeyAuth API 키 인증 미들웨어
func APIKeyAuth() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// API 키가 설정되지 않은 경우 인증 건너뛰기
		expectedAPIKey := os.Getenv("API_KEY")
		if expectedAPIKey == "" {
			return c.Next()
		}

		// 헤더에서 API 키 추출
		apiKey := c.Get("X-API-Key")
		if apiKey == "" {
			// Authorization 헤더에서도 확인
			auth := c.Get("Authorization")
			if strings.HasPrefix(auth, "Bearer ") {
				apiKey = strings.TrimPrefix(auth, "Bearer ")
			}
		}

		// API 키 검증
		if apiKey != expectedAPIKey {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error":   true,
				"message": "유효하지 않은 API 키입니다",
				"code":    fiber.StatusUnauthorized,
			})
		}

		// 인증 성공 시 사용자 정보 설정
		c.Locals("authenticated", true)
		c.Locals("api_key", apiKey)

		return c.Next()
	}
}

// OptionalAPIKeyAuth 선택적 API 키 인증 (키가 있으면 검증, 없으면 통과)
func OptionalAPIKeyAuth() fiber.Handler {
	return func(c *fiber.Ctx) error {
		expectedAPIKey := os.Getenv("API_KEY")
		if expectedAPIKey == "" {
			return c.Next()
		}

		apiKey := c.Get("X-API-Key")
		if apiKey == "" {
			auth := c.Get("Authorization")
			if strings.HasPrefix(auth, "Bearer ") {
				apiKey = strings.TrimPrefix(auth, "Bearer ")
			}
		}

		// API 키가 제공되었다면 검증
		if apiKey != "" {
			if apiKey != expectedAPIKey {
				return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
					"error":   true,
					"message": "유효하지 않은 API 키입니다",
					"code":    fiber.StatusUnauthorized,
				})
			}
			c.Locals("authenticated", true)
			c.Locals("api_key", apiKey)
		}

		return c.Next()
	}
}

// AdminAuth 관리자 인증 미들웨어
func AdminAuth() fiber.Handler {
	return func(c *fiber.Ctx) error {
		adminKey := os.Getenv("ADMIN_KEY")
		if adminKey == "" {
			// 관리자 키가 설정되지 않은 경우 모든 요청 허용
			return c.Next()
		}

		apiKey := c.Get("X-Admin-Key")
		if apiKey == "" {
			auth := c.Get("Authorization")
			if strings.HasPrefix(auth, "Admin ") {
				apiKey = strings.TrimPrefix(auth, "Admin ")
			}
		}

		if apiKey != adminKey {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error":   true,
				"message": "관리자 권한이 필요합니다",
				"code":    fiber.StatusForbidden,
			})
		}

		c.Locals("admin", true)
		return c.Next()
	}
}

// RequestIDMiddleware 요청 ID 미들웨어
func RequestIDMiddleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		requestID := c.Get("X-Request-ID")
		if requestID == "" {
			requestID = generateRequestID()
		}

		c.Set("X-Request-ID", requestID)
		c.Locals("requestId", requestID)

		return c.Next()
	}
}

// generateRequestID 간단한 요청 ID 생성
func generateRequestID() string {
	return fmt.Sprintf("req_%d", time.Now().UnixNano())
}
