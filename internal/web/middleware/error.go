// internal/web/middleware/error.go
package middleware

import (
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
)

// ErrorHandler 에러 처리 미들웨어
func ErrorHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// 에러가 발생한 경우 처리
		err := c.Next()

		if err != nil {
			code := fiber.StatusInternalServerError

			// Fiber 에러인 경우 상태 코드 추출
			if e, ok := err.(*fiber.Error); ok {
				code = e.Code
			}

			// API 요청인 경우 JSON 응답
			if strings.HasPrefix(c.Path(), "/api/") {
				return c.Status(code).JSON(fiber.Map{
					"error":     true,
					"message":   getErrorMessage(code),
					"details":   err.Error(),
					"code":      code,
					"timestamp": time.Now(),
					"path":      c.Path(),
					"requestId": c.Locals("requestId"),
				})
			}

			// 웹 페이지 요청인 경우 에러 페이지 렌더링
			return c.Status(code).Render("error", fiber.Map{
				"title":     "오류 발생",
				"code":      code,
				"message":   getErrorMessage(code),
				"details":   err.Error(),
				"timestamp": time.Now().Format("2006-01-02 15:04:05"),
				"path":      c.Path(),
			})
		}

		return nil
	}
}

// getErrorMessage 상태 코드에 따른 사용자 친화적 메시지
func getErrorMessage(code int) string {
	switch code {
	case fiber.StatusBadRequest:
		return "잘못된 요청입니다"
	case fiber.StatusUnauthorized:
		return "인증이 필요합니다"
	case fiber.StatusForbidden:
		return "접근 권한이 없습니다"
	case fiber.StatusNotFound:
		return "요청한 리소스를 찾을 수 없습니다"
	case fiber.StatusMethodNotAllowed:
		return "허용되지 않은 메서드입니다"
	case fiber.StatusTooManyRequests:
		return "요청 한도를 초과했습니다"
	case fiber.StatusInternalServerError:
		return "서버 내부 오류가 발생했습니다"
	case fiber.StatusBadGateway:
		return "게이트웨이 오류입니다"
	case fiber.StatusServiceUnavailable:
		return "서비스를 일시적으로 사용할 수 없습니다"
	case fiber.StatusGatewayTimeout:
		return "게이트웨이 시간 초과입니다"
	default:
		return "예상치 못한 오류가 발생했습니다"
	}
}
