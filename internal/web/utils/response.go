// internal/web/utils/response.go
package utils

import (
	"fmt"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/gofiber/fiber/v2"

	"bus-tracker/internal/web/models/responses"
)

var validate *validator.Validate

func init() {
	validate = validator.New()
}

// HandleError 에러 응답 처리
func HandleError(c *fiber.Ctx, err error, message string) error {
	// 에러 타입에 따른 상태 코드 결정
	statusCode := fiber.StatusInternalServerError

	// 비즈니스 로직 에러인 경우
	if strings.Contains(err.Error(), "not found") {
		statusCode = fiber.StatusNotFound
	} else if strings.Contains(err.Error(), "validation") {
		statusCode = fiber.StatusBadRequest
	} else if strings.Contains(err.Error(), "unauthorized") {
		statusCode = fiber.StatusUnauthorized
	} else if strings.Contains(err.Error(), "forbidden") {
		statusCode = fiber.StatusForbidden
	}

	response := responses.ErrorResponse{
		Error:   true,
		Message: message,
		Code:    statusCode,
		Details: err.Error(),
	}

	return c.Status(statusCode).JSON(response)
}

// HandleValidationError 검증 에러 응답 처리
func HandleValidationError(c *fiber.Ctx, err error, message string) error {
	var details string

	if err != nil {
		// validator 에러인 경우 상세 메시지 생성
		if validationErrors, ok := err.(validator.ValidationErrors); ok {
			var errorMessages []string
			for _, validationErr := range validationErrors {
				errorMessages = append(errorMessages, formatValidationError(validationErr))
			}
			details = strings.Join(errorMessages, "; ")
		} else {
			details = err.Error()
		}
	}

	response := responses.ErrorResponse{
		Error:   true,
		Message: message,
		Code:    fiber.StatusBadRequest,
		Details: details,
	}

	return c.Status(fiber.StatusBadRequest).JSON(response)
}

// formatValidationError 검증 에러 메시지 포맷팅
func formatValidationError(err validator.FieldError) string {
	switch err.Tag() {
	case "required":
		return fmt.Sprintf("%s 필드는 필수입니다", err.Field())
	case "min":
		return fmt.Sprintf("%s 필드는 최소 %s 이상이어야 합니다", err.Field(), err.Param())
	case "max":
		return fmt.Sprintf("%s 필드는 최대 %s 이하여야 합니다", err.Field(), err.Param())
	case "email":
		return fmt.Sprintf("%s 필드는 유효한 이메일 형식이어야 합니다", err.Field())
	case "oneof":
		return fmt.Sprintf("%s 필드는 다음 값 중 하나여야 합니다: %s", err.Field(), err.Param())
	default:
		return fmt.Sprintf("%s 필드가 유효하지 않습니다", err.Field())
	}
}

// ValidateStruct 구조체 검증
func ValidateStruct(s interface{}) error {
	return validate.Struct(s)
}

// SendSuccessResponse 성공 응답 전송
func SendSuccessResponse(c *fiber.Ctx, data interface{}, message string) error {
	response := responses.NewDataResponse(data, message)
	return c.JSON(response)
}

// SendListResponse 리스트 응답 전송
func SendListResponse(c *fiber.Ctx, data interface{}, count int, message string) error {
	response := responses.ListResponse{
		BaseResponse: responses.NewSuccessResponse(message),
		Data:         data,
		Count:        count,
	}
	return c.JSON(response)
}

// SendPaginatedResponse 페이지네이션 응답 전송
func SendPaginatedResponse(c *fiber.Ctx, data interface{}, pagination responses.PaginationInfo, message string) error {
	response := responses.PaginatedResponse{
		BaseResponse: responses.NewSuccessResponse(message),
		Data:         data,
		Pagination:   pagination,
	}
	return c.JSON(response)
}

// SendOperationResponse 작업 결과 응답 전송
func SendOperationResponse(c *fiber.Ctx, operationID string, result interface{}, message string) error {
	response := responses.OperationResponse{
		BaseResponse: responses.NewSuccessResponse(message),
		OperationID:  operationID,
		Result:       result,
	}
	return c.JSON(response)
}

// ParsePagination 페이지네이션 파라미터 파싱
func ParsePagination(c *fiber.Ctx, defaultLimit int) (page, limit, offset int) {
	page = c.QueryInt("page", 1)
	limit = c.QueryInt("limit", defaultLimit)

	if page < 1 {
		page = 1
	}
	if limit < 1 {
		limit = defaultLimit
	}
	if limit > 100 {
		limit = 100 // 최대 100개로 제한
	}

	offset = (page - 1) * limit
	return page, limit, offset
}

// CreatePaginationInfo 페이지네이션 정보 생성
func CreatePaginationInfo(page, perPage, total int) responses.PaginationInfo {
	totalPages := (total + perPage - 1) / perPage
	if totalPages < 1 {
		totalPages = 1
	}

	return responses.PaginationInfo{
		Page:       page,
		PerPage:    perPage,
		Total:      total,
		TotalPages: totalPages,
		HasNext:    page < totalPages,
		HasPrev:    page > 1,
	}
}

// ExtractUserContext 사용자 컨텍스트 추출 (인증 미들웨어에서 설정)
func ExtractUserContext(c *fiber.Ctx) (userID string, userName string, ok bool) {
	userID, ok1 := c.Locals("user_id").(string)
	userName, ok2 := c.Locals("user_name").(string)
	return userID, userName, ok1 && ok2
}

// SetResponseHeaders 공통 응답 헤더 설정
func SetResponseHeaders(c *fiber.Ctx) {
	c.Set("X-Content-Type-Options", "nosniff")
	c.Set("X-Frame-Options", "DENY")
	c.Set("X-XSS-Protection", "1; mode=block")
	c.Set("Cache-Control", "no-cache, no-store, must-revalidate")
}
