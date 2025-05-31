// internal/web/models/responses/common.go
package responses

import "time"

// BaseResponse 모든 응답의 기본 구조체
type BaseResponse struct {
	Success   bool      `json:"success"`
	Message   string    `json:"message,omitempty"`
	Error     bool      `json:"error,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// ErrorResponse 에러 응답 구조체
type ErrorResponse struct {
	Error   bool   `json:"error"`
	Message string `json:"message"`
	Code    int    `json:"code,omitempty"`
	Details string `json:"details,omitempty"`
}

// PaginatedResponse 페이지네이션이 포함된 응답
type PaginatedResponse struct {
	BaseResponse
	Data       interface{}    `json:"data"`
	Pagination PaginationInfo `json:"pagination"`
}

// PaginationInfo 페이지네이션 정보
type PaginationInfo struct {
	Page       int  `json:"page"`
	PerPage    int  `json:"perPage"`
	Total      int  `json:"total"`
	TotalPages int  `json:"totalPages"`
	HasNext    bool `json:"hasNext"`
	HasPrev    bool `json:"hasPrev"`
}

// DataResponse 데이터가 포함된 응답
type DataResponse struct {
	BaseResponse
	Data interface{} `json:"data"`
}

// ListResponse 리스트 데이터 응답
type ListResponse struct {
	BaseResponse
	Data  interface{} `json:"data"`
	Count int         `json:"count"`
}

// OperationResponse 작업 결과 응답
type OperationResponse struct {
	BaseResponse
	OperationID string      `json:"operationId,omitempty"`
	Result      interface{} `json:"result,omitempty"`
}

// NewSuccessResponse 성공 응답 생성
func NewSuccessResponse(message string) BaseResponse {
	return BaseResponse{
		Success:   true,
		Message:   message,
		Timestamp: time.Now(),
	}
}

// NewErrorResponse 에러 응답 생성
func NewErrorResponse(message string, code int) ErrorResponse {
	return ErrorResponse{
		Error:   true,
		Message: message,
		Code:    code,
	}
}

// NewDataResponse 데이터 응답 생성
func NewDataResponse(data interface{}, message string) DataResponse {
	return DataResponse{
		BaseResponse: BaseResponse{
			Success:   true,
			Message:   message,
			Timestamp: time.Now(),
		},
		Data: data,
	}
}
