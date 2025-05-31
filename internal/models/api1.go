// internal/models/api1.go - API1 관련 모델들
package models

import (
	"encoding/json"
	"fmt"
	"strings"
)

// API1Response 경기버스정보 API 응답 구조체
type API1Response struct {
	Response API1ResponseWrapper `json:"response"`
}

// API1ResponseWrapper API1 response 래퍼 구조체
type API1ResponseWrapper struct {
	ComMsgHeader string     `json:"comMsgHeader"`
	MsgHeader    API1Header `json:"msgHeader"`
	MsgBody      API1Body   `json:"msgBody"`
}

// API1Header API1 응답 헤더
type API1Header struct {
	QueryTime     string `json:"queryTime"`
	ResultCode    int    `json:"resultCode"`
	ResultMessage string `json:"resultMessage"`
}

// API1Body API1 응답의 msgBody 구조체 - 단일/배열 처리 개선
type API1Body struct {
	BusLocationList API1BusLocationList `json:"busLocationList"`
}

// API1BusLocationList API1 버스 위치 리스트 - 단일 객체와 배열 모두 처리
type API1BusLocationList struct {
	Items []BusLocation `json:"-"` // 실제 데이터는 여기에 저장
}

// UnmarshalJSON API1BusLocationList에 대한 커스텀 JSON 언마샬링
func (list *API1BusLocationList) UnmarshalJSON(data []byte) error {
	// 빈 문자열이나 null인 경우 빈 배열로 처리
	if string(data) == `""` || string(data) == `null` {
		list.Items = []BusLocation{}
		return nil
	}

	trimmed := strings.TrimSpace(string(data))
	if len(trimmed) == 0 {
		list.Items = []BusLocation{}
		return nil
	}

	// 첫 번째 문자가 '['이면 배열, '{'이면 단일 객체
	if trimmed[0] == '[' {
		// 배열인 경우
		var itemArray []BusLocation
		if err := json.Unmarshal(data, &itemArray); err != nil {
			return fmt.Errorf("API1 배열 파싱 실패: %v", err)
		}
		list.Items = itemArray
	} else if trimmed[0] == '{' {
		// 단일 객체인 경우
		var singleItem BusLocation
		if err := json.Unmarshal(data, &singleItem); err != nil {
			return fmt.Errorf("API1 단일 객체 파싱 실패: %v", err)
		}
		list.Items = []BusLocation{singleItem}
	} else {
		// 예상치 못한 형식
		return fmt.Errorf("API1 예상치 못한 busLocationList 형식: %s", trimmed[:min(50, len(trimmed))])
	}

	return nil
}

// IsSuccess API1 응답이 성공인지 확인
func (ar *API1Response) IsSuccess() bool {
	return ar.Response.MsgHeader.ResultCode == 0
}

// GetErrorMessage API1 오류 메시지 반환
func (ar *API1Response) GetErrorMessage() string {
	if ar.IsSuccess() {
		return ""
	}
	return ar.Response.MsgHeader.ResultMessage
}

// GetBusLocationList API1 버스 위치 리스트 반환
func (ar *API1Response) GetBusLocationList() []BusLocation {
	return ar.Response.MsgBody.BusLocationList.Items
}
