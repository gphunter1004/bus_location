// internal/models/api2.go - API2 관련 모델들
package models

import (
	"encoding/json"
	"fmt"
	"strings"
)

// API2Response 공공데이터포털 버스위치정보 API 응답 구조체
type API2Response struct {
	Response API2ResponseWrapper `json:"response"`
}

// API2ResponseWrapper API2 response 래퍼
type API2ResponseWrapper struct {
	Header API2Header `json:"header"`
	Body   API2Body   `json:"body"`
}

// API2Header API2 헤더
type API2Header struct {
	ResultCode string `json:"resultCode"`
	ResultMsg  string `json:"resultMsg"`
}

// API2Body API2 바디 (빈 응답 처리 개선)
type API2Body struct {
	Items      API2Items `json:"items"`
	NumOfRows  int       `json:"numOfRows"`
	PageNo     int       `json:"pageNo"`
	TotalCount int       `json:"totalCount"`
}

// API2Items API2 아이템들 - 단일 객체와 배열 모두 처리하는 커스텀 언마샬링
type API2Items struct {
	Item []API2BusLocationItem `json:"item"`
}

// UnmarshalJSON API2Items에 대한 커스텀 JSON 언마샬링 (단일/배열 처리)
func (items *API2Items) UnmarshalJSON(data []byte) error {
	// 빈 문자열이나 null인 경우 빈 배열로 처리
	if string(data) == `""` || string(data) == `null` {
		items.Item = []API2BusLocationItem{}
		return nil
	}

	// 임시 구조체로 먼저 파싱 시도
	var temp struct {
		Item json.RawMessage `json:"item"`
	}

	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}

	// item이 없는 경우
	if len(temp.Item) == 0 {
		items.Item = []API2BusLocationItem{}
		return nil
	}

	// item이 배열인지 단일 객체인지 확인
	var itemData []byte = temp.Item

	// 첫 번째 문자가 '['이면 배열, '{'이면 단일 객체
	trimmed := strings.TrimSpace(string(itemData))
	if len(trimmed) == 0 {
		items.Item = []API2BusLocationItem{}
		return nil
	}

	if trimmed[0] == '[' {
		// 배열인 경우
		var itemArray []API2BusLocationItem
		if err := json.Unmarshal(itemData, &itemArray); err != nil {
			return fmt.Errorf("배열 파싱 실패: %v", err)
		}
		items.Item = itemArray
	} else if trimmed[0] == '{' {
		// 단일 객체인 경우
		var singleItem API2BusLocationItem
		if err := json.Unmarshal(itemData, &singleItem); err != nil {
			return fmt.Errorf("단일 객체 파싱 실패: %v", err)
		}
		items.Item = []API2BusLocationItem{singleItem}
	} else {
		// 예상치 못한 형식
		return fmt.Errorf("예상치 못한 item 형식: %s", trimmed[:min(50, len(trimmed))])
	}

	return nil
}

// API2BusLocationItem API2의 실제 버스 위치 정보
type API2BusLocationItem struct {
	// 위치 정보
	GpsLati float64 `json:"gpslati"` // 위도
	GpsLong float64 `json:"gpslong"` // 경도
	// 정류장 정보 (선택적 - 정류장에 있는 버스만)
	NodeId  string `json:"nodeid,omitempty"` // 정류장ID (예: "GGB233000979")
	NodeNm  string `json:"nodenm,omitempty"` // 정류장명 (예: "포스코더샵.롯데캐슬")
	NodeOrd int    `json:"nodeord"`          // 정류장순서
	// 노선 정보 - routeNm은 실제로 숫자로 온다
	RouteNm int    `json:"routenm"`           // 노선번호 (예: 6003) - 숫자로 변경
	RouteId string `json:"routeid,omitempty"` // 노선ID (예: "GGB233000266") - API2에서는 RouteId 필드 사용
	RouteTp string `json:"routetp,omitempty"` // 노선유형 (예: "직행좌석버스")
	// 차량 정보
	VehicleNo string `json:"vehicleno"` // 차량번호 (예: "경기76아4432")
	Timestamp string `json:"@timestamp,omitempty"`
}

// IsSuccess API2 응답이 성공인지 확인
func (ar *API2Response) IsSuccess() bool {
	return ar.Response.Header.ResultCode == "00"
}

// GetErrorMessage API2 오류 메시지 반환
func (ar *API2Response) GetErrorMessage() string {
	if ar.IsSuccess() {
		return ""
	}
	return ar.Response.Header.ResultMsg
}

// GetBusLocationItemList API2 버스 위치 아이템 리스트 반환
func (ar *API2Response) GetBusLocationItemList() []API2BusLocationItem {
	return ar.Response.Body.Items.Item
}

// ConvertToBusLocation API2BusLocationItem을 BusLocation으로 변환
func (item *API2BusLocationItem) ConvertToBusLocation() BusLocation {
	// RouteNm을 숫자에서 문자열로 변환
	routeNm := fmt.Sprintf("%d", item.RouteNm)

	// API2의 routeId는 "GGB233000266" 형식이므로 숫자 부분 추출
	var routeId int64 = 0
	if item.RouteId != "" && len(item.RouteId) > 3 {
		if id, err := ParseRouteID(item.RouteId[3:]); err == nil {
			routeId = id
		}
	}

	// StationId 생성 - NodeId에서 GGB 제거한 값 사용
	var stationId int64 = 0
	if item.NodeId != "" && len(item.NodeId) > 3 {
		// NodeId에서 "GGB" 제거하고 숫자 부분을 stationId로 사용
		if nodeIdInt, err := ParseRouteID(item.NodeId[3:]); err == nil {
			stationId = nodeIdInt
		}
	}
	// NodeId가 없으면 StationId는 0으로 유지 (캐시 조회는 NodeOrd로 함)

	nodeOrd := item.NodeOrd
	if nodeOrd == 0 {
		nodeOrd = 1 // 기본값
	}

	return BusLocation{
		// API2 실제 데이터 매핑
		PlateNo:    item.VehicleNo, // 차량번호
		RouteId:    routeId,        // 숫자형 노선ID (API1 호환용)
		RouteNm:    routeNm,        // 문자열 노선번호 (API2 원본을 문자열로 변환)
		StationId:  stationId,      // NodeId에서 GGB 제거한 값 또는 NodeOrd
		StationSeq: nodeOrd,        // 정류장 순서
		NodeId:     item.NodeId,    // 정류장ID (API2 원본)
		NodeNm:     item.NodeNm,    // 정류장명 (API2 원본)
		NodeOrd:    nodeOrd,        // 정류장순서
		GpsLati:    item.GpsLati,   // 위도
		GpsLong:    item.GpsLong,   // 경도
		// 기본값 설정 (API2에서 제공되지 않는 필드)
		Crowded:       0,
		LowPlate:      0,
		RemainSeatCnt: 0,
		RouteTypeCd:   0,
		StateCd:       0,
		TaglessCd:     0,
		VehId:         0,
		NodeNo:        0,
		TotalStations: 0,
		TripNumber:    0, // 버스 트래커에서 설정
	}
}
