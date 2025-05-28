package models

import (
	"strconv"
)

// API1Response 경기버스정보 API 응답 구조체
type API1Response struct {
	Response API1ResponseWrapper `json:"response"`
}

// API1ResponseWrapper API1 response 래퍼 구조체
type API1ResponseWrapper struct {
	ComMsgHeader string       `json:"comMsgHeader"`
	MsgHeader    API1Header   `json:"msgHeader"`
	MsgBody      API1Body     `json:"msgBody"`
}

// API1Header API1 응답 헤더
type API1Header struct {
	QueryTime     string `json:"queryTime"`
	ResultCode    int    `json:"resultCode"`
	ResultMessage string `json:"resultMessage"`
}

// API1Body API1 응답의 msgBody 구조체
type API1Body struct {
	BusLocationList []BusLocation `json:"busLocationList"`
}

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

// API2Body API2 바디
type API2Body struct {
	Items      API2Items `json:"items"`
	NumOfRows  int       `json:"numOfRows"`
	PageNo     int       `json:"pageNo"`
	TotalCount int       `json:"totalCount"`
}

// API2Items API2 아이템들 - 실제 버스 위치 데이터
type API2Items struct {
	Item []API2BusLocationItem `json:"item"`
}

// API2BusLocationItem API2의 실제 버스 위치 정보
type API2BusLocationItem struct {
	// 위치 정보
	GpsLati   float64 `json:"gpslati"`   // 위도
	GpsLong   float64 `json:"gpslong"`   // 경도
	// 정류장 정보 (선택적 - 정류장에 있는 버스만)
	NodeId    string  `json:"nodeid,omitempty"`    // 정류장ID (예: "GGB233000979")
	NodeNm    string  `json:"nodenm,omitempty"`    // 정류장명 (예: "포스코더샵.롯데캐슬")
	NodeOrd   int     `json:"nodeord"`             // 정류장순서
	// 노선 정보
	RouteNm   int     `json:"routenm"`   // 노선번호 (예: 6003)
	RouteTp   string  `json:"routetp"`   // 노선유형 (예: "직행좌석버스")
	// 차량 정보
	VehicleNo string  `json:"vehicleno"` // 차량번호 (예: "경기76아4432")
	Timestamp string  `json:"@timestamp,omitempty"`
}

// BusLocation 공통 버스 위치 정보 구조체
type BusLocation struct {
	Crowded        int    `json:"crowded"`
	LowPlate       int    `json:"lowPlate"`
	PlateNo        string `json:"plateNo"`
	RemainSeatCnt  int    `json:"remainSeatCnt"`
	RouteId        int64  `json:"routeId"`
	RouteTypeCd    int    `json:"routeTypeCd"`
	StateCd        int    `json:"stateCd"`
	StationId      int64  `json:"stationId"`
	StationSeq     int    `json:"stationSeq"`
	TaglessCd      int    `json:"taglessCd"`
	VehId          int64  `json:"vehId"`
	Timestamp      string `json:"@timestamp,omitempty"`
	// API2 전용 필드 (정류장 정보)
	NodeId    string  `json:"nodeId,omitempty"`
	NodeNm    string  `json:"nodeNm,omitempty"`
	NodeNo    int     `json:"nodeNo,omitempty"`
	NodeOrd   int     `json:"nodeOrd,omitempty"`
	GpsLati   float64 `json:"gpsLati,omitempty"`
	GpsLong   float64 `json:"gpsLong,omitempty"`
}

// BulkResponse Elasticsearch 벌크 응답 구조체
type BulkResponse struct {
	Took   int64 `json:"took"`
	Errors bool  `json:"errors"`
	Items  []struct {
		Index struct {
			Index   string `json:"_index"`
			ID      string `json:"_id"`
			Version int    `json:"_version"`
			Result  string `json:"result"`
			Status  int    `json:"status"`
			Error   *struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			} `json:"error,omitempty"`
		} `json:"index"`
	} `json:"items"`
}

// ParseRouteID 문자열 routeId를 int64로 변환
func ParseRouteID(routeID string) (int64, error) {
	return strconv.ParseInt(routeID, 10, 64)
}

// GetRouteIDString int64 routeId를 문자열로 변환
func (bl *BusLocation) GetRouteIDString() string {
	return strconv.FormatInt(bl.RouteId, 10)
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
	return ar.Response.MsgBody.BusLocationList
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
	// RouteId를 routenm에서 가져오기 (int를 int64로 변환)
	routeId := int64(item.RouteNm)
	
	// StationId 생성 - NodeId가 있으면 사용, 없으면 NodeOrd 기반으로 생성
	stationId := int64(item.NodeOrd) // 기본적으로 순서 사용
	if item.NodeId != "" && len(item.NodeId) > 3 {
		// NodeId에서 숫자 부분 추출 (예: "GGB233000979" -> 233000979)
		if nodeIdInt, err := ParseRouteID(item.NodeId[3:]); err == nil {
			stationId = nodeIdInt
		}
	}
	
	return BusLocation{
		// API2 실제 데이터 매핑
		PlateNo:       item.VehicleNo,  // 차량번호
		RouteId:       routeId,         // 노선번호
		StationId:     stationId,       // 정류장ID
		StationSeq:    item.NodeOrd,    // 정류장순서
		// API2 전용 필드 (추가 정보)
		NodeId:        item.NodeId,     // 정류장ID (원본)
		NodeNm:        item.NodeNm,     // 정류장명
		NodeOrd:       item.NodeOrd,    // 정류장순서
		GpsLati:       item.GpsLati,    // 위도
		GpsLong:       item.GpsLong,    // 경도
		// 기본값 설정 (API2에서 제공되지 않는 필드)
		Crowded:       0,
		LowPlate:      0,
		RemainSeatCnt: 0,
		RouteTypeCd:   0,
		StateCd:       0,
		TaglessCd:     0,
		VehId:         0, // 차량번호를 해시해서 생성할 수도 있음
		NodeNo:        0,
	}
}