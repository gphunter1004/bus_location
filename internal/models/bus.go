package models

import (
	"encoding/json"
	"strconv"
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

// API2Body API2 바디 (빈 응답 처리 개선)
type API2Body struct {
	Items      API2Items `json:"items"`
	NumOfRows  int       `json:"numOfRows"`
	PageNo     int       `json:"pageNo"`
	TotalCount int       `json:"totalCount"`
}

// API2Items API2 아이템들 - 빈 응답 처리를 위한 커스텀 언마샬링
type API2Items struct {
	Item []API2BusLocationItem `json:"item"`
}

// UnmarshalJSON API2Items에 대한 커스텀 JSON 언마샬링
func (items *API2Items) UnmarshalJSON(data []byte) error {
	// 빈 문자열인 경우 빈 배열로 처리
	if string(data) == `""` || string(data) == `null` {
		items.Item = []API2BusLocationItem{}
		return nil
	}

	// 정상적인 객체인 경우 일반 언마샬링
	type Alias API2Items
	return json.Unmarshal(data, (*Alias)(items))
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
	// 노선 정보
	RouteNm int    `json:"routenm"` // 노선번호 (예: 6003)
	RouteTp string `json:"routetp"` // 노선유형 (예: "직행좌석버스")
	// 차량 정보
	VehicleNo string `json:"vehicleno"` // 차량번호 (예: "경기76아4432")
	Timestamp string `json:"@timestamp,omitempty"`
}

// BusLocation 공통 버스 위치 정보 구조체
type BusLocation struct {
	Crowded       int    `json:"crowded"`  // 차내혼잡도 (1:여유, 2:보통, 3:혼잡, 4:매우혼잡) 차내혼잡도 제공노선유형 (13:일반형시내버스, 15:따복형시내버스, 23:일반형농어촌버스)
	LowPlate      int    `json:"lowPlate"` // 특수차량여부 (0: 일반버스, 1: 저상버스, 2: 2층버스, 5: 전세버스, 6: 예약버스, 7: 트롤리)
	PlateNo       string `json:"plateNo"`
	RemainSeatCnt int    `json:"remainSeatCnt"` // 차내빈자리수 (-1:정보없음, 0~:빈자리 수) 차내빈자리수 제공노선유형 (11: 직행좌석형시내버스, 12:좌석형시내버스, 14: 광역급행형시내버스, 16: 경기순환버스, 17: 준공영제직행좌석시내버스, 21: 직행좌석형농어촌버스, 22: 좌석형농어촌버스)
	RouteId       int64  `json:"routeId"`
	RouteTypeCd   int    `json:"routeTypeCd"` // 노선유형코드 (11: 직행좌석형시내버스, 12:좌석형시내버스, 13:일반형시내버스, 14: 광역급행형시내버스, 15: 따복형시내버스, 16: 경기순환버스, 21: 직행좌석형농어촌버스, 22: 좌석형농어촌버스, 23:일반형농어촌버스, 30: 마을버스, 41: 고속형시외버스, 42: 좌석형시외버스, 43: 일반형시외버스, 51: 리무진공항버스, 52: 좌석형공항버스, 53: 일반형공항버스)
	StateCd       int    `json:"stateCd"`     // 상태코드 (0:교차로통과, 1:정류소 도착, 2:정류소 출발)
	StationId     int64  `json:"stationId"`
	StationSeq    int    `json:"stationSeq"` // 정류소순번
	TaglessCd     int    `json:"taglessCd"`  // 태그리스 서비스가 제공되는 차량 여부 (0:일반차량, 1:태그리스차량)
	VehId         int64  `json:"vehId"`
	Timestamp     string `json:"@timestamp,omitempty"`
	// API2 전용 필드 (정류장 정보)
	NodeId  string  `json:"nodeId,omitempty"`
	NodeNm  string  `json:"nodeNm,omitempty"`
	NodeNo  int     `json:"nodeNo,omitempty"`
	NodeOrd int     `json:"nodeOrd,omitempty"`
	GpsLati float64 `json:"gpsLati,omitempty"`
	GpsLong float64 `json:"gpsLong,omitempty"`
	// 추가 필드 (전체 정류소 개수)
	TotalStations int `json:"totalStations,omitempty"` // 해당 노선의 전체 정류소 개수
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
// models/bus.go의 ConvertToBusLocation 메서드 수정

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

	// 🔧 NodeOrd가 0인 경우 기본값 설정 로직 추가
	nodeOrd := item.NodeOrd
	if nodeOrd == 0 {
		// NodeId에서 순서 추출 시도
		if item.NodeId != "" {
			// 간단한 해시 기반 순서 생성 (임시 해결책)
			hash := 0
			for _, c := range item.NodeId {
				hash = hash*31 + int(c)
			}
			nodeOrd = (hash % 60) + 1 // 1-60 범위로 제한
		} else {
			nodeOrd = 1 // 기본값
		}
	}

	return BusLocation{
		// API2 실제 데이터 매핑
		PlateNo:    item.VehicleNo, // 차량번호
		RouteId:    routeId,        // 노선번호
		StationId:  stationId,      // 정류장ID
		StationSeq: nodeOrd,        // 🔧 수정된 NodeOrd 사용
		// API2 전용 필드 (API 응답에 이미 포함된 정보 사용)
		NodeId:  item.NodeId,  // 정류장ID (API 응답에서 가져옴)
		NodeNm:  item.NodeNm,  // 정류장명 (API 응답에서 가져옴)
		NodeOrd: nodeOrd,      // 🔧 수정된 NodeOrd 사용
		GpsLati: item.GpsLati, // 위도
		GpsLong: item.GpsLong, // 경도
		// 기본값 설정 (API2에서 제공되지 않는 필드)
		Crowded:       0,
		LowPlate:      0,
		RemainSeatCnt: 0,
		RouteTypeCd:   0,
		StateCd:       0,
		TaglessCd:     0,
		VehId:         0, // 차량번호를 해시해서 생성할 수도 있음
		NodeNo:        0, // 캐시에서 채워짐 (필요시)
		TotalStations: 0, // 캐시에서 설정됨
	}
}
