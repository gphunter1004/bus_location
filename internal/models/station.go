package models

import "fmt"

// API2용 정류소 정보 구조체들
// StationInfoResponse 정류소 정보 API 응답 구조체 (API2용)
type StationInfoResponse struct {
	Response StationInfoResponseWrapper `json:"response"`
}

// StationInfoResponseWrapper 정류소 정보 응답 래퍼 (API2용)
type StationInfoResponseWrapper struct {
	Header StationInfoHeader `json:"header"`
	Body   StationInfoBody   `json:"body"`
}

// StationInfoHeader 정류소 정보 헤더 (API2용)
type StationInfoHeader struct {
	ResultCode string `json:"resultCode"`
	ResultMsg  string `json:"resultMsg"`
}

// StationInfoBody 정류소 정보 바디 (API2용)
type StationInfoBody struct {
	Items      StationInfoItems `json:"items"`
	NumOfRows  int              `json:"numOfRows"`
	PageNo     int              `json:"pageNo"`
	TotalCount int              `json:"totalCount"`
}

// StationInfoItems 정류소 정보 아이템들 (API2용)
type StationInfoItems struct {
	Item []StationInfoItem `json:"item"`
}

// StationInfoItem 개별 정류소 정보 (API2용)
type StationInfoItem struct {
	NodeId  string `json:"nodeid"`  // 정류장ID (예: "GGB233000979")
	NodeNm  string `json:"nodenm"`  // 정류장명 (예: "포스코더샵.롯데캐슬")
	NodeNo  int    `json:"nodeno"`  // 정류장번호 (숫자)
	NodeOrd int    `json:"nodeord"` // 정류장순서 (1, 2, 3...)
	RouteId string `json:"routeid"` // 노선ID (예: "GGB233000266")
	// GPS 정보 - API에서 숫자로 제공됨
	GPSLat  float64 `json:"gpslati"` // 위도 (숫자)
	GPSLong float64 `json:"gpslong"` // 경도 (숫자)
	// 추가 정보들 (있는 경우)
	UpNodeId string `json:"upnodeid,omitempty"` // 상행 정류장ID
	UpNodeNm string `json:"upnodenm,omitempty"` // 상행 정류장명
}

// API1용 정류소 정보 구조체들
// API1StationInfoResponse API1 정류소 정보 API 응답 구조체
type API1StationInfoResponse struct {
	Response API1StationInfoResponseWrapper `json:"response"`
}

// API1StationInfoResponseWrapper API1 정류소 정보 응답 래퍼
type API1StationInfoResponseWrapper struct {
	ComMsgHeader string                `json:"comMsgHeader"`
	MsgHeader    API1StationInfoHeader `json:"msgHeader"`
	MsgBody      API1StationInfoBody   `json:"msgBody"`
}

// API1StationInfoHeader API1 정류소 정보 헤더
type API1StationInfoHeader struct {
	QueryTime     string `json:"queryTime"`
	ResultCode    int    `json:"resultCode"`
	ResultMessage string `json:"resultMessage"`
}

// API1StationInfoBody API1 정류소 정보 바디
type API1StationInfoBody struct {
	BusRouteStationList []API1StationInfoItem `json:"busRouteStationList"`
}

// API1StationInfoItem API1 개별 정류소 정보
type API1StationInfoItem struct {
	CenterYn    string  `json:"centerYn"`    // 중앙차로 여부
	DistrictCd  int     `json:"districtCd"`  // 관할지역코드
	MobileNo    string  `json:"mobileNo"`    // 정류장번호
	RegionName  string  `json:"regionName"`  // 지역명
	RouteId     int64   `json:"routeId"`     // 노선ID
	StationId   int64   `json:"stationId"`   // 정류장ID
	StationName string  `json:"stationName"` // 정류장명
	StationSeq  int     `json:"stationSeq"`  // 정류장순서
	TurnYn      string  `json:"turnYn"`      // 회차지 여부
	X           float64 `json:"x"`           // X좌표
	Y           float64 `json:"y"`           // Y좌표
}

// 공통 캐시 구조체
// StationCache 정류소 캐시 정보 (StationId 기반)
type StationCache struct {
	StationId int64   `json:"stationId"` // 정류장ID (숫자) - 캐시 키로 사용
	NodeId    string  `json:"nodeId"`    // 정류장ID (문자열)
	NodeNm    string  `json:"nodeNm"`    // 정류장명
	NodeNo    int     `json:"nodeNo"`    // 정류장번호 (숫자)
	NodeOrd   int     `json:"nodeOrd"`   // 정류장순서
	GPSLat    float64 `json:"gpsLat"`    // 위도 (숫자)
	GPSLong   float64 `json:"gpsLong"`   // 경도 (숫자)
}

// API2용 메서드들
// IsSuccess 정류소 정보 API 응답이 성공인지 확인 (API2)
func (sr *StationInfoResponse) IsSuccess() bool {
	return sr.Response.Header.ResultCode == "00"
}

// GetErrorMessage 정류소 정보 API 오류 메시지 반환 (API2)
func (sr *StationInfoResponse) GetErrorMessage() string {
	if sr.IsSuccess() {
		return ""
	}
	return sr.Response.Header.ResultMsg
}

// GetStationInfoList 정류소 정보 리스트 반환 (API2)
func (sr *StationInfoResponse) GetStationInfoList() []StationInfoItem {
	return sr.Response.Body.Items.Item
}

// ToStationCache StationInfoItem을 StationCache로 변환 (API2)
func (si *StationInfoItem) ToStationCache() StationCache {
	// NodeId에서 GGB 제거하고 StationId 생성
	var stationId int64
	if si.NodeId != "" && len(si.NodeId) > 3 {
		if id, err := ParseRouteID(si.NodeId[3:]); err == nil {
			stationId = id
		}
	}

	return StationCache{
		StationId: stationId,  // NodeId에서 추출한 숫자
		NodeId:    si.NodeId,  // 원본 NodeId
		NodeNm:    si.NodeNm,  // 정류장명
		NodeNo:    si.NodeNo,  // 정류장번호
		NodeOrd:   si.NodeOrd, // 정류장순서
		GPSLat:    si.GPSLat,  // 위도
		GPSLong:   si.GPSLong, // 경도
	}
}

// API1용 메서드들
// IsSuccess API1 정류소 정보 API 응답이 성공인지 확인
func (sr *API1StationInfoResponse) IsSuccess() bool {
	return sr.Response.MsgHeader.ResultCode == 0
}

// GetErrorMessage API1 정류소 정보 API 오류 메시지 반환
func (sr *API1StationInfoResponse) GetErrorMessage() string {
	if sr.IsSuccess() {
		return ""
	}
	return sr.Response.MsgHeader.ResultMessage
}

// GetStationInfoList API1 정류소 정보 리스트 반환
func (sr *API1StationInfoResponse) GetStationInfoList() []API1StationInfoItem {
	return sr.Response.MsgBody.BusRouteStationList
}

// ToStationCache API1StationInfoItem을 StationCache로 변환
func (si *API1StationInfoItem) ToStationCache() StationCache {
	return StationCache{
		StationId: si.StationId,                    // API1의 StationId 직접 사용
		NodeId:    fmt.Sprintf("%d", si.StationId), // StationId를 문자열로 변환
		NodeNm:    si.StationName,                  // 정류장명
		NodeNo:    0,                               // API1에서는 제공하지 않음
		NodeOrd:   si.StationSeq,                   // 정류장순서
		GPSLat:    si.Y,                            // Y좌표 (위도)
		GPSLong:   si.X,                            // X좌표 (경도)
	}
}
