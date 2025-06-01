// internal/models/bus_location.go - 명확한 API 분리
package models

import (
	"strconv"
)

// BusLocation 공통 버스 위치 정보 구조체
type BusLocation struct {
	Crowded       int    `json:"crowded"`  // 차내혼잡도 (1:여유, 2:보통, 3:혼잡, 4:매우혼잡) 차내혼잡도 제공노선유형 (13:일반형시내버스, 15:따복형시내버스, 23:일반형농어촌버스)
	LowPlate      int    `json:"lowPlate"` // 특수차량여부 (0: 일반버스, 1: 저상버스, 2: 2층버스, 5: 전세버스, 6: 예약버스, 7: 트롤리)
	PlateNo       string `json:"plateNo"`
	RemainSeatCnt int    `json:"remainSeatCnt"` // 차내빈자리수 (-1:정보없음, 0~:빈자리 수) 차내빈자리수 제공노선유형 (11: 직행좌석형시내버스, 12:좌석형시내버스, 14: 광역급행형시내버스, 16: 경기순환버스, 17: 준공영제직행좌석시내버스, 21: 직행좌석형농어촌버스, 22: 좌석형농어촌버스)
	RouteId       int64  `json:"routeId"`       // API1에서 사용하는 숫자형 노선ID
	RouteNm       string `json:"routeNm"`       // API2에서 사용하는 노선번호 (문자열)
	RouteTypeCd   int    `json:"routeTypeCd"`   // 노선유형코드
	StateCd       int    `json:"stateCd"`       // 상태코드 (0:교차로통과, 1:정류소 도착, 2:정류소 출발)
	StationId     int64  `json:"stationId"`
	StationSeq    int    `json:"stationSeq"` // 정류소순번
	TaglessCd     int    `json:"taglessCd"`  // 태그리스 서비스가 제공되는 차량 여부 (0:일반차량, 1:태그리스차량)
	VehId         int64  `json:"vehId"`
	Timestamp     string `json:"@timestamp,omitempty"`
	// API2 전용 필드 (정류장 정보)
	NodeId        string  `json:"nodeId,omitempty"`
	NodeNm        string  `json:"nodeNm,omitempty"`
	NodeNo        int     `json:"nodeNo,omitempty"`
	NodeOrd       int     `json:"nodeOrd,omitempty"`
	GpsLati       float64 `json:"gpsLati,omitempty"`
	GpsLong       float64 `json:"gpsLong,omitempty"`
	TotalStations int     `json:"totalStations,omitempty"` // 해당 노선의 전체 정류소 개수
	// 운행 차수 추가
	TripNumber int `json:"tripNumber,omitempty"` // 운행 차수 (운영시간 내 시작 기준)
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

// GetCacheKey 캐시 조회용 RouteId 문자열 반환
func (bl *BusLocation) GetCacheKey() string {
	return strconv.FormatInt(bl.RouteId, 10)
}

// GetStationOrder 정류장 순서 반환
func (bl *BusLocation) GetStationOrder() int {
	if bl.NodeOrd > 0 {
		return bl.NodeOrd // API2
	}
	return bl.StationSeq // API1
}

// IsAPI2Data API2 데이터인지 확인
func (bl *BusLocation) IsAPI2Data() bool {
	return bl.NodeId != ""
}

// IsAPI1Data API1 데이터인지 확인
func (bl *BusLocation) IsAPI1Data() bool {
	return bl.VehId > 0 && bl.NodeId == ""
}

// min 함수 추가
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
