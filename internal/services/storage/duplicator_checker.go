// internal/services/storage/duplicate_checker.go - 완전한 구현
package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"bus-tracker/internal/utils"
)

// BusLastData 버스의 마지막 데이터 (중복 체크용 + 운행 차수 포함)
type BusLastData struct {
	PlateNo    string    `json:"plateNo"`
	StationSeq int       `json:"stationSeq"`
	NodeOrd    int       `json:"nodeOrd"`
	StationId  int64     `json:"stationId"`
	NodeId     string    `json:"nodeId"`
	TripNumber int       `json:"tripNumber"` // 🆕 운행 차수
	RouteId    int64     `json:"routeId"`    // 🆕 노선 ID
	LastUpdate time.Time `json:"lastUpdate"`
}

// ElasticsearchDuplicateChecker Elasticsearch에서 중복 체크하는 서비스
type ElasticsearchDuplicateChecker struct {
	esService *ElasticsearchService
	logger    *utils.Logger
	indexName string
}

// NewElasticsearchDuplicateChecker 생성자
func NewElasticsearchDuplicateChecker(esService *ElasticsearchService, logger *utils.Logger, indexName string) *ElasticsearchDuplicateChecker {
	return &ElasticsearchDuplicateChecker{
		esService: esService,
		logger:    logger,
		indexName: indexName,
	}
}

// GetRecentBusData 최근 버스 데이터 조회 (중복 체크용 + 운행 차수 포함) - 최초 실행시에만
func (edc *ElasticsearchDuplicateChecker) GetRecentBusData(lookbackMinutes int) (map[string]*BusLastData, error) {
	if lookbackMinutes <= 0 {
		lookbackMinutes = 30 // 기본값: 30분
	}

	// Elasticsearch 쿼리 작성 - 운행 차수와 노선 정보 포함
	query := map[string]interface{}{
		"size": 0, // 집계 결과만 필요
		"query": map[string]interface{}{
			"range": map[string]interface{}{
				"@timestamp": map[string]interface{}{
					"gte": fmt.Sprintf("now-%dm", lookbackMinutes),
				},
			},
		},
		"aggs": map[string]interface{}{
			"buses": map[string]interface{}{
				"terms": map[string]interface{}{
					"field": "plateNo.keyword",
					"size":  1000, // 최대 1000대 버스
				},
				"aggs": map[string]interface{}{
					"latest": map[string]interface{}{
						"top_hits": map[string]interface{}{
							"sort": []map[string]interface{}{
								{
									"@timestamp": map[string]interface{}{
										"order": "desc",
									},
								},
							},
							"size": 1, // 각 버스당 최신 1개 문서만
							"_source": []string{
								"plateNo", "stationSeq", "nodeOrd", "stationId", "nodeId",
								"tripNumber", "routeId", "@timestamp", // 🆕 운행 차수, 노선 정보 추가
							},
						},
					},
				},
			},
		},
	}

	queryBytes, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("쿼리 마샬링 실패: %v", err)
	}

	edc.logger.Infof("최초 실행 - 중복 체크 + 운행 차수 조회 (이후 캐시 운영) - 조회 범위: 최근 %d분", lookbackMinutes)

	// Elasticsearch에 쿼리 실행
	res, err := edc.esService.client.Search(
		edc.esService.client.Search.WithContext(context.Background()),
		edc.esService.client.Search.WithIndex(edc.indexName),
		edc.esService.client.Search.WithBody(strings.NewReader(string(queryBytes))),
		edc.esService.client.Search.WithTrackTotalHits(true),
	)
	if err != nil {
		return nil, fmt.Errorf("Elasticsearch 쿼리 실행 실패: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("Elasticsearch 쿼리 오류 [%s]: %s", res.Status(), string(body))
	}

	// 응답 파싱
	var searchResponse SearchResponse
	if err := json.NewDecoder(res.Body).Decode(&searchResponse); err != nil {
		return nil, fmt.Errorf("응답 파싱 실패: %v", err)
	}

	// 결과 변환
	busLastData := make(map[string]*BusLastData)
	for _, bucket := range searchResponse.Aggregations.Buses.Buckets {
		plateNo := bucket.Key

		if len(bucket.Latest.Hits.Hits) == 0 {
			continue
		}

		hit := bucket.Latest.Hits.Hits[0]
		source := hit.Source

		// timestamp 파싱
		var lastUpdate time.Time
		if timestampStr, ok := source["@timestamp"].(string); ok {
			if parsed, err := time.Parse(time.RFC3339, timestampStr); err == nil {
				lastUpdate = parsed
			}
		}

		// 각 필드 안전하게 추출
		busData := &BusLastData{
			PlateNo:    plateNo,
			StationSeq: safeGetInt(source["stationSeq"]),
			NodeOrd:    safeGetInt(source["nodeOrd"]),
			StationId:  safeGetInt64(source["stationId"]),
			NodeId:     safeGetString(source["nodeId"]),
			TripNumber: safeGetInt(source["tripNumber"]), // 🆕 운행 차수
			RouteId:    safeGetInt64(source["routeId"]),  // 🆕 노선 ID
			LastUpdate: lastUpdate,
		}

		busLastData[plateNo] = busData
	}

	edc.logger.Infof("최초 실행 - 중복 체크 + 운행 차수 데이터 조회 완료 - %d대 버스 (이후 캐시 기반 운영)", len(busLastData))

	// 🔍 조회된 각 버스 데이터를 개별 로그로 출력
	if len(busLastData) > 0 {
		edc.logger.Info("📋 조회된 버스 목록:")
		for plateNo, data := range busLastData {
			edc.logger.Infof("   🚌 차량: %s | 위치: StationSeq=%d, NodeOrd=%d, StationId=%d, NodeId=%s | 운행차수: T%d | 노선ID: %d | 시간: %s",
				plateNo,
				data.StationSeq,
				data.NodeOrd,
				data.StationId,
				data.NodeId,
				data.TripNumber,
				data.RouteId,
				data.LastUpdate.Format("15:04:05"))
		}
	}

	return busLastData, nil
}

// IsDuplicateData 중복 데이터인지 확인
func (bld *BusLastData) IsDuplicateData(newStationSeq, newNodeOrd int, newStationId int64, newNodeId string) bool {
	// NodeOrd 우선 비교 (API2)
	if newNodeOrd > 0 && bld.NodeOrd > 0 {
		return bld.NodeOrd == newNodeOrd
	}

	// StationSeq 비교 (API1)
	if newStationSeq > 0 && bld.StationSeq > 0 {
		return bld.StationSeq == newStationSeq
	}

	// NodeId 비교 (문자열)
	if newNodeId != "" && bld.NodeId != "" {
		return bld.NodeId == newNodeId
	}

	// StationId 비교 (fallback)
	if newStationId > 0 && bld.StationId > 0 {
		return bld.StationId == newStationId
	}

	return false
}

// 🆕 헬퍼 함수들 - interface{} 안전 변환
func safeGetInt(value interface{}) int {
	if value == nil {
		return 0
	}
	switch v := value.(type) {
	case int:
		return v
	case float64:
		return int(v)
	case string:
		// 문자열에서 숫자 변환 시도
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return 0
}

func safeGetInt64(value interface{}) int64 {
	if value == nil {
		return 0
	}
	switch v := value.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	case string:
		// 문자열에서 숫자 변환 시도
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i
		}
	}
	return 0
}

func safeGetString(value interface{}) string {
	if value == nil {
		return ""
	}
	if str, ok := value.(string); ok {
		return str
	}
	return ""
}

// Elasticsearch 응답 구조체들
type SearchResponse struct {
	Aggregations struct {
		Buses struct {
			Buckets []struct {
				Key    string `json:"key"`
				Latest struct {
					Hits struct {
						Hits []struct {
							Source map[string]interface{} `json:"_source"`
						} `json:"hits"`
					} `json:"hits"`
				} `json:"latest"`
			} `json:"buckets"`
		} `json:"buses"`
	} `json:"aggregations"`
}
