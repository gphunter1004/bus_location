// internal/services/storage/duplicate_checker.go
package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"bus-tracker/internal/utils"
)

// BusLastData 버스의 마지막 데이터 (중복 체크용)
type BusLastData struct {
	PlateNo    string    `json:"plateNo"`
	StationSeq int       `json:"stationSeq"`
	NodeOrd    int       `json:"nodeOrd"`
	StationId  int64     `json:"stationId"`
	NodeId     string    `json:"nodeId"`
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

// GetRecentBusData 최근 버스 데이터 조회 (중복 체크용)
func (edc *ElasticsearchDuplicateChecker) GetRecentBusData(lookbackMinutes int) (map[string]*BusLastData, error) {
	if lookbackMinutes <= 0 {
		lookbackMinutes = 30 // 기본값: 30분
	}

	// Elasticsearch 쿼리 작성
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
								"plateNo", "stationSeq", "nodeOrd", "stationId", "nodeId", "@timestamp",
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

	edc.logger.Infof("첫 실행 중복 체크 - 조회 범위: 최근 %d분", lookbackMinutes)

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
			StationSeq: getIntFromInterface(source["stationSeq"]),
			NodeOrd:    getIntFromInterface(source["nodeOrd"]),
			StationId:  getInt64FromInterface(source["stationId"]),
			NodeId:     getStringFromInterface(source["nodeId"]),
			LastUpdate: lastUpdate,
		}

		busLastData[plateNo] = busData
	}

	edc.logger.Infof("중복 체크용 데이터 조회 완료 - 조회된 버스: %d대", len(busLastData))

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

// 유틸리티 함수들
func getStringFromInterface(value interface{}) string {
	if str, ok := value.(string); ok {
		return str
	}
	return ""
}

func getIntFromInterface(value interface{}) int {
	switch v := value.(type) {
	case int:
		return v
	case float64:
		return int(v)
	case int64:
		return int(v)
	}
	return 0
}

func getInt64FromInterface(value interface{}) int64 {
	switch v := value.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	}
	return 0
}
