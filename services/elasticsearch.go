package services

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"

	"bus-tracker/config"
	"bus-tracker/models"
	"bus-tracker/utils"
)

// ElasticsearchService Elasticsearch 관련 작업을 담당하는 서비스
type ElasticsearchService struct {
	client *elasticsearch.Client
	logger *utils.Logger
}

// NewElasticsearchService 새로운 Elasticsearch 서비스 생성
func NewElasticsearchService(cfg *config.Config, logger *utils.Logger) *ElasticsearchService {
	esConfig := elasticsearch.Config{
		Addresses: []string{cfg.ElasticsearchURL},
	}
	
	// 인증 정보가 있는 경우 추가
	if cfg.ElasticsearchUsername != "" {
		esConfig.Username = cfg.ElasticsearchUsername
		esConfig.Password = cfg.ElasticsearchPassword
	}
	
	client, err := elasticsearch.NewClient(esConfig)
	if err != nil {
		logger.Fatalf("Elasticsearch 클라이언트 생성 실패: %v", err)
	}

	return &ElasticsearchService{
		client: client,
		logger: logger,
	}
}

// TestConnection Elasticsearch 연결 테스트
func (es *ElasticsearchService) TestConnection() error {
	info, err := es.client.Info()
	if err != nil {
		return fmt.Errorf("연결 실패: %v", err)
	}
	defer info.Body.Close()
	
	if info.IsError() {
		return fmt.Errorf("연결 오류: %s", info.String())
	}
	
	return nil
}

// BulkSendBusLocations 벌크 인서트로 버스 위치 정보를 Elasticsearch에 전송
func (es *ElasticsearchService) BulkSendBusLocations(indexName string, busLocations []models.BusLocation) error {
	if len(busLocations) == 0 {
		return nil
	}

	var buf bytes.Buffer
	// 현재 시간을 모든 데이터에 동일하게 적용 (동일 배치의 데이터이므로)
	timestamp := time.Now().Format(time.RFC3339)

	// 벌크 요청 바디 생성
	for _, busLocation := range busLocations {
		// 모든 버스 데이터에 동일한 타임스탬프 적용
		busLocation.Timestamp = timestamp
		
		// 인덱스 메타데이터
		meta := map[string]interface{}{
			"index": map[string]interface{}{
				"_index": indexName,
			},
		}
		
		metaBytes, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("메타데이터 마샬링 실패: %v", err)
		}
		
		// 문서 데이터
		docBytes, err := json.Marshal(busLocation)
		if err != nil {
			return fmt.Errorf("문서 데이터 마샬링 실패: %v", err)
		}
		
		// 벌크 요청 형식: 각 라인은 \n으로 구분
		buf.Write(metaBytes)
		buf.WriteByte('\n')
		buf.Write(docBytes)
		buf.WriteByte('\n')
	}

	// 벌크 요청 실행
	req := esapi.BulkRequest{
		Index: indexName,
		Body:  strings.NewReader(buf.String()),
	}

	res, err := req.Do(context.Background(), es.client)
	if err != nil {
		return fmt.Errorf("벌크 요청 실행 실패: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("벌크 요청 오류 [%s]: %s", res.Status(), string(body))
	}

	// 응답 파싱하여 에러 확인
	var bulkResponse models.BulkResponse
	if err := json.NewDecoder(res.Body).Decode(&bulkResponse); err != nil {
		return fmt.Errorf("벌크 응답 파싱 실패: %v", err)
	}

	// 에러가 있는 항목 체크
	errorCount := 0
	successCount := 0
	
	for _, item := range bulkResponse.Items {
		if item.Index.Error != nil {
			errorCount++
			es.logger.Errorf("벌크 인서트 오류: %s - %s", item.Index.Error.Type, item.Index.Error.Reason)
		} else {
			successCount++
		}
	}

	es.logger.Infof("벌크 인서트 완료 (%s) - 성공: %d, 실패: %d, 총 소요시간: %dms", 
		timestamp, successCount, errorCount, bulkResponse.Took)

	if errorCount > 0 {
		return fmt.Errorf("벌크 인서트 중 %d개 항목 실패", errorCount)
	}

	return nil
}