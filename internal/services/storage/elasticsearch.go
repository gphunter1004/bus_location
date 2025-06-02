package storage

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
	"bus-tracker/internal/models"
	"bus-tracker/internal/utils"
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

	es.logger.Infof("📤 ES 전송 시작 - 인덱스: %s, 건수: %d건", indexName, len(busLocations))

	// 전송할 모든 데이터 로깅
	for i, busLocation := range busLocations {
		// 모든 버스 데이터에 동일한 타임스탬프 적용
		busLocation.Timestamp = timestamp

		// 각 버스 데이터 상세 로깅
		stationInfo := ""
		if busLocation.NodeNm != "" {
			stationInfo = fmt.Sprintf("%s(%d/%d)", busLocation.NodeNm, busLocation.GetStationOrder(), busLocation.TotalStations)
		} else {
			stationInfo = "정류장정보없음"
		}

		es.logger.Infof("📤 [%d/%d] %s: 노선=%d, T%d, 위치=%s, StationId=%d, StationSeq=%d, NodeOrd=%d, NodeId=%s, GPS=(%.6f,%.6f), 상태=%d, 혼잡=%d, 좌석=%d",
			i+1, len(busLocations),
			busLocation.PlateNo,
			busLocation.RouteId,
			busLocation.TripNumber,
			stationInfo,
			busLocation.StationId,
			busLocation.StationSeq,
			busLocation.NodeOrd,
			busLocation.NodeId,
			busLocation.GpsLati,
			busLocation.GpsLong,
			busLocation.StateCd,
			busLocation.Crowded,
			busLocation.RemainSeatCnt)

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

	es.logger.Infof("📤 ES 실제 전송 시작... (타임스탬프: %s)", timestamp)
	sendStart := time.Now()
	res, err := req.Do(context.Background(), es.client)
	sendDuration := time.Since(sendStart)

	if err != nil {
		es.logger.Errorf("❌ ES 전송 실패 (소요시간: %v): %v", sendDuration, err)
		return fmt.Errorf("벌크 요청 실행 실패: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		es.logger.Errorf("❌ ES 응답 오류 [%s] (소요시간: %v): %s", res.Status(), sendDuration, string(body))
		return fmt.Errorf("벌크 요청 오류 [%s]: %s", res.Status(), string(body))
	}

	// 응답 파싱하여 에러 확인
	var bulkResponse models.BulkResponse
	if err := json.NewDecoder(res.Body).Decode(&bulkResponse); err != nil {
		es.logger.Errorf("❌ ES 응답 파싱 실패 (소요시간: %v): %v", sendDuration, err)
		return fmt.Errorf("벌크 응답 파싱 실패: %v", err)
	}

	// 에러가 있는 항목 체크
	errorCount := 0
	successCount := 0

	for i, item := range bulkResponse.Items {
		if item.Index.Error != nil {
			errorCount++
			es.logger.Errorf("❌ ES 인덱싱 실패 [%d/%d] %s: %s - %s",
				i+1, len(busLocations),
				busLocations[i].PlateNo,
				item.Index.Error.Type,
				item.Index.Error.Reason)
		} else {
			successCount++
		}
	}

	if errorCount > 0 {
		es.logger.Errorf("⚠️ ES 전송 부분 실패 - 성공: %d건, 실패: %d건, 소요시간: %v",
			successCount, errorCount, sendDuration)
		return fmt.Errorf("벌크 인서트 중 %d개 항목 실패", errorCount)
	} else {
		es.logger.Infof("✅ ES 전송 완료 - %d건 성공, 소요시간: %v",
			successCount, sendDuration)
	}

	return nil
}
