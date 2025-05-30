#!/bin/bash

# 버스 트래커 상태 실시간 확인 스크립트
# 사용법: ./status_check.sh

echo "🚌 버스 트래커 실시간 상태 체크"
echo "======================================"

# Elasticsearch 상태 확인
echo "📊 Elasticsearch 상태:"
ES_URL=${ELASTICSEARCH_URL:-"http://localhost:9200"}
INDEX_NAME=${INDEX_NAME:-"bus-locations"}

if curl -s "${ES_URL}/_cluster/health" > /dev/null; then
    echo "✅ Elasticsearch 연결: 정상"
    
    # 전체 문서 수
    TOTAL_DOCS=$(curl -s "${ES_URL}/${INDEX_NAME}/_count" | jq -r '.count // 0' 2>/dev/null || echo "0")
    echo "   📄 총 문서 수: ${TOTAL_DOCS}개"
    
    # 최근 1분간 데이터
    RECENT_1MIN=$(curl -s "${ES_URL}/${INDEX_NAME}/_search" \
        -H "Content-Type: application/json" \
        -d '{
            "query": {
                "range": {
                    "@timestamp": {
                        "gte": "now-1m"
                    }
                }
            },
            "size": 0
        }' | jq -r '.hits.total.value // 0' 2>/dev/null || echo "0")
    echo "   ⏰ 최근 1분: ${RECENT_1MIN}개"
    
    # 최근 5분간 데이터
    RECENT_5MIN=$(curl -s "${ES_URL}/${INDEX_NAME}/_search" \
        -H "Content-Type: application/json" \
        -d '{
            "query": {
                "range": {
                    "@timestamp": {
                        "gte": "now-5m"
                    }
                }
            },
            "size": 0
        }' | jq -r '.hits.total.value // 0' 2>/dev/null || echo "0")
    echo "   📅 최근 5분: ${RECENT_5MIN}개"
    
    # 고유 버스 개수 (최근 10분)
    UNIQUE_BUSES=$(curl -s "${ES_URL}/${INDEX_NAME}/_search" \
        -H "Content-Type: application/json" \
        -d '{
            "query": {
                "range": {
                    "@timestamp": {
                        "gte": "now-10m"
                    }
                }
            },
            "aggs": {
                "unique_buses": {
                    "cardinality": {
                        "field": "plateNo.keyword"
                    }
                }
            },
            "size": 0
        }' | jq -r '.aggregations.unique_buses.value // 0' 2>/dev/null || echo "0")
    echo "   🚌 활성 버스: ${UNIQUE_BUSES}대 (최근 10분)"
    
    # 최근 데이터 샘플 (최신 5개)
    echo "   📋 최근 데이터 샘플:"
    curl -s "${ES_URL}/${INDEX_NAME}/_search" \
        -H "Content-Type: application/json" \
        -d '{
            "query": {
                "match_all": {}
            },
            "sort": [
                {
                    "@timestamp": {
                        "order": "desc"
                    }
                }
            ],
            "size": 5,
            "_source": ["plateNo", "nodeNm", "nodeOrd", "totalStations", "@timestamp"]
        }' | jq -r '.hits.hits[]._source | "      🚌 \(.plateNo): \(.nodeNm // "정류장정보없음") (\(.nodeOrd // 0)/\(.totalStations // 0)) [\(."@timestamp")]"' 2>/dev/null || echo "      데이터 없음"
    
else
    echo "❌ Elasticsearch 연결: 실패"
fi

echo ""

# 프로세스 상태
echo "⚙️ 프로세스 상태:"
if pgrep -f "bus-tracker" > /dev/null; then
    PID=$(pgrep -f "bus-tracker")
    echo "✅ bus-tracker 실행 중 (PID: ${PID})"
    
    # 메모리 사용량
    MEMORY=$(ps -p ${PID} -o rss= 2>/dev/null | awk '{printf "%.1f MB", $1/1024}')
    echo "   💾 메모리: ${MEMORY}"
    
    # CPU 사용량
    CPU=$(ps -p ${PID} -o %cpu= 2>/dev/null | awk '{print $1"%"}')
    echo "   🔥 CPU: ${CPU}"
    
    # 실행 시간
    ETIME=$(ps -p ${PID} -o etime= 2>/dev/null | xargs)
    echo "   ⏱️ 실행시간: ${ETIME}"
    
else
    echo "❌ bus-tracker 프로세스 없음"
fi

echo ""

# API 상태 확인
echo "🌐 API 연결 상태:"

# 경기도 API 테스트
if timeout 5 curl -s "https://apis.data.go.kr/6410000" > /dev/null; then
    echo "✅ 경기도 버스 API: 연결 가능"
else
    echo "❌ 경기도 버스 API: 연결 실패"
fi

# 공공데이터포털 API 테스트
if timeout 5 curl -s "http://apis.data.go.kr/1613000" > /dev/null; then
    echo "✅ 공공데이터포털 API: 연결 가능"
else
    echo "❌ 공공데이터포털 API: 연결 실패"
fi

echo ""

# 데이터 플로우 분석
echo "📈 데이터 플로우 분석:"

if [ "${RECENT_1MIN:-0}" -gt 0 ]; then
    echo "✅ 실시간 데이터 수신 중"
    RATE_PER_MIN=${RECENT_1MIN}
    RATE_PER_HOUR=$((RATE_PER_MIN * 60))
    echo "   📊 예상 시간당 처리량: ${RATE_PER_HOUR}건"
elif [ "${RECENT_5MIN:-0}" -gt 0 ]; then
    echo "⚠️ 느린 데이터 수신 (5분 내에만 데이터 있음)"
    RATE_PER_MIN=$((RECENT_5MIN / 5))
    echo "   📊 평균 분당 처리량: ${RATE_PER_MIN}건"
else
    echo "❌ 데이터 수신 없음"
fi

echo ""

# 시스템 리소스
echo "💻 시스템 리소스:"
df -h / | tail -1 | awk '{print "   💾 디스크: " $3 " / " $2 " (" $5 " 사용)"}'
free -h | grep '^Mem:' | awk '{print "   🧠 메모리: " $3 " / " $2 " (" int($3/$2*100) "% 사용)"}'

echo ""
echo "🔄 상태 체크 완료 ($(date '+%H:%M:%S'))"

# 상태 요약
echo ""
echo "📋 요약:"
ISSUES=0

if ! pgrep -f "bus-tracker" > /dev/null; then
    echo "❌ 프로세스 중단"
    ISSUES=$((ISSUES + 1))
fi

if ! curl -s "${ES_URL}/_cluster/health" > /dev/null; then
    echo "❌ Elasticsearch 연결 실패"
    ISSUES=$((ISSUES + 1))
fi

if [ "${RECENT_1MIN:-0}" -eq 0 ]; then
    echo "⚠️ 실시간 데이터 없음"
    ISSUES=$((ISSUES + 1))
fi

if [ "${UNIQUE_BUSES:-0}" -eq 0 ]; then
    echo "⚠️ 활성 버스 없음"
    ISSUES=$((ISSUES + 1))
fi

if [ $ISSUES -eq 0 ]; then
    echo "✅ 모든 시스템 정상"
else
    echo "⚠️ ${ISSUES}개 문제 감지됨"
fi