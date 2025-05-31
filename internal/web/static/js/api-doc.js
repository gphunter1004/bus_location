// API 문서 페이지 JavaScript

// API 테스트 함수 (GET 요청)
function testAPI(endpoint) {
    const resultElement = document.getElementById('testResult');
    
    // 로딩 상태 설정
    resultElement.className = 'test-result loading';
    resultElement.innerHTML = `
        <div class="result-meta">
            <span>테스트 중...</span>
            <span>GET ${endpoint}</span>
        </div>
        <p>API 요청을 처리하고 있습니다...</p>
    `;

    const startTime = performance.now();

    fetch(endpoint)
        .then(response => {
            const endTime = performance.now();
            const responseTime = Math.round(endTime - startTime);
            
            // 응답 상태에 따른 클래스 설정
            if (response.ok) {
                resultElement.className = 'test-result success';
            } else {
                resultElement.className = 'test-result error';
            }

            return response.json().then(data => ({
                status: response.status,
                statusText: response.statusText,
                data: data,
                responseTime: responseTime
            }));
        })
        .then(result => {
            displayTestResult(endpoint, 'GET', result);
            saveTestHistory(endpoint, 'GET');
        })
        .catch(error => {
            const endTime = performance.now();
            const responseTime = Math.round(endTime - startTime);
            
            resultElement.className = 'test-result error';
            displayTestError(endpoint, 'GET', error, responseTime);
        });
}

// API 테스트 함수 (POST 요청)
function testAPIPost(endpoint, data = {}) {
    const resultElement = document.getElementById('testResult');
    
    // 확인 대화상자 (시스템 제어 API의 경우)
    if (endpoint.includes('/control/')) {
        const action = endpoint.split('/').pop();
        const actionNames = {
            'start': '시작',
            'stop': '정지',
            'restart': '재시작'
        };
        const actionName = actionNames[action] || action;
        
        if (!confirm(`시스템 ${actionName} 작업을 실행하시겠습니까?`)) {
            return;
        }
    }
    
    // 로딩 상태 설정
    resultElement.className = 'test-result loading';
    resultElement.innerHTML = `
        <div class="result-meta">
            <span>테스트 중...</span>
            <span>POST ${endpoint}</span>
        </div>
        <p>API 요청을 처리하고 있습니다...</p>
    `;

    const startTime = performance.now();

    fetch(endpoint, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(data)
    })
        .then(response => {
            const endTime = performance.now();
            const responseTime = Math.round(endTime - startTime);
            
            // 응답 상태에 따른 클래스 설정
            if (response.ok) {
                resultElement.className = 'test-result success';
            } else {
                resultElement.className = 'test-result error';
            }

            return response.json().then(data => ({
                status: response.status,
                statusText: response.statusText,
                data: data,
                responseTime: responseTime
            }));
        })
        .then(result => {
            displayTestResult(endpoint, 'POST', result);
            saveTestHistory(endpoint, 'POST');
        })
        .catch(error => {
            const endTime = performance.now();
            const responseTime = Math.round(endTime - startTime);
            
            resultElement.className = 'test-result error';
            displayTestError(endpoint, 'POST', error, responseTime);
        });
}

// 테스트 결과 표시
function displayTestResult(endpoint, method, result) {
    const resultElement = document.getElementById('testResult');
    
    const statusClass = result.status >= 200 && result.status < 300 ? 'success' : 
                       result.status >= 400 ? 'error' : 'warning';
    
    resultElement.innerHTML = `
        <div class="result-meta">
            <span><strong>${method}</strong> ${endpoint}</span>
            <span class="status-code ${statusClass}">${result.status} ${result.statusText}</span>
            <span>응답시간: ${result.responseTime}ms</span>
        </div>
        <pre><code>${JSON.stringify(result.data, null, 2)}</code></pre>
    `;
    
    // 성공시 알림
    if (result.status >= 200 && result.status < 300) {
        showNotification(`${method} ${endpoint} 테스트 성공`, 'success');
    } else {
        showNotification(`${method} ${endpoint} 테스트 실패 (${result.status})`, 'error');
    }
}

// 테스트 에러 표시
function displayTestError(endpoint, method, error, responseTime) {
    const resultElement = document.getElementById('testResult');
    
    resultElement.innerHTML = `
        <div class="result-meta">
            <span><strong>${method}</strong> ${endpoint}</span>
            <span class="status-code error">네트워크 오류</span>
            <span>응답시간: ${responseTime}ms</span>
        </div>
        <p><strong>오류:</strong> ${error.message}</p>
        <div style="margin-top: 15px; padding: 10px; background: #f8f9fa; border-radius: 4px;">
            <strong>가능한 원인:</strong>
            <ul style="margin: 5px 0 0 20px;">
                <li>서버가 실행되지 않음</li>
                <li>네트워크 연결 문제</li>
                <li>CORS 정책에 의한 차단</li>
                <li>API 엔드포인트 오류</li>
            </ul>
        </div>
    `;
    
    showNotification(`${method} ${endpoint} 테스트 실패`, 'error');
}

// 테스트 기록 저장
function saveTestHistory(endpoint, method) {
    try {
        localStorage.setItem('lastTestedEndpoint', endpoint);
        localStorage.setItem('lastTestedMethod', method);
        localStorage.setItem('lastTestTime', new Date().toISOString());
    } catch (e) {
        console.warn('로컬 스토리지 저장 실패:', e);
    }
}

// 페이지별 특화 테스트 함수들
function testWithParameters() {
    const endpoint = '/api/v1/monitoring/logs';
    const limit = prompt('로그 개수를 입력하세요 (기본값: 10):', '10');
    const level = prompt('로그 레벨을 입력하세요 (debug, info, warn, error, 또는 빈값):', '');
    
    let url = `${endpoint}?limit=${limit || 10}`;
    if (level && level.trim()) {
        url += `&level=${level.trim()}`;
    }
    
    testAPI(url);
}

function testBusTracking() {
    const plateNo = prompt('조회할 버스 번호를 입력하세요 (예: 경기12가3456):', '');
    if (plateNo && plateNo.trim()) {
        testAPI(`/api/v1/tracking/buses/${encodeURIComponent(plateNo.trim())}`);
    } else {
        showNotification('버스 번호를 입력해주세요', 'warning');
    }
}

function testRouteInfo() {
    const routeId = prompt('조회할 노선 ID를 입력하세요 (예: 233000266 또는 GGB233000266):', '');
    if (routeId && routeId.trim()) {
        testAPI(`/api/v1/tracking/routes/${encodeURIComponent(routeId.trim())}/buses`);
    } else {
        showNotification('노선 ID를 입력해주세요', 'warning');
    }
}

function testCacheReload() {
    if (confirm('정류소 캐시를 새로고침하시겠습니까? 시간이 오래 걸릴 수 있습니다.')) {
        testAPIPost('/api/v1/cache/stations/reload');
    }
}

function testTripReset() {
    if (confirm('운행 차수 카운터를 리셋하시겠습니까? 모든 차량의 일일 운행 기록이 초기화됩니다.')) {
        testAPIPost('/api/v1/tracking/trips/reset-counters');
    }
}

// 알림 표시
function showNotification(message, type = 'info') {
    const notification = document.createElement('div');
    notification.className = `notification ${type}`;
    notification.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        padding: 15px 20px;
        border-radius: 4px;
        color: white;
        z-index: 1000;
        max-width: 300px;
        opacity: 0;
        transform: translateX(100%);
        transition: all 0.3s ease;
        font-size: 14px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    `;
    
    const colors = {
        success: '#4CAF50',
        error: '#f44336',
        warning: '#ff9800',
        info: '#2196F3'
    };
    
    notification.style.backgroundColor = colors[type] || colors.info;
    notification.textContent = message;
    
    document.body.appendChild(notification);
    
    // 애니메이션
    setTimeout(() => {
        notification.style.opacity = '1';
        notification.style.transform = 'translateX(0)';
    }, 10);
    
    // 자동 제거
    setTimeout(() => {
        notification.style.opacity = '0';
        notification.style.transform = 'translateX(100%)';
        setTimeout(() => {
            if (notification.parentNode) {
                notification.parentNode.removeChild(notification);
            }
        }, 300);
    }, 3000);
}

// 자동 새로고침 기능
let autoRefresh = false;
let refreshInterval;

function toggleAutoRefresh() {
    autoRefresh = !autoRefresh;
    
    if (autoRefresh) {
        // 마지막으로 테스트한 엔드포인트 확인
        const lastEndpoint = localStorage.getItem('lastTestedEndpoint');
        const lastMethod = localStorage.getItem('lastTestedMethod') || 'GET';
        
        if (!lastEndpoint) {
            showNotification('먼저 API를 테스트해주세요', 'warning');
            autoRefresh = false;
            return;
        }
        
        refreshInterval = setInterval(() => {
            if (lastMethod === 'POST') {
                // POST 요청은 자동 새로고침에서 제외 (부작용 방지)
                return;
            }
            testAPI(lastEndpoint);
        }, 30000); // 30초마다
        
        showNotification('자동 새로고침이 활성화되었습니다 (30초 간격)', 'success');
        updateAutoRefreshButton(true);
    } else {
        clearInterval(refreshInterval);
        showNotification('자동 새로고침이 비활성화되었습니다', 'info');
        updateAutoRefreshButton(false);
    }
}

function updateAutoRefreshButton(isActive) {
    const button = document.getElementById('autoRefreshBtn');
    if (button) {
        button.textContent = isActive ? '🔄 자동새로고침 중지' : '🔄 자동새로고침 시작';
        button.className = `btn ${isActive ? 'danger' : 'success'}`;
    }
}

// 빠른 테스트 함수들
function quickHealthCheck() {
    testAPI('/api/v1/status/health');
}

function quickStatistics() {
    testAPI('/api/v1/status/statistics');
}

function quickMetrics() {
    testAPI('/api/v1/monitoring/metrics');
}

function quickConfig() {
    testAPI('/api/v1/config');
}

// 키보드 단축키
document.addEventListener('keydown', function(event) {
    // 입력 필드에서는 단축키 비활성화
    if (event.target.tagName === 'INPUT' || event.target.tagName === 'TEXTAREA') {
        return;
    }
    
    // Ctrl + T: 상태 API 테스트
    if (event.ctrlKey && event.key === 't') {
        event.preventDefault();
        testAPI('/api/v1/status');
    }
    
    // Ctrl + H: 헬스체크 테스트
    if (event.ctrlKey && event.key === 'h') {
        event.preventDefault();
        quickHealthCheck();
    }
    
    // Ctrl + S: 통계 API 테스트
    if (event.ctrlKey && event.key === 's') {
        event.preventDefault();
        quickStatistics();
    }
    
    // Ctrl + M: 메트릭 API 테스트
    if (event.ctrlKey && event.key === 'm') {
        event.preventDefault();
        quickMetrics();
    }
    
    // Ctrl + C: 설정 API 테스트
    if (event.ctrlKey && event.key === 'c') {
        event.preventDefault();
        quickConfig();
    }
    
    // Ctrl + R: 자동 새로고침 토글
    if (event.ctrlKey && event.key === 'r') {
        event.preventDefault();
        toggleAutoRefresh();
    }
    
    // ESC: 자동 새로고침 정지
    if (event.key === 'Escape' && autoRefresh) {
        toggleAutoRefresh();
    }
});

// 페이지 초기화
function initializeAPIDoc() {
    console.log('API 문서 페이지 초기화 중...');
    
    // 마지막 테스트 정보 표시
    const lastEndpoint = localStorage.getItem('lastTestedEndpoint');
    const lastTestTime = localStorage.getItem('lastTestTime');
    
    let helpMessage = `
        <div style="text-align: center; color: #666;">
            <h3>🧪 API 테스트</h3>
            <p>위의 API 엔드포인트에서 "🧪 테스트" 버튼을 클릭하면 실제 API를 호출하고 결과를 여기에 표시합니다.</p>
    `;
    
    if (lastEndpoint && lastTestTime) {
        const testTime = new Date(lastTestTime).toLocaleString();
        helpMessage += `
            <div style="background: #e3f2fd; padding: 10px; border-radius: 4px; margin: 15px 0;">
                <strong>마지막 테스트:</strong> ${lastEndpoint}<br>
                <strong>시간:</strong> ${testTime}
            </div>
        `;
    }
    
    helpMessage += `
            <p><strong>키보드 단축키:</strong></p>
            <ul style="display: inline-block; text-align: left;">
                <li>Ctrl + T: 상태 API 테스트</li>
                <li>Ctrl + H: 헬스체크 테스트</li>
                <li>Ctrl + S: 통계 API 테스트</li>
                <li>Ctrl + M: 메트릭 API 테스트</li>
                <li>Ctrl + C: 설정 API 테스트</li>
                <li>Ctrl + R: 자동 새로고침 토글</li>
                <li>ESC: 자동 새로고침 정지</li>
            </ul>
            
            <div style="margin-top: 20px;">
                <button id="autoRefreshBtn" class="btn success" onclick="toggleAutoRefresh()">🔄 자동새로고침 시작</button>
            </div>
        </div>
    `;
    
    // 테스트 결과 영역에 도움말 표시
    const resultElement = document.getElementById('testResult');
    resultElement.innerHTML = helpMessage;
    
    console.log('API 문서 페이지 초기화 완료');
    showNotification('API 문서 페이지가 로드되었습니다', 'success');
}

// 페이지 언로드 시 정리
window.addEventListener('beforeunload', function() {
    if (autoRefresh) {
        clearInterval(refreshInterval);
    }
});

// 에러 핸들링
window.addEventListener('error', function(event) {
    console.error('페이지 오류:', event.error);
    showNotification('페이지에서 오류가 발생했습니다', 'error');
});

// DOM 로드 완료 후 초기화
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initializeAPIDoc);
} else {
    initializeAPIDoc();
}