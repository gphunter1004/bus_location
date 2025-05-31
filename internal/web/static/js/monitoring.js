// 모니터링 페이지 JavaScript

// 전역 변수
let updateInterval;
let logUpdateInterval;
let metricsHistory = {
    memory: [],
    goroutines: [],
    timestamps: []
};

// 모니터링 데이터 업데이트
function updateMonitoringData() {
    updateSystemStatus();
    updateMetrics();
    updateAPIStatus();
    updateAlerts();
}

// 시스템 상태 업데이트
function updateSystemStatus() {
    fetch('/api/v1/status/health')
        .then(response => response.json())
        .then(data => {
            const isHealthy = data.success;
            const statusElement = document.getElementById('systemStatusValue');
            const indicatorElement = document.getElementById('systemStatusIndicator');
            
            if (isHealthy) {
                statusElement.textContent = '정상';
                indicatorElement.className = 'monitor-indicator healthy';
            } else {
                statusElement.textContent = '문제 발생';
                indicatorElement.className = 'monitor-indicator error';
            }
        })
        .catch(err => {
            console.error('시스템 상태 업데이트 실패:', err);
            document.getElementById('systemStatusValue').textContent = '확인 불가';
            document.getElementById('systemStatusIndicator').className = 'monitor-indicator error';
        });

    // 통계 데이터 업데이트
    fetch('/api/v1/status/statistics')
        .then(response => response.json())
        .then(data => {
            if (data.success && data.data) {
                const stats = data.data.busStatistics;
                document.getElementById('activeBusesValue').textContent = stats.totalBuses || 0;
                
                // 데이터 플로우 계산 (임시)
                const dataFlow = (stats.totalBuses || 0) * 2; // 분당 예상 데이터
                document.getElementById('dataFlowValue').textContent = dataFlow + '/분';
            }
        })
        .catch(err => console.error('통계 업데이트 실패:', err));
}

// 시스템 메트릭 업데이트
function updateMetrics() {
    fetch('/api/v1/monitoring/metrics')
        .then(response => response.json())
        .then(data => {
            if (data.success && data.data) {
                const metrics = data.data;
                
                // 메모리 사용량
                if (metrics.memory && metrics.memory.alloc) {
                    document.getElementById('memoryValue').textContent = metrics.memory.alloc;
                    
                    // 메모리 히스토리 업데이트
                    const memoryMB = parseFloat(metrics.memory.alloc.replace(' MB', ''));
                    updateMetricsHistory('memory', memoryMB);
                }
                
                // 고루틴 수
                if (metrics.goroutines) {
                    document.getElementById('goroutineValue').textContent = metrics.goroutines;
                    updateMetricsHistory('goroutines', metrics.goroutines);
                }
                
                // 차트 업데이트
                updateCharts();
            }
        })
        .catch(err => console.error('메트릭 업데이트 실패:', err));
}

// 메트릭 히스토리 업데이트
function updateMetricsHistory(type, value) {
    const maxPoints = 20;
    
    if (!metricsHistory[type]) {
        metricsHistory[type] = [];
    }
    
    metricsHistory[type].push(value);
    
    if (metricsHistory[type].length > maxPoints) {
        metricsHistory[type].shift();
    }
    
    // 타임스탬프 업데이트
    metricsHistory.timestamps.push(new Date());
    if (metricsHistory.timestamps.length > maxPoints) {
        metricsHistory.timestamps.shift();
    }
}

// 차트 업데이트 (간단한 텍스트 기반)
function updateCharts() {
    updateMemoryChart();
    updateGoroutineChart();
}

// 메모리 차트 업데이트
function updateMemoryChart() {
    const chartElement = document.getElementById('memoryChart');
    const data = metricsHistory.memory;
    
    if (data.length < 2) {
        chartElement.innerHTML = '<div class="chart-placeholder">데이터 수집 중...</div>';
        return;
    }
    
    const max = Math.max(...data);
    const min = Math.min(...data);
    const range = max - min || 1;
    
    let chartHtml = '<div class="simple-chart">';
    for (let i = 0; i < data.length; i++) {
        const height = ((data[i] - min) / range) * 80 + 10; // 10-90% 높이
        chartHtml += `<div class="chart-bar" style="height: ${height}%; background: #2196F3; width: ${100/data.length}%; display: inline-block; margin-right: 1px;"></div>`;
    }
    chartHtml += '</div>';
    chartHtml += `<div class="chart-info">최근: ${data[data.length-1]}MB (최고: ${max}MB)</div>`;
    
    chartElement.innerHTML = chartHtml;
}

// 고루틴 차트 업데이트
function updateGoroutineChart() {
    const chartElement = document.getElementById('goroutineChart');
    const data = metricsHistory.goroutines;
    
    if (data.length < 2) {
        chartElement.innerHTML = '<div class="chart-placeholder">데이터 수집 중...</div>';
        return;
    }
    
    const max = Math.max(...data);
    const min = Math.min(...data);
    const range = max - min || 1;
    
    let chartHtml = '<div class="simple-chart">';
    for (let i = 0; i < data.length; i++) {
        const height = ((data[i] - min) / range) * 80 + 10;
        chartHtml += `<div class="chart-bar" style="height: ${height}%; background: #4CAF50; width: ${100/data.length}%; display: inline-block; margin-right: 1px;"></div>`;
    }
    chartHtml += '</div>';
    chartHtml += `<div class="chart-info">현재: ${data[data.length-1]}개 (최고: ${max}개)</div>`;
    
    chartElement.innerHTML = chartHtml;
}

// API 상태 업데이트
function updateAPIStatus() {
    // 설정 정보로부터 API 상태 확인
    fetch('/api/v1/config')
        .then(response => response.json())
        .then(data => {
            if (data.success && data.data) {
                const config = data.data;
                
                // API1 상태
                updateAPIStatusIndicator('api1', config.apis.api1.enabled, config.apis.api1.interval);
                
                // API2 상태
                updateAPIStatusIndicator('api2', config.apis.api2.enabled, config.apis.api2.interval);
            }
        })
        .catch(err => console.error('API 상태 업데이트 실패:', err));
}

// API 상태 표시기 업데이트
function updateAPIStatusIndicator(apiName, isEnabled, interval) {
    const statusElement = document.getElementById(`${apiName}StatusText`);
    const indicatorElement = document.getElementById(`${apiName}Indicator`);
    const intervalElement = document.getElementById(`${apiName}Interval`);
    const responseTimeElement = document.getElementById(`${apiName}ResponseTime`);
    
    if (isEnabled) {
        statusElement.textContent = '활성';
        indicatorElement.className = 'status-indicator healthy';
        intervalElement.textContent = interval;
        responseTimeElement.textContent = Math.floor(Math.random() * 500 + 100); // 모의 응답시간
    } else {
        statusElement.textContent = '비활성';
        indicatorElement.className = 'status-indicator error';
        intervalElement.textContent = '-';
        responseTimeElement.textContent = '-';
    }
}

// 로그 업데이트
function updateLogs() {
    const levelFilter = document.getElementById('logLevel').value;
    const params = levelFilter ? `?level=${levelFilter}&limit=50` : '?limit=50';
    
    fetch(`/api/v1/monitoring/logs${params}`)
        .then(response => response.json())
        .then(data => {
            const logContainer = document.getElementById('logContainer');
            
            if (data.success && data.data && data.data.length > 0) {
                let logHtml = '';
                data.data.forEach(log => {
                    const timestamp = new Date(log.timestamp).toLocaleTimeString();
                    logHtml += `<div class="log-entry ${log.level.toLowerCase()}">`;
                    logHtml += `<span class="log-timestamp">${timestamp}</span>`;
                    logHtml += `<span class="log-level">${log.level}</span>`;
                    logHtml += `<span class="log-message">${log.message}</span>`;
                    logHtml += `</div>`;
                });
                logContainer.innerHTML = logHtml;
                
                // 스크롤을 맨 아래로
                logContainer.scrollTop = logContainer.scrollHeight;
            } else {
                logContainer.innerHTML = '<div class="log-loading">로그가 없습니다</div>';
            }
        })
        .catch(err => {
            console.error('로그 업데이트 실패:', err);
            document.getElementById('logContainer').innerHTML = 
                '<div class="log-loading">로그 로딩 실패</div>';
        });
}

// 알림 업데이트
function updateAlerts() {
    fetch('/api/v1/monitoring/alerts')
        .then(response => response.json())
        .then(data => {
            const alertsContainer = document.getElementById('alertsContainer');
            
            if (data.success && data.data && data.data.length > 0) {
                let alertsHtml = '';
                data.data.forEach(alert => {
                    const timestamp = new Date(alert.timestamp).toLocaleTimeString();
                    alertsHtml += `<div class="alert-item ${alert.type}">`;
                    alertsHtml += `<div class="alert-title">${alert.title}</div>`;
                    alertsHtml += `<div class="alert-message">${alert.message}</div>`;
                    alertsHtml += `<div class="alert-timestamp">${timestamp}</div>`;
                    alertsHtml += `</div>`;
                });
                alertsContainer.innerHTML = alertsHtml;
            } else {
                alertsContainer.innerHTML = '<div class="alert-loading">활성 알림이 없습니다</div>';
            }
        })
        .catch(err => {
            console.error('알림 업데이트 실패:', err);
            document.getElementById('alertsContainer').innerHTML = 
                '<div class="alert-loading">알림 로딩 실패</div>';
        });
}

// 로그 새로고침
function refreshLogs() {
    updateLogs();
    showNotification('로그가 새로고침되었습니다', 'success');
}

// 로그 지우기
function clearLogs() {
    document.getElementById('logContainer').innerHTML = 
        '<div class="log-loading">로그가 지워졌습니다</div>';
    showNotification('로그 화면이 지워졌습니다', 'info');
}

// 응답 시간 측정
function measureResponseTime(url) {
    const startTime = performance.now();
    
    return fetch(url)
        .then(response => {
            const endTime = performance.now();
            return Math.round(endTime - startTime);
        })
        .catch(() => -1);
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
    `;
    
    // 타입별 색상
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

// 실시간 업데이트 시작/정지
function startRealTimeUpdates() {
    // 메인 데이터 업데이트 (15초마다)
    updateInterval = setInterval(updateMonitoringData, 15000);
    
    // 로그 업데이트 (30초마다)
    logUpdateInterval = setInterval(updateLogs, 30000);
    
    console.log('실시간 업데이트 시작');
}

function stopRealTimeUpdates() {
    if (updateInterval) {
        clearInterval(updateInterval);
        updateInterval = null;
    }
    
    if (logUpdateInterval) {
        clearInterval(logUpdateInterval);
        logUpdateInterval = null;
    }
    
    console.log('실시간 업데이트 정지');
}

// 페이지 가시성 변경 처리
document.addEventListener('visibilitychange', function() {
    if (document.hidden) {
        stopRealTimeUpdates();
        console.log('페이지가 숨겨져 업데이트를 일시정지합니다');
    } else {
        updateMonitoringData(); // 즉시 한 번 업데이트
        updateLogs();
        startRealTimeUpdates(); // 실시간 업데이트 재시작
        console.log('페이지가 다시 보여 업데이트를 재개합니다');
    }
});

// 로그 레벨 변경 이벤트
document.getElementById('logLevel').addEventListener('change', function() {
    updateLogs();
});

// 키보드 단축키
document.addEventListener('keydown', function(event) {
    // F5 또는 Ctrl+R: 새로고침
    if (event.key === 'F5' || (event.ctrlKey && event.key === 'r')) {
        event.preventDefault();
        location.reload();
    }
    
    // Ctrl+L: 로그 새로고침
    if (event.ctrlKey && event.key === 'l') {
        event.preventDefault();
        refreshLogs();
    }
    
    // Ctrl+M: 메트릭 즉시 업데이트
    if (event.ctrlKey && event.key === 'm') {
        event.preventDefault();
        updateMetrics();
        showNotification('메트릭이 업데이트되었습니다', 'success');
    }
});

// 초기화
function initializeMonitoring() {
    console.log('모니터링 시스템 초기화 중...');
    
    // 초기 데이터 로드
    updateMonitoringData();
    updateLogs();
    
    // 실시간 업데이트 시작
    startRealTimeUpdates();
    
    console.log('모니터링 시스템 초기화 완료');
    showNotification('모니터링 시스템이 시작되었습니다', 'success');
}

// DOM 로드 완료 후 초기화
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initializeMonitoring);
} else {
    initializeMonitoring();
}

// 페이지 언로드 시 정리
window.addEventListener('beforeunload', function() {
    stopRealTimeUpdates();
});