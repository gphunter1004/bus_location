// Dashboard JavaScript

// API 호출 관련 함수들
function makeAPICall(url, targetElementId, description) {
    const targetElement = document.getElementById(targetElementId);
    
    // 로딩 상태 표시
    targetElement.innerHTML = '<div class="loading">로딩 중...</div>';
    
    fetch(url)
        .then(response => response.json())
        .then(data => {
            targetElement.innerHTML = 
                '<pre>' + JSON.stringify(data, null, 2) + '</pre>';
        })
        .catch(err => {
            targetElement.innerHTML = 
                '<div style="color: red;">오류: ' + err.message + '</div>';
        });
}

// 상태 조회
function getStatus() {
    makeAPICall('/api/v1/status', 'controlResult', '시스템 상태');
}

// 헬스체크
function getHealth() {
    makeAPICall('/api/v1/status/health', 'controlResult', '헬스체크');
}

// 설정 조회
function getConfig() {
    makeAPICall('/api/v1/config', 'infoResult', '설정 정보');
}

// 메트릭 조회
function getMetrics() {
    makeAPICall('/api/v1/monitoring/metrics', 'infoResult', '시스템 메트릭');
}

// 통계 업데이트
function updateStatistics() {
    fetch('/api/v1/status/statistics')
        .then(response => response.json())
        .then(data => {
            if (data.success && data.data) {
                const stats = data.data.busStatistics;
                
                // 애니메이션 효과를 위한 클래스 추가
                document.getElementById('statistics').classList.add('updating');
                
                // 통계 업데이트
                updateStatElement('totalBuses', stats.totalBuses || 0);
                updateStatElement('api1Only', stats.api1Only || 0);
                updateStatElement('api2Only', stats.api2Only || 0);
                updateStatElement('bothAPIs', stats.both || 0);
                updateStatElement('trackedBuses', stats.trackedBuses || 0);
                
                // 시간 업데이트
                const updateTime = document.getElementById('updateTime');
                if (updateTime) {
                    updateTime.textContent = '(' + new Date().toLocaleTimeString() + ')';
                }
                
                // 애니메이션 클래스 제거
                setTimeout(() => {
                    document.getElementById('statistics').classList.remove('updating');
                }, 500);
            }
        })
        .catch(err => {
            console.error('통계 업데이트 실패:', err);
            showErrorNotification('통계 업데이트에 실패했습니다');
        });
}

// 개별 통계 요소 업데이트 (애니메이션 효과)
function updateStatElement(elementId, newValue) {
    const element = document.getElementById(elementId);
    if (!element) return;
    
    const currentValue = parseInt(element.textContent) || 0;
    
    if (currentValue !== newValue) {
        // 값이 변경된 경우 하이라이트 효과
        element.style.backgroundColor = '#4CAF50';
        element.style.color = 'white';
        element.style.borderRadius = '4px';
        element.style.padding = '2px 6px';
        
        element.textContent = newValue;
        
        // 하이라이트 효과 제거
        setTimeout(() => {
            element.style.backgroundColor = '';
            element.style.color = '';
            element.style.borderRadius = '';
            element.style.padding = '';
        }, 1000);
    } else {
        element.textContent = newValue;
    }
}

// 에러 알림 표시
function showErrorNotification(message) {
    // 간단한 알림 표시 (실제로는 toast 라이브러리 사용 권장)
    const notification = document.createElement('div');
    notification.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        background: #f44336;
        color: white;
        padding: 15px;
        border-radius: 4px;
        z-index: 1000;
        max-width: 300px;
    `;
    notification.textContent = message;
    
    document.body.appendChild(notification);
    
    // 3초 후 제거
    setTimeout(() => {
        if (notification.parentNode) {
            notification.parentNode.removeChild(notification);
        }
    }, 3000);
}

// 성공 알림 표시
function showSuccessNotification(message) {
    const notification = document.createElement('div');
    notification.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        background: #4CAF50;
        color: white;
        padding: 15px;
        border-radius: 4px;
        z-index: 1000;
        max-width: 300px;
    `;
    notification.textContent = message;
    
    document.body.appendChild(notification);
    
    setTimeout(() => {
        if (notification.parentNode) {
            notification.parentNode.removeChild(notification);
        }
    }, 3000);
}

// 시스템 상태 확인
function checkSystemHealth() {
    fetch('/api/v1/status/health')
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                // 건강한 상태인 경우 통계 업데이트 계속
                updateStatistics();
            } else {
                showErrorNotification('시스템 상태가 불안정합니다');
            }
        })
        .catch(err => {
            console.error('헬스체크 실패:', err);
            showErrorNotification('헬스체크에 실패했습니다');
        });
}

// 키보드 단축키
document.addEventListener('keydown', function(event) {
    // Ctrl + R 또는 F5: 새로고침
    if ((event.ctrlKey && event.key === 'r') || event.key === 'F5') {
        event.preventDefault();
        location.reload();
    }
    
    // Ctrl + S: 상태 확인
    if (event.ctrlKey && event.key === 's') {
        event.preventDefault();
        getStatus();
    }
    
    // Ctrl + H: 헬스체크
    if (event.ctrlKey && event.key === 'h') {
        event.preventDefault();
        getHealth();
    }
});

// 페이지 가시성 API를 사용한 자동 일시정지/재개
document.addEventListener('visibilitychange', function() {
    if (document.hidden) {
        // 페이지가 숨겨졌을 때 업데이트 일시정지
        clearInterval(window.statsUpdateInterval);
        console.log('페이지가 숨겨져 업데이트를 일시정지합니다');
    } else {
        // 페이지가 다시 보일 때 업데이트 재개
        updateStatistics(); // 즉시 한 번 업데이트
        window.statsUpdateInterval = setInterval(updateStatistics, 10000);
        console.log('페이지가 다시 보여 업데이트를 재개합니다');
    }
});

// 네트워크 상태 모니터링
window.addEventListener('online', function() {
    showSuccessNotification('네트워크 연결이 복구되었습니다');
    updateStatistics();
});

window.addEventListener('offline', function() {
    showErrorNotification('네트워크 연결이 끊어졌습니다');
});

// 초기화 함수
function initializeDashboard() {
    console.log('대시보드 초기화 중...');
    
    // 초기 통계 로드
    updateStatistics();
    
    // 주기적 업데이트 설정 (10초마다)
    window.statsUpdateInterval = setInterval(updateStatistics, 10000);
    
    // 5분마다 헬스체크
    setInterval(checkSystemHealth, 5 * 60 * 1000);
    
    console.log('대시보드 초기화 완료');
    showSuccessNotification('대시보드가 초기화되었습니다');
}

// DOM 로드 완료 후 초기화
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initializeDashboard);
} else {
    initializeDashboard();
}

// 전역 에러 핸들러
window.addEventListener('error', function(event) {
    console.error('전역 오류:', event.error);
    showErrorNotification('예기치 못한 오류가 발생했습니다');
});

// Promise rejection 핸들러
window.addEventListener('unhandledrejection', function(event) {
    console.error('처리되지 않은 Promise 거부:', event.reason);
    showErrorNotification('비동기 작업 중 오류가 발생했습니다');
});