package cache

import (
	"context"
	"fmt"
	"sync"
	"time"

	"bus-tracker/config"
	"bus-tracker/internal/utils"
)

// DailyCacheRefreshManager 일일 캐시 새로고침 관리자 (V2)
type DailyCacheRefreshManager struct {
	config          *config.Config
	logger          *utils.Logger
	redisCache      *RedisStationCacheServiceV2 // V2 구체 타입 사용 (상세 기능 필요)
	allRouteIDs     []string
	lastRefreshDate string
	ctx             context.Context
	cancel          context.CancelFunc
	mutex           sync.RWMutex
	isRefreshing    bool
}

// NewDailyCacheRefreshManager 새로운 일일 캐시 새로고침 관리자 생성 (V2)
func NewDailyCacheRefreshManager(cfg *config.Config, logger *utils.Logger, redisCache *RedisStationCacheServiceV2, routeIDs []string) *DailyCacheRefreshManager {
	ctx, cancel := context.WithCancel(context.Background())

	return &DailyCacheRefreshManager{
		config:          cfg,
		logger:          logger,
		redisCache:      redisCache,
		allRouteIDs:     routeIDs,
		lastRefreshDate: "",
		ctx:             ctx,
		cancel:          cancel,
		isRefreshing:    false,
	}
}

// Start 일일 캐시 새로고침 워커 시작
func (dcrm *DailyCacheRefreshManager) Start() {
	go dcrm.runDailyCacheRefreshWorker()
}

// Stop 일일 캐시 새로고침 워커 정지
func (dcrm *DailyCacheRefreshManager) Stop() {
	dcrm.cancel()
}

// runDailyCacheRefreshWorker 일일 캐시 새로고침 워커
func (dcrm *DailyCacheRefreshManager) runDailyCacheRefreshWorker() {
	dcrm.logger.Info("🔄 일일 캐시 새로고침 워커 V2 시작")

	ticker := time.NewTicker(1 * time.Minute) // 1분마다 체크
	defer ticker.Stop()

	// 초기 실행: 프로그램 시작 시 한 번 체크
	dcrm.checkAndRefreshCache()

	for {
		select {
		case <-dcrm.ctx.Done():
			dcrm.logger.Info("🔄 일일 캐시 새로고침 워커 V2 종료")
			return
		case <-ticker.C:
			dcrm.checkAndRefreshCache()
		}
	}
}

// checkAndRefreshCache 캐시 새로고침 필요 여부 체크 및 실행
func (dcrm *DailyCacheRefreshManager) checkAndRefreshCache() {
	now := time.Now()
	currentDate := dcrm.getDailyOperatingDate(now)

	// 운영시간 내에서만 새로고침 수행
	if !dcrm.config.IsOperatingTime(now) {
		return
	}

	dcrm.mutex.Lock()
	defer dcrm.mutex.Unlock()

	// 이미 새로고침 중이거나 오늘 이미 새로고침했으면 건너뛰기
	if dcrm.isRefreshing || dcrm.lastRefreshDate == currentDate {
		return
	}

	// 운영 시작 시간에 가까운지 확인 (시작 후 30분 이내)
	if dcrm.isWithinRefreshWindow(now) {
		dcrm.logger.Infof("🗓️ 새로운 운영일 감지 (%s) - 캐시 V2 새로고침 시작", currentDate)
		dcrm.performCacheRefresh(currentDate)
	}
}

// isWithinRefreshWindow 캐시 새로고침 시간 윈도우 내인지 확인
func (dcrm *DailyCacheRefreshManager) isWithinRefreshWindow(now time.Time) bool {
	// 운영 시작 시간 계산
	operatingStart := time.Date(now.Year(), now.Month(), now.Day(),
		dcrm.config.OperatingStartHour, dcrm.config.OperatingStartMinute, 0, 0, now.Location())

	// 자정을 넘어가는 운영시간인 경우 (예: 22:00~06:00)
	if dcrm.config.OperatingStartHour > dcrm.config.OperatingEndHour {
		// 현재 시간이 운영 종료 시간 이전이면 전날 운영 시작 시간 사용
		if now.Hour() < dcrm.config.OperatingEndHour {
			operatingStart = operatingStart.AddDate(0, 0, -1)
		}
	}

	// 운영 시작 후 30분 이내인지 확인
	refreshWindow := 30 * time.Minute
	return now.After(operatingStart) && now.Before(operatingStart.Add(refreshWindow))
}

// performCacheRefresh 실제 캐시 새로고침 수행
func (dcrm *DailyCacheRefreshManager) performCacheRefresh(currentDate string) {
	dcrm.isRefreshing = true
	defer func() {
		dcrm.isRefreshing = false
	}()

	refreshStart := time.Now()
	dcrm.logger.Info("🧹 캐시 V2 전체 초기화 시작...")

	// 1. 기존 캐시 완전 초기화 (V2)
	if err := dcrm.redisCache.ClearCache(); err != nil {
		dcrm.logger.Errorf("캐시 V2 초기화 실패: %v", err)
		return
	}

	dcrm.logger.Info("📦 정류소 정보 새로 로드 시작... (V2 구조)")

	// 2. 정류소 정보 새로 로드 (V2)
	if err := dcrm.redisCache.LoadStationCache(dcrm.allRouteIDs); err != nil {
		dcrm.logger.Errorf("정류소 캐시 V2 새로고침 실패: %v", err)
		return
	}

	// 3. 새로고침 완료 처리
	dcrm.lastRefreshDate = currentDate
	refreshDuration := time.Since(refreshStart)

	// 4. 캐시 상태 출력 (V2)
	dcrm.redisCache.PrintCacheStatus()

	dcrm.logger.Infof("✅ 일일 캐시 V2 새로고침 완료 - 운영일: %s, 소요시간: %v",
		currentDate, refreshDuration)

	// 5. 통계 정보 출력 (V2) - 상세 통계 사용
	l1Routes, l1Stations, redisKeys := dcrm.redisCache.GetCacheStatisticsV2()
	dcrm.logger.Infof("📊 새로고침 후 캐시 V2 현황 - L1: %d노선/%d정류소, Redis: %d키",
		l1Routes, l1Stations, redisKeys)
}

// ForceRefresh 강제 캐시 새로고침 (관리용)
func (dcrm *DailyCacheRefreshManager) ForceRefresh() error {
	dcrm.mutex.Lock()
	defer dcrm.mutex.Unlock()

	if dcrm.isRefreshing {
		return fmt.Errorf("이미 새로고침 진행 중입니다")
	}

	dcrm.logger.Info("🔧 강제 캐시 V2 새로고침 요청")
	currentDate := dcrm.getDailyOperatingDate(time.Now())
	dcrm.performCacheRefresh(currentDate)

	return nil
}

// GetLastRefreshDate 마지막 새로고침 날짜 반환
func (dcrm *DailyCacheRefreshManager) GetLastRefreshDate() string {
	dcrm.mutex.RLock()
	defer dcrm.mutex.RUnlock()
	return dcrm.lastRefreshDate
}

// IsRefreshing 현재 새로고침 중인지 확인
func (dcrm *DailyCacheRefreshManager) IsRefreshing() bool {
	dcrm.mutex.RLock()
	defer dcrm.mutex.RUnlock()
	return dcrm.isRefreshing
}

// getDailyOperatingDate 운영일자 계산 (config의 로직과 동일)
func (dcrm *DailyCacheRefreshManager) getDailyOperatingDate(now time.Time) string {
	if !dcrm.config.IsOperatingTime(now) {
		nextOperatingTime := dcrm.config.GetNextOperatingTime(now)
		if nextOperatingTime.Day() != now.Day() {
			return now.AddDate(0, 0, -1).Format("2006-01-02")
		}
	}
	return now.Format("2006-01-02")
}

// GetRefreshStatus 새로고침 상태 정보 반환
func (dcrm *DailyCacheRefreshManager) GetRefreshStatus() map[string]interface{} {
	dcrm.mutex.RLock()
	defer dcrm.mutex.RUnlock()

	status := map[string]interface{}{
		"lastRefreshDate": dcrm.lastRefreshDate,
		"isRefreshing":    dcrm.isRefreshing,
		"totalRoutes":     len(dcrm.allRouteIDs),
		"currentDate":     dcrm.getDailyOperatingDate(time.Now()),
		"cacheVersion":    "V2",
	}

	// 다음 새로고침 예상 시간 계산
	now := time.Now()
	nextOperatingTime := dcrm.config.GetNextOperatingTime(now)
	status["nextRefreshTime"] = nextOperatingTime.Format("2006-01-02 15:04:05")

	// 캐시 히트율 정보 추가 (V2)
	if hitRateInfo := dcrm.redisCache.GetCacheHitRate(); hitRateInfo != nil {
		status["cacheStats"] = hitRateInfo
	}

	return status
}

// GetRefreshWindowInfo 새로고침 윈도우 정보 반환 (디버깅용)
func (dcrm *DailyCacheRefreshManager) GetRefreshWindowInfo() map[string]interface{} {
	now := time.Now()
	operatingStart := time.Date(now.Year(), now.Month(), now.Day(),
		dcrm.config.OperatingStartHour, dcrm.config.OperatingStartMinute, 0, 0, now.Location())

	// 자정을 넘어가는 운영시간 처리
	if dcrm.config.OperatingStartHour > dcrm.config.OperatingEndHour {
		if now.Hour() < dcrm.config.OperatingEndHour {
			operatingStart = operatingStart.AddDate(0, 0, -1)
		}
	}

	refreshWindow := 30 * time.Minute
	windowEnd := operatingStart.Add(refreshWindow)

	return map[string]interface{}{
		"operatingStartTime": operatingStart.Format("2006-01-02 15:04:05"),
		"refreshWindowStart": operatingStart.Format("2006-01-02 15:04:05"),
		"refreshWindowEnd":   windowEnd.Format("2006-01-02 15:04:05"),
		"currentTime":        now.Format("2006-01-02 15:04:05"),
		"isInWindow":         dcrm.isWithinRefreshWindow(now),
		"isOperatingTime":    dcrm.config.IsOperatingTime(now),
		"cacheVersion":       "V2",
	}
}
