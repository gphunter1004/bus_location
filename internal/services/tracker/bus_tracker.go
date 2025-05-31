// internal/services/tracker/bus_tracker.go - 메인 파일 (분리 후)
package tracker

// 이 파일은 다른 분리된 파일들을 import하고 공통 타입과 인터페이스를 정의합니다.
// 실제 구현은 다음 파일들로 분리되었습니다:
//
// 1. bus_tracker_core.go - 핵심 추적 기능
//    - BusTrackingInfo 구조체
//    - BusTracker 구조체
//    - IsStationChanged, FilterChangedStations 등 핵심 추적 로직
//
// 2. bus_tracker_trip_management.go - 운행 차수 관리
//    - 일일 운행 차수 카운터 관리
//    - 운영일자 계산 및 리셋
//    - ResetDailyTripCounters, GetDailyTripStatistics 등
//
// 3. bus_tracker_cleanup.go - 정리 및 관리 기능
//    - CleanupMissingBuses (미목격 버스 정리)
//    - UpdateLastSeenTime, RemoveFromTracking 등
//    - 각종 조회 메서드들
//
// 4. bus_tracker_duplicate_check.go - 중복 체크 기능
//    - BusTrackerWithDuplicateCheck 구조체
//    - 첫 실행 시 ES 데이터 중복 체크
//    - FilterChangedStationsWithDuplicateCheck

// 분리된 파일들은 모두 같은 package tracker이므로
// 자동으로 함께 컴파일되며 서로의 메서드에 접근할 수 있습니다.

// 필요한 경우 여기에 공통 상수나 인터페이스를 정의할 수 있습니다.
const (
	// DefaultCleanupInterval 기본 정리 작업 주기
	DefaultCleanupInterval = 5 // 분

	// DefaultDisappearanceTimeout 기본 미목격 종료 시간
	DefaultDisappearanceTimeout = 10 // 분
)

// BusTrackerInterface 버스 트래커 인터페이스 (필요한 경우)
type BusTrackerInterface interface {
	IsStationChanged(plateNo string, currentPosition int64, routeNm string, totalStations int) (bool, int)
	UpdateLastSeenTime(plateNo string)
	GetTrackedBusCount() int
	CleanupMissingBuses(logger interface{}) int
}
