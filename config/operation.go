package config

import (
	"fmt"
	"time"
)

// IsOperatingTime 현재 시간이 운영 시간인지 확인
func (c *Config) IsOperatingTime(currentTime time.Time) bool {
	hour := currentTime.Hour()
	minute := currentTime.Minute()

	startHour := c.OperatingStartHour
	startMinute := c.OperatingStartMinute
	endHour := c.OperatingEndHour
	endMinute := c.OperatingEndMinute

	// 현재 시간을 분 단위로 변환 (하루 = 1440분)
	currentMinutes := hour*60 + minute
	startMinutes := startHour*60 + startMinute
	endMinutes := endHour*60 + endMinute

	// 시작 시간이 종료 시간보다 작은 경우 (예: 06:00 ~ 10:00)
	if startMinutes < endMinutes {
		return currentMinutes >= startMinutes && currentMinutes < endMinutes
	}

	// 시작 시간이 종료 시간보다 큰 경우 (예: 22:00 ~ 06:00, 자정을 넘어가는 경우)
	if startMinutes > endMinutes {
		return currentMinutes >= startMinutes || currentMinutes < endMinutes
	}

	// 시작 시간과 종료 시간이 같은 경우 (24시간 운영)
	return true
}

// GetNextOperatingTime 다음 운영 시작 시간 반환
func (c *Config) GetNextOperatingTime(currentTime time.Time) time.Time {
	startHour := c.OperatingStartHour
	startMinute := c.OperatingStartMinute

	// 당일 시작 시간 계산
	todayStart := time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day(),
		startHour, startMinute, 0, 0, currentTime.Location())

	// 현재 시간이 당일 시작 시간 이전이면 당일 시작 시간 반환
	if currentTime.Before(todayStart) {
		return todayStart
	}

	// 그렇지 않으면 다음날 시작 시간 반환
	nextDay := currentTime.AddDate(0, 0, 1)
	return time.Date(nextDay.Year(), nextDay.Month(), nextDay.Day(),
		startHour, startMinute, 0, 0, currentTime.Location())
}

// GetOperatingDuration 하루 운영 시간 길이 계산
func (c *Config) GetOperatingDuration() time.Duration {
	startMinutes := c.OperatingStartHour*60 + c.OperatingStartMinute
	endMinutes := c.OperatingEndHour*60 + c.OperatingEndMinute

	var durationMinutes int

	// 시작 시간이 종료 시간보다 작은 경우 (예: 06:00 ~ 22:00)
	if startMinutes < endMinutes {
		durationMinutes = endMinutes - startMinutes
	} else if startMinutes > endMinutes {
		// 자정을 넘어가는 경우 (예: 22:00 ~ 06:00)
		durationMinutes = (24*60 - startMinutes) + endMinutes
	} else {
		// 24시간 운영
		durationMinutes = 24 * 60
	}

	return time.Duration(durationMinutes) * time.Minute
}

// GetOperatingScheduleString 운영시간을 문자열로 반환
func (c *Config) GetOperatingScheduleString() string {
	if c.OperatingStartHour == c.OperatingEndHour && c.OperatingStartMinute == c.OperatingEndMinute {
		return "24시간 운영"
	}

	return fmt.Sprintf("%02d:%02d ~ %02d:%02d",
		c.OperatingStartHour, c.OperatingStartMinute,
		c.OperatingEndHour, c.OperatingEndMinute)
}

// IsWithinOperatingHours 지정된 시간이 운영시간 범위 내인지 확인
func (c *Config) IsWithinOperatingHours(checkTime time.Time) (bool, time.Duration) {
	if c.IsOperatingTime(checkTime) {
		return true, 0
	}

	nextOperatingTime := c.GetNextOperatingTime(checkTime)
	timeUntilNext := nextOperatingTime.Sub(checkTime)

	return false, timeUntilNext
}

// GetDailyOperatingDate 운영일자 계산 (운영시간 기준)
// 예: 04:55~01:00 운영시간의 경우, 새벽 1:30은 전날 운영일자에 속함
func (c *Config) GetDailyOperatingDate(now time.Time) string {
	// 현재 시간이 운영 종료 시간 이후이고 다음 운영 시작 시간 이전이면 전날로 계산
	if !c.IsOperatingTime(now) {
		nextOperatingTime := c.GetNextOperatingTime(now)

		// 다음 운영 시간이 다음날이면 현재는 전날 운영일자
		if nextOperatingTime.Day() != now.Day() {
			return now.AddDate(0, 0, -1).Format("2006-01-02")
		}
	}

	return now.Format("2006-01-02")
}
