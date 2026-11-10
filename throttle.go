package main

import (
	"sync"
	"time"
)

// logThrottle lets one log line through per interval and counts the ones it
// held back, so a condition that hits every request (a saturated ClickHouse
// timing out every /events read) is logged once a minute with a count instead
// of once per request.
type logThrottle struct {
	mu         sync.Mutex
	interval   time.Duration
	last       time.Time
	suppressed int64
}

func newLogThrottle(interval time.Duration) *logThrottle {
	return &logThrottle{interval: interval}
}

// allow reports whether a line may be logged at now and, when it may, how many
// were suppressed since the last one that was.
func (t *logThrottle) allow(now time.Time) (bool, int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.last.IsZero() && now.Sub(t.last) < t.interval {
		t.suppressed++
		return false, 0
	}
	n := t.suppressed
	t.last = now
	t.suppressed = 0
	return true, n
}
