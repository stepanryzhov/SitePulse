package monitor

import (
	"sync"
	"time"
)

type rateEntry struct {
	count int
	reset time.Time
}
type rateLimiter struct {
	mu      sync.Mutex
	entries map[string]rateEntry
}

func (l *rateLimiter) allow(key string, limit int, window time.Duration) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	for k, v := range l.entries {
		if now.After(v.reset) {
			delete(l.entries, k)
		}
	}
	e := l.entries[key]
	if e.reset.IsZero() {
		if len(l.entries) >= 10000 {
			return false
		}
		e.reset = now.Add(window)
	}
	e.count++
	l.entries[key] = e
	return e.count <= limit
}
