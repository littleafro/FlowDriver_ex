package transport

import "sync"

type dynamicLimiter struct {
	mu      sync.Mutex
	inFlight int
	limit    int
}

func newDynamicLimiter(limit int) *dynamicLimiter {
	if limit <= 0 {
		limit = 1
	}
	return &dynamicLimiter{limit: limit}
}

func (l *dynamicLimiter) TryAcquire() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.inFlight >= l.limit {
		return false
	}
	l.inFlight++
	return true
}

func (l *dynamicLimiter) Release() {
	l.mu.Lock()
	if l.inFlight > 0 {
		l.inFlight--
	}
	l.mu.Unlock()
}

func (l *dynamicLimiter) SetLimit(limit int) {
	if limit <= 0 {
		limit = 1
	}
	l.mu.Lock()
	l.limit = limit
	l.mu.Unlock()
}
