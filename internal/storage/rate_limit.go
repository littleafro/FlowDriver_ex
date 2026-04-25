package storage

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

var errRateBudgetExhausted = fmt.Errorf("rate limit budget exhausted")

type tokenBucket struct {
	rate     float64
	capacity float64

	mu         sync.Mutex
	tokens     float64
	lastRefill time.Time
}

func newTokenBucket(rate float64) *tokenBucket {
	if rate <= 0 {
		return nil
	}
	return &tokenBucket{
		rate:       rate,
		capacity:   rate,
		tokens:     rate,
		lastRefill: time.Now(),
	}
}

func (b *tokenBucket) wait(ctx context.Context, amount float64) error {
	if b == nil || amount <= 0 {
		return nil
	}

	for {
		b.mu.Lock()
		now := time.Now()
		elapsed := now.Sub(b.lastRefill).Seconds()
		if elapsed > 0 {
			b.tokens += elapsed * b.rate
			if b.tokens > b.capacity {
				b.tokens = b.capacity
			}
			b.lastRefill = now
		}
		if b.tokens >= amount {
			b.tokens -= amount
			b.mu.Unlock()
			return nil
		}
		missing := amount - b.tokens
		waitFor := time.Duration((missing / b.rate) * float64(time.Second))
		if waitFor < 10*time.Millisecond {
			waitFor = 10 * time.Millisecond
		}
		b.mu.Unlock()

		timer := time.NewTimer(waitFor)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

type dailyBudget struct {
	limit int64

	mu      sync.Mutex
	day     string
	current int64
}

func newDailyBudget(limit int64) *dailyBudget {
	if limit <= 0 {
		return nil
	}
	return &dailyBudget{
		limit: limit,
		day:   time.Now().UTC().Format("2006-01-02"),
	}
}

func (b *dailyBudget) reserve(bytes int64) error {
	if b == nil || bytes <= 0 {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	today := time.Now().UTC().Format("2006-01-02")
	if b.day != today {
		b.day = today
		b.current = 0
	}
	if b.current+bytes > b.limit {
		return errRateBudgetExhausted
	}
	b.current += bytes
	return nil
}

type requestRateLimiter struct {
	name   string
	config RateLimitConfig

	reads    *tokenBucket
	writes   *tokenBucket
	deletes  *tokenBucket
	requests *tokenBucket
	upload   *dailyBudget
	download *dailyBudget
}

func newRequestRateLimiter(name string, cfg RateLimitConfig) *requestRateLimiter {
	return &requestRateLimiter{
		name:     name,
		config:   cfg,
		reads:    newTokenBucket(cfg.MaxReadsPerSecond),
		writes:   newTokenBucket(cfg.MaxWritesPerSecond),
		deletes:  newTokenBucket(cfg.MaxDeletesPerSecond),
		requests: newTokenBucket(cfg.MaxRequestsPerMinute / 60.0),
		upload:   newDailyBudget(cfg.MaxDailyUploadBytes),
		download: newDailyBudget(cfg.MaxDailyDownloadBytes),
	}
}

func (r *requestRateLimiter) wait(ctx context.Context, op string) error {
	if r == nil {
		return nil
	}
	if err := r.requests.wait(ctx, 1); err != nil {
		return err
	}

	var bucket *tokenBucket
	switch op {
	case "list", "download":
		bucket = r.reads
	case "upload", "put", "token":
		bucket = r.writes
	case "delete":
		bucket = r.deletes
	}
	if bucket == nil {
		return nil
	}

	start := time.Now()
	if err := bucket.wait(ctx, 1); err != nil {
		return err
	}
	if waited := time.Since(start); waited >= 50*time.Millisecond {
		log.Printf("backend %s rate limiter wait for %s: %s", r.name, op, waited)
	}
	return nil
}

func (r *requestRateLimiter) reserveUpload(bytes int64) error {
	if r == nil {
		return nil
	}
	if err := r.upload.reserve(bytes); err != nil && r.config.StopWhenBudgetExhausted {
		return err
	}
	return nil
}

func (r *requestRateLimiter) reserveDownload(bytes int64) error {
	if r == nil {
		return nil
	}
	if err := r.download.reserve(bytes); err != nil && r.config.StopWhenBudgetExhausted {
		return err
	}
	return nil
}
