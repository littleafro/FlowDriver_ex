package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"
	"time"
)

type httpStatusError struct {
	Operation  string
	StatusCode int
	Body       string
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("%s returned %d: %s", e.Operation, e.StatusCode, e.Body)
}

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	var statusErr *httpStatusError
	if errors.As(err, &statusErr) {
		switch statusErr.StatusCode {
		case http.StatusTooManyRequests,
			http.StatusInternalServerError,
			http.StatusBadGateway,
			http.StatusServiceUnavailable,
			http.StatusGatewayTimeout:
			return true
		case http.StatusForbidden:
			body := strings.ToLower(statusErr.Body)
			return strings.Contains(body, "quota") ||
				strings.Contains(body, "rate") ||
				strings.Contains(body, "user rate limit") ||
				strings.Contains(body, "daily limit")
		default:
			return false
		}
	}

	if errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET) {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary()) {
		return true
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}

	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return dnsErr.IsTemporary || dnsErr.IsTimeout
	}

	return strings.Contains(strings.ToLower(err.Error()), "connection reset") ||
		strings.Contains(strings.ToLower(err.Error()), "tls") ||
		strings.Contains(strings.ToLower(err.Error()), "timeout") ||
		strings.Contains(strings.ToLower(err.Error()), "temporary") ||
		errors.Is(err, os.ErrDeadlineExceeded)
}

func IsNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	var statusErr *httpStatusError
	if errors.As(err, &statusErr) {
		return statusErr.StatusCode == http.StatusNotFound
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "not found") || strings.Contains(msg, "no such file")
}

func isUnauthorizedError(err error) bool {
	var statusErr *httpStatusError
	if errors.As(err, &statusErr) {
		if statusErr.StatusCode == http.StatusUnauthorized {
			return true
		}
		if statusErr.StatusCode == http.StatusForbidden {
			body := strings.ToLower(statusErr.Body)
			return strings.Contains(body, "insufficient authentication") ||
				strings.Contains(body, "invalid credentials") ||
				strings.Contains(body, "invalid_grant") ||
				strings.Contains(body, "permission denied")
		}
	}
	return false
}

func retryDelay(cfg RetryConfig, attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}
	min := time.Duration(cfg.MinBackoffMs) * time.Millisecond
	max := time.Duration(cfg.MaxBackoffMs) * time.Millisecond
	delay := float64(min)
	for i := 1; i < attempt; i++ {
		delay *= cfg.BackoffMultiplier
		if delay >= float64(max) {
			delay = float64(max)
			break
		}
	}
	jitter := 1.0
	if cfg.JitterPercent > 0 {
		span := float64(cfg.JitterPercent) / 100.0
		jitter = 1 - span + rand.Float64()*(span*2)
	}
	d := time.Duration(delay * jitter)
	if d < min {
		return min
	}
	if d > max {
		return max
	}
	return d
}

func waitForRetry(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
