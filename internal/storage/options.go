package storage

import (
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/NullLatency/flow-driver/internal/httpclient"
)

type HealthState string

const (
	HealthHealthy     HealthState = "healthy"
	HealthDegraded    HealthState = "degraded"
	HealthRateLimited HealthState = "rate_limited"
	HealthAuthFailed  HealthState = "auth_failed"
	HealthDisabled    HealthState = "disabled"
)

type RetryConfig struct {
	MinBackoffMs           int
	MaxBackoffMs           int
	BackoffMultiplier      float64
	JitterPercent          int
	MaxRetriesPerOperation int
}

type RateLimitConfig struct {
	MaxReadsPerSecond       float64
	MaxWritesPerSecond      float64
	MaxDeletesPerSecond     float64
	MaxRequestsPerMinute    float64
	MaxDailyUploadBytes     int64
	MaxDailyDownloadBytes   int64
	StopWhenBudgetExhausted bool
}

type GoogleBackendOptions struct {
	Name            string
	CredentialsPath string
	FolderID        string
	TokenURL        string
	APITransport    httpclient.TransportConfig
	TokenTransport  httpclient.TransportConfig
	Retry           RetryConfig
	RateLimits      RateLimitConfig
}

type BackendHandle struct {
	Name    string
	Weight  int
	Backend Backend

	mu     sync.RWMutex
	health HealthState
}

func (h *BackendHandle) Health() HealthState {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.health
}

func (h *BackendHandle) SetHealth(state HealthState) {
	h.mu.Lock()
	h.health = state
	h.mu.Unlock()
}

type BackendPool struct {
	mu       sync.RWMutex
	backends []*BackendHandle
	byName   map[string]*BackendHandle
}

func NewBackendPool(handles ...*BackendHandle) *BackendPool {
	p := &BackendPool{
		backends: make([]*BackendHandle, 0, len(handles)),
		byName:   make(map[string]*BackendHandle, len(handles)),
	}
	for _, handle := range handles {
		if handle == nil {
			continue
		}
		if handle.Weight <= 0 {
			handle.Weight = 1
		}
		if handle.health == "" {
			handle.health = HealthHealthy
		}
		p.backends = append(p.backends, handle)
		p.byName[handle.Name] = handle
	}
	sort.Slice(p.backends, func(i, j int) bool {
		return p.backends[i].Name < p.backends[j].Name
	})
	return p
}

func NewSingleBackendPool(name string, backend Backend) *BackendPool {
	return NewBackendPool(&BackendHandle{
		Name:    name,
		Weight:  1,
		Backend: backend,
		health:  HealthHealthy,
	})
}

func (p *BackendPool) Backends() []*BackendHandle {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]*BackendHandle, len(p.backends))
	copy(out, p.backends)
	return out
}

func (p *BackendPool) Get(name string) *BackendHandle {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.byName[name]
}

func (p *BackendPool) HealthyBackends() []*BackendHandle {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]*BackendHandle, 0, len(p.backends))
	for _, be := range p.backends {
		switch be.Health() {
		case HealthHealthy, HealthDegraded:
			out = append(out, be)
		}
	}
	if len(out) == 0 {
		for _, be := range p.backends {
			if be.Health() != HealthDisabled && be.Health() != HealthAuthFailed {
				out = append(out, be)
			}
		}
	}
	return out
}

func (p *BackendPool) Select(sessionKey string) (*BackendHandle, error) {
	candidates := p.HealthyBackends()
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no healthy backends available")
	}

	var (
		best      *BackendHandle
		bestScore = math.Inf(1)
	)
	for _, candidate := range candidates {
		score := rendezvousScore(sessionKey, candidate.Name, candidate.Weight)
		if score < bestScore {
			bestScore = score
			best = candidate
		}
	}
	if best == nil {
		return nil, fmt.Errorf("failed to select backend")
	}
	return best, nil
}
