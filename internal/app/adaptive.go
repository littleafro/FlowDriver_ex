package app

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/NullLatency/flow-driver/internal/config"
	"github.com/NullLatency/flow-driver/internal/httpclient"
	"github.com/NullLatency/flow-driver/internal/netutil"
	"github.com/NullLatency/flow-driver/internal/storage"
	"github.com/NullLatency/flow-driver/internal/transport"
)

type AdaptiveMode string

const (
	AdaptiveInteractive  AdaptiveMode = "interactive"
	AdaptiveBulk         AdaptiveMode = "bulk"
	AdaptiveQuotaBackoff AdaptiveMode = "quota_backoff"
)

type AdaptiveTransportProfile struct {
	TargetIP   string `json:"target_ip,omitempty"`
	SNI        string `json:"sni,omitempty"`
	HostHeader string `json:"host_header,omitempty"`
	TokenURL   string `json:"token_url,omitempty"`
}

type AdaptiveBackendState struct {
	Score           float64                  `json:"score"`
	LastGoodProfile AdaptiveTransportProfile `json:"last_good_profile"`
}

type AdaptiveState struct {
	Mode          AdaptiveMode                    `json:"mode"`
	UpdatedUnixMs int64                           `json:"updated_unix_ms"`
	Tuning        transport.RuntimeTuning         `json:"tuning"`
	Backends      map[string]AdaptiveBackendState `json:"backends"`
}

type AdaptiveController struct {
	mu        sync.Mutex
	statePath string
	state     AdaptiveState
	engine    *transport.Engine
	policy    *netutil.DialPolicy
	pool      *storage.BackendPool
	lastStats transport.StatsSnapshot
}

func NewAdaptiveController(dataDir string) (*AdaptiveController, error) {
	statePath := filepath.Join(dataDir, "adaptive_state.json")
	state := defaultAdaptiveState()
	if data, err := os.ReadFile(statePath); err == nil {
		if err := json.Unmarshal(data, &state); err != nil {
			return nil, fmt.Errorf("decode adaptive state: %w", err)
		}
	}
	return &AdaptiveController{
		statePath: statePath,
		state:     state,
	}, nil
}

func defaultAdaptiveState() AdaptiveState {
	return AdaptiveState{
		Mode:          AdaptiveInteractive,
		UpdatedUnixMs: time.Now().UnixMilli(),
		Tuning:        adaptiveTuningForMode(AdaptiveInteractive),
		Backends:      make(map[string]AdaptiveBackendState),
	}
}

func (c *AdaptiveController) InitialEngineOptions(cfg *config.AppConfig) transport.Options {
	opts := transport.DefaultOptions()
	opts.DataDir = cfg.DataDir
	opts.ClientID = cfg.ClientID
	opts.ServerID = cfg.ServerID
	opts.TunnelID = cfg.TunnelID
	opts.SessionIdleTimeout = 10 * time.Minute
	opts.CleanupInterval = 20 * time.Second
	opts.HeartbeatInterval = 20 * time.Second
	c.applyTuningToOptions(&opts, c.state.Tuning)
	opts.ApplyDefaults()
	return opts
}

func (c *AdaptiveController) Attach(engine *transport.Engine, pool *storage.BackendPool, policy *netutil.DialPolicy) {
	c.mu.Lock()
	c.engine = engine
	c.pool = pool
	c.policy = policy
	c.mu.Unlock()
	engine.ApplyRuntimeTuning(c.state.Tuning)
	engine.SetBackendSelector(c.selectBackend)
	c.applyPolicyTuning(c.state.Tuning)
}

func (c *AdaptiveController) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				c.persist()
				return
			case <-ticker.C:
				c.tick()
			}
		}
	}()
}

func (c *AdaptiveController) tick() {
	c.mu.Lock()
	engine := c.engine
	c.mu.Unlock()
	if engine == nil {
		return
	}

	stats := engine.StatsSnapshot()
	c.mu.Lock()
	prev := c.lastStats
	c.mu.Unlock()
	mode := chooseAdaptiveMode(stats, prev)
	tuning := adaptiveTuningForMode(mode)

	c.mu.Lock()
	c.state.Mode = mode
	c.state.Tuning = tuning
	c.state.UpdatedUnixMs = time.Now().UnixMilli()
	if c.state.Backends == nil {
		c.state.Backends = make(map[string]AdaptiveBackendState)
	}
	for name, backendStats := range stats.Backends {
		c.state.Backends[name] = AdaptiveBackendState{
			Score:           scoreBackend(backendStats),
			LastGoodProfile: c.state.Backends[name].LastGoodProfile,
		}
	}
	c.lastStats = stats
	c.mu.Unlock()

	engine.ApplyRuntimeTuning(tuning)
	c.applyPolicyTuning(tuning)
	c.persist()
}

func (c *AdaptiveController) selectBackend(s *transport.Session, candidates []*storage.BackendHandle) (*storage.BackendHandle, error) {
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no candidate backends available")
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	var (
		bestScore = math.Inf(-1)
		bestSet   []*storage.BackendHandle
	)
	for _, candidate := range candidates {
		score := 100.0
		if state, ok := c.state.Backends[candidate.Name]; ok {
			score = state.Score
		}
		if backendStats, ok := c.lastStats.Backends[candidate.Name]; ok {
			score -= float64(backendStats.PendingChunks) * 1.5
			score -= backendStats.ManifestLag.Seconds() * 3
			score -= backendStats.LastPublishLatency.Seconds() * 3
			score -= backendStats.LastDownloadLatency.Seconds() * 3
		}
		switch candidate.Health() {
		case storage.HealthHealthy:
			score += 10
		case storage.HealthDegraded:
			score += 2
		case storage.HealthRateLimited:
			score -= 25
		case storage.HealthAuthFailed, storage.HealthDisabled:
			score = math.Inf(-1)
		}
		if score > bestScore+1 {
			bestScore = score
			bestSet = []*storage.BackendHandle{candidate}
			continue
		}
		if math.Abs(score-bestScore) <= 1 {
			bestSet = append(bestSet, candidate)
		}
	}
	if len(bestSet) == 0 {
		return nil, fmt.Errorf("no selectable backends available")
	}
	if len(bestSet) == 1 {
		return bestSet[0], nil
	}

	best := bestSet[0]
	bestRank := adaptiveBackendRank(sessionBackendKey(s), best.Name)
	for _, candidate := range bestSet[1:] {
		rank := adaptiveBackendRank(sessionBackendKey(s), candidate.Name)
		if rank < bestRank {
			best = candidate
			bestRank = rank
		}
	}
	return best, nil
}

func sessionBackendKey(s *transport.Session) string {
	if s == nil {
		return ""
	}
	if s.ClientID != "" {
		return s.ClientID + ":" + s.ID
	}
	return s.ID
}

func adaptiveBackendRank(sessionKey, backendName string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(sessionKey))
	_, _ = h.Write([]byte{':'})
	_, _ = h.Write([]byte(backendName))
	return h.Sum64()
}

func (c *AdaptiveController) RecordBackendProfile(name string, profile AdaptiveTransportProfile) {
	c.mu.Lock()
	if c.state.Backends == nil {
		c.state.Backends = make(map[string]AdaptiveBackendState)
	}
	state := c.state.Backends[name]
	state.LastGoodProfile = profile
	c.state.Backends[name] = state
	c.state.UpdatedUnixMs = time.Now().UnixMilli()
	c.mu.Unlock()
}

func (c *AdaptiveController) TransportProfile(name string) (AdaptiveTransportProfile, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	state, ok := c.state.Backends[name]
	if !ok {
		return AdaptiveTransportProfile{}, false
	}
	if state.LastGoodProfile.HostHeader == "" && state.LastGoodProfile.SNI == "" && state.LastGoodProfile.TargetIP == "" {
		return AdaptiveTransportProfile{}, false
	}
	return state.LastGoodProfile, true
}

func (c *AdaptiveController) persist() {
	c.mu.Lock()
	payload, err := json.MarshalIndent(c.state, "", "  ")
	path := c.statePath
	c.mu.Unlock()
	if err != nil {
		log.Printf("warning: failed to encode adaptive state: %v", err)
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		log.Printf("warning: failed to create adaptive state dir: %v", err)
		return
	}
	if err := os.WriteFile(path, payload, 0644); err != nil {
		log.Printf("warning: failed to save adaptive state: %v", err)
	}
}

func (c *AdaptiveController) applyTuningToOptions(opts *transport.Options, tuning transport.RuntimeTuning) {
	if opts == nil {
		return
	}
	if tuning.PollRate > 0 {
		opts.PollRate = tuning.PollRate
	}
	if tuning.ActivePollRate > 0 {
		opts.ActivePollRate = tuning.ActivePollRate
	}
	if tuning.IdlePollRate > 0 {
		opts.IdlePollRate = tuning.IdlePollRate
	}
	if tuning.FlushRate > 0 {
		opts.FlushRate = tuning.FlushRate
	}
	if tuning.UploadInterval > 0 {
		opts.UploadInterval = tuning.UploadInterval
	}
	if tuning.SegmentBytes > 0 {
		opts.SegmentBytes = tuning.SegmentBytes
	}
	if tuning.MaxSegmentBytes > 0 {
		opts.MaxSegmentBytes = tuning.MaxSegmentBytes
	}
	if tuning.MaxMuxSegments > 0 {
		opts.MaxMuxSegments = tuning.MaxMuxSegments
	}
	if tuning.MaxConcurrentUploads > 0 {
		opts.MaxConcurrentUploads = tuning.MaxConcurrentUploads
	}
	if tuning.MaxConcurrentDownloads > 0 {
		opts.MaxConcurrentDownloads = tuning.MaxConcurrentDownloads
	}
	if tuning.MaxConcurrentDeletes > 0 {
		opts.MaxConcurrentDeletes = tuning.MaxConcurrentDeletes
	}
	if tuning.MaxPendingChunksPerBackend > 0 {
		opts.MaxPendingSegmentsPerSession = tuning.MaxPendingChunksPerBackend
	}
	if tuning.MaxTxBufferBytesPerSession > 0 {
		opts.MaxTxBufferBytesPerSession = tuning.MaxTxBufferBytesPerSession
	}
	if tuning.MaxOutOfOrderSegments > 0 {
		opts.MaxOutOfOrderSegments = tuning.MaxOutOfOrderSegments
	}
	if tuning.Compression != "" {
		opts.Compression = tuning.Compression
	}
	if tuning.CompressionMinBytes > 0 {
		opts.CompressionMinBytes = tuning.CompressionMinBytes
	}
	if tuning.GapGracePeriod > 0 {
		opts.GapGracePeriod = tuning.GapGracePeriod
	}
}

func (c *AdaptiveController) applyPolicyTuning(tuning transport.RuntimeTuning) {
	c.mu.Lock()
	policy := c.policy
	mode := c.state.Mode
	c.mu.Unlock()
	if policy == nil {
		return
	}
	switch mode {
	case AdaptiveBulk:
		policy.SetTiming(15*time.Second, 30*time.Second)
	case AdaptiveQuotaBackoff:
		policy.SetTiming(25*time.Second, 45*time.Second)
	default:
		policy.SetTiming(12*time.Second, 20*time.Second)
	}
}

func chooseAdaptiveMode(stats, prev transport.StatsSnapshot) AdaptiveMode {
	if stats.UploadErrors > prev.UploadErrors || stats.DownloadErrors > prev.DownloadErrors || stats.ManifestErrors > prev.ManifestErrors {
		return AdaptiveQuotaBackoff
	}
	if stats.PendingChunks > 8 || stats.ActiveSessions >= 6 || stats.BytesC2S+stats.BytesS2C > 8*1024*1024 {
		return AdaptiveBulk
	}
	return AdaptiveInteractive
}

func adaptiveTuningForMode(mode AdaptiveMode) transport.RuntimeTuning {
	switch mode {
	case AdaptiveBulk:
		return transport.RuntimeTuning{
			PollRate:                   350 * time.Millisecond,
			ActivePollRate:             180 * time.Millisecond,
			IdlePollRate:               1200 * time.Millisecond,
			FlushRate:                  180 * time.Millisecond,
			UploadInterval:             100 * time.Millisecond,
			SegmentBytes:               128 * 1024,
			MaxSegmentBytes:            512 * 1024,
			MaxMuxSegments:             64,
			MaxConcurrentUploads:       8,
			MaxConcurrentDownloads:     8,
			MaxConcurrentDeletes:       4,
			MaxPendingChunksPerBackend: 512,
			MaxTxBufferBytesPerSession: 8 * 1024 * 1024,
			MaxOutOfOrderSegments:      128,
			Compression:                "gzip",
			CompressionMinBytes:        8192,
			GapGracePeriod:             30 * time.Second,
		}
	case AdaptiveQuotaBackoff:
		return transport.RuntimeTuning{
			PollRate:                   1200 * time.Millisecond,
			ActivePollRate:             600 * time.Millisecond,
			IdlePollRate:               3 * time.Second,
			FlushRate:                  600 * time.Millisecond,
			UploadInterval:             400 * time.Millisecond,
			SegmentBytes:               64 * 1024,
			MaxSegmentBytes:            128 * 1024,
			MaxMuxSegments:             16,
			MaxConcurrentUploads:       2,
			MaxConcurrentDownloads:     2,
			MaxConcurrentDeletes:       1,
			MaxPendingChunksPerBackend: 128,
			MaxTxBufferBytesPerSession: 2 * 1024 * 1024,
			MaxOutOfOrderSegments:      32,
			Compression:                "gzip",
			CompressionMinBytes:        2048,
			GapGracePeriod:             45 * time.Second,
		}
	default:
		return transport.RuntimeTuning{
			PollRate:                   250 * time.Millisecond,
			ActivePollRate:             120 * time.Millisecond,
			IdlePollRate:               1000 * time.Millisecond,
			FlushRate:                  120 * time.Millisecond,
			UploadInterval:             80 * time.Millisecond,
			SegmentBytes:               32 * 1024,
			MaxSegmentBytes:            128 * 1024,
			MaxMuxSegments:             24,
			MaxConcurrentUploads:       4,
			MaxConcurrentDownloads:     4,
			MaxConcurrentDeletes:       2,
			MaxPendingChunksPerBackend: 256,
			MaxTxBufferBytesPerSession: 4 * 1024 * 1024,
			MaxOutOfOrderSegments:      64,
			Compression:                "off",
			CompressionMinBytes:        4096,
			GapGracePeriod:             20 * time.Second,
		}
	}
}

func scoreBackend(stats transport.BackendStats) float64 {
	score := 100.0
	score -= float64(stats.PendingChunks) * 2
	score -= float64(stats.UploadErrors+stats.DownloadErrors+stats.DeleteErrors+stats.ManifestErrors) * 25
	score -= stats.ManifestLag.Seconds() * 5
	score -= stats.LastDownloadLatency.Seconds() * 10
	score -= stats.LastPublishLatency.Seconds() * 10
	return score
}

func adaptiveTransportCandidates(profile AdaptiveTransportProfile) []AdaptiveTransportProfile {
	candidates := make([]AdaptiveTransportProfile, 0, 3)
	if profile != (AdaptiveTransportProfile{}) {
		candidates = append(candidates, profile)
	}
	candidates = append(candidates,
		AdaptiveTransportProfile{
			SNI:        "www.googleapis.com",
			HostHeader: "www.googleapis.com",
			TokenURL:   "https://www.googleapis.com/oauth2/v4/token",
		},
		AdaptiveTransportProfile{
			TargetIP:   "216.239.38.120:443",
			SNI:        "google.com",
			HostHeader: "www.googleapis.com",
			TokenURL:   "https://www.googleapis.com/oauth2/v4/token",
		},
	)
	return candidates
}

func adaptiveTransportPinned(cfg *config.AppConfig, backendCfg config.GoogleBackendConfig) bool {
	return backendCfg.TokenURL != "" ||
		cfg.TokenURL != "" ||
		hasExplicitFronting(cfg.Transport) ||
		hasExplicitFronting(cfg.TokenTransport) ||
		hasExplicitFronting(backendCfg.Transport) ||
		hasExplicitFronting(backendCfg.TokenTransport)
}

func hasExplicitFronting(transportCfg httpclient.TransportConfig) bool {
	return transportCfg.TargetIP != "" || transportCfg.SNI != "" || transportCfg.HostHeader != ""
}

func adaptiveConfiguredProfile(cfg *config.AppConfig, backendCfg config.GoogleBackendConfig) AdaptiveTransportProfile {
	apiTransport, tokenTransport := googleTransports(cfg, backendCfg)
	return AdaptiveTransportProfile{
		TargetIP:   apiTransport.TargetIP,
		SNI:        apiTransport.SNI,
		HostHeader: apiTransport.HostHeader,
		TokenURL:   googleTokenURL(cfg, backendCfg, tokenTransport),
	}
}

func BuildAdaptiveBackendPool(ctx context.Context, cfg *config.AppConfig, configPath, defaultCredentialsPath string, controller *AdaptiveController) (*storage.BackendPool, error) {
	if cfg.StorageType == "local" {
		return BuildBackendPool(ctx, cfg, configPath, defaultCredentialsPath)
	}

	backends := cfg.EnabledGoogleBackends()
	if len(backends) == 0 {
		return nil, fmt.Errorf("no enabled google backends configured")
	}

	handles := make([]*storage.BackendHandle, 0, len(backends))
	updatedConfig := false
	foldersByID := make(map[string]string, len(backends))
	var failed []string

	for i, backendCfg := range backends {
		creds := resolveCredentialsPath(backendCfg.CredentialsPath)
		if creds == "" {
			creds = resolveCredentialsPath(defaultCredentialsPath)
		}

		profile, _ := controller.TransportProfile(backendCfg.Name)
		candidates := adaptiveTransportCandidates(profile)
		if adaptiveTransportPinned(cfg, backendCfg) {
			candidates = []AdaptiveTransportProfile{adaptiveConfiguredProfile(cfg, backendCfg)}
		}
		var (
			handle   *storage.BackendHandle
			loginErr error
			chosen   AdaptiveTransportProfile
		)
		for _, candidate := range candidates {
			forceHTTP2 := true
			apiTransport := httpclient.TransportConfig{
				TargetIP:   candidate.TargetIP,
				SNI:        candidate.SNI,
				HostHeader: candidate.HostHeader,
				ForceHTTP2: &forceHTTP2,
			}
			tokenTransport := apiTransport
			apiTransport.ApplyDefaults()
			tokenTransport.ApplyDefaults()
			tokenURL := candidate.TokenURL
			if tokenURL == "" {
				tokenURL = "https://www.googleapis.com/oauth2/v4/token"
			}

			gb := storage.NewGoogleBackendWithOptions(storage.GoogleBackendOptions{
				Name:            backendCfg.Name,
				CredentialsPath: creds,
				FolderID:        backendCfg.FolderID,
				TokenURL:        tokenURL,
				APITransport:    apiTransport,
				TokenTransport:  tokenTransport,
				Retry: storage.RetryConfig{
					MinBackoffMs:           config.DefaultRetryMinBackoffMs,
					MaxBackoffMs:           config.DefaultRetryMaxBackoffMs,
					BackoffMultiplier:      config.DefaultRetryBackoffMultiplier,
					JitterPercent:          config.DefaultRetryJitterPercent,
					MaxRetriesPerOperation: config.DefaultRetryMaxRetriesPerOperation,
				},
			})
			tmpHandle := &storage.BackendHandle{
				Name:    backendCfg.Name,
				Weight:  1,
				Backend: gb,
			}
			gb.SetHealthSink(tmpHandle.SetHealth)
			if err := gb.Login(ctx); err != nil {
				loginErr = err
				tmpHandle.SetHealth(storage.HealthAuthFailed)
				continue
			}
			handle = tmpHandle
			chosen = candidate
			break
		}
		if handle == nil {
			log.Printf("warning: adaptive backend %s login failed and will be skipped: %v", backendCfg.Name, loginErr)
			failed = append(failed, fmt.Sprintf("%s: %v", backendCfg.Name, loginErr))
			continue
		}
		controller.RecordBackendProfile(backendCfg.Name, chosen)
		if adaptiveTransportPinned(cfg, backendCfg) {
			log.Printf("adaptive backend %s using pinned transport target_ip=%q sni=%q host_header=%q token_url=%q", backendCfg.Name, chosen.TargetIP, chosen.SNI, chosen.HostHeader, chosen.TokenURL)
		} else {
			log.Printf("adaptive backend %s selected transport target_ip=%q sni=%q host_header=%q token_url=%q", backendCfg.Name, chosen.TargetIP, chosen.SNI, chosen.HostHeader, chosen.TokenURL)
		}

		gb := handle.Backend.(*storage.GoogleBackend)
		if backendCfg.FolderID == "" {
			folderID, err := gb.FindFolder(ctx, "Flow-Data")
			if err != nil {
				return nil, fmt.Errorf("backend %s folder lookup failed: %w", backendCfg.Name, err)
			}
			if folderID == "" {
				folderID, err = gb.CreateFolder(ctx, "Flow-Data")
				if err != nil {
					return nil, fmt.Errorf("backend %s folder creation failed: %w", backendCfg.Name, err)
				}
			}
			backends[i].FolderID = folderID
			for idx := range cfg.GoogleBackends {
				if cfg.GoogleBackends[idx].Name == backendCfg.Name {
					cfg.GoogleBackends[idx].FolderID = folderID
					updatedConfig = true
				}
			}
			if len(cfg.GoogleBackends) == 0 {
				cfg.GoogleFolderID = folderID
				updatedConfig = true
			}
		} else if err := gb.ValidateFolder(ctx); err != nil {
			return nil, fmt.Errorf("backend %s folder validation failed: %w", backendCfg.Name, err)
		}

		if err := trackFolderAssignment(foldersByID, backendCfg.Name, backends[i].FolderID); err != nil {
			return nil, err
		}

		log.Printf("adaptive backend %s ready folder=%s", backendCfg.Name, backends[i].FolderID)
		handles = append(handles, handle)
	}

	if updatedConfig {
		if err := cfg.Save(configPath); err != nil {
			log.Printf("warning: failed to persist auto-discovered folder IDs: %v", err)
		}
	}
	if len(handles) == 0 {
		if len(failed) > 0 {
			return nil, fmt.Errorf("all enabled adaptive backends failed to initialize: %s", strings.Join(failed, "; "))
		}
		return nil, fmt.Errorf("no enabled adaptive backends initialized")
	}

	controller.persist()
	return storage.NewBackendPool(handles...), nil
}
