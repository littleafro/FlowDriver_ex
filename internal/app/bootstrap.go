package app

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/NullLatency/flow-driver/internal/config"
	"github.com/NullLatency/flow-driver/internal/httpclient"
	"github.com/NullLatency/flow-driver/internal/storage"
	"github.com/NullLatency/flow-driver/internal/transport"
)

func BuildEngineOptions(cfg *config.AppConfig) transport.Options {
	opts := transport.Options{
		DataDir:                      cfg.DataDir,
		ClientID:                     cfg.ClientID,
		ServerID:                     cfg.ServerID,
		TunnelID:                     cfg.TunnelID,
		SessionIdleTimeout:           time.Duration(cfg.SessionIdleTimeoutMs) * time.Millisecond,
		CleanupInterval:              time.Duration(cfg.CleanupIntervalMs) * time.Millisecond,
		HeartbeatInterval:            time.Duration(cfg.HeartbeatIntervalMs) * time.Millisecond,
		PollRate:                     time.Duration(cfg.PollRateMs) * time.Millisecond,
		ActivePollRate:               time.Duration(cfg.ActivePollRateMs) * time.Millisecond,
		IdlePollRate:                 time.Duration(cfg.IdlePollRateMs) * time.Millisecond,
		FlushRate:                    time.Duration(cfg.FlushRateMs) * time.Millisecond,
		SegmentBytes:                 cfg.SegmentBytes,
		MaxSegmentBytes:              cfg.MaxSegmentBytes,
		MaxMuxSegments:               cfg.MaxMuxSegments,
		MaxConcurrentUploads:         cfg.MaxConcurrentUploads,
		MaxConcurrentDownloads:       cfg.MaxConcurrentDownloads,
		MaxConcurrentDeletes:         cfg.MaxConcurrentDeletes,
		MaxPendingSegmentsPerSession: cfg.MaxPendingSegmentsPerSession,
		MaxTxBufferBytesPerSession:   cfg.MaxTxBufferBytesPerSession,
		MaxOutOfOrderSegments:        cfg.MaxOutOfOrderSegments,
		Compression:                  cfg.Compression,
		CompressionMinBytes:          cfg.CompressionMinBytes,
		UploadInterval:               time.Duration(cfg.UploadIntervalMs) * time.Millisecond,
	}
	opts.ApplyDefaults()
	return opts
}

func BuildBackendPool(ctx context.Context, cfg *config.AppConfig, configPath, defaultCredentialsPath string) (*storage.BackendPool, error) {
	if cfg.StorageType == "local" {
		localDir := cfg.LocalDir
		if localDir == "" {
			localDir = cfg.DataDir
		}
		backend, err := storage.NewLocalBackend(localDir)
		if err != nil {
			return nil, err
		}
		if err := backend.Login(ctx); err != nil {
			return nil, err
		}
		return storage.NewSingleBackendPool("local", backend), nil
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
		creds := backendCfg.CredentialsPath
		if creds == "" {
			creds = defaultCredentialsPath
		}
		apiTransport, tokenTransport := googleTransports(cfg, backendCfg)
		warnGoogleTransport(backendCfg.Name, apiTransport, googleTokenURL(cfg, backendCfg, tokenTransport), tokenTransport)
		gb := storage.NewGoogleBackendWithOptions(storage.GoogleBackendOptions{
			Name:            backendCfg.Name,
			CredentialsPath: creds,
			FolderID:        backendCfg.FolderID,
			TokenURL:        googleTokenURL(cfg, backendCfg, tokenTransport),
			APITransport:    apiTransport,
			TokenTransport:  tokenTransport,
			Retry: storage.RetryConfig{
				MinBackoffMs:           cfg.Retry.MinBackoffMs,
				MaxBackoffMs:           cfg.Retry.MaxBackoffMs,
				BackoffMultiplier:      cfg.Retry.BackoffMultiplier,
				JitterPercent:          cfg.Retry.JitterPercent,
				MaxRetriesPerOperation: cfg.Retry.MaxRetriesPerOperation,
			},
			RateLimits: storage.RateLimitConfig{
				MaxReadsPerSecond:       applyRateLimitScale(backendCfg.RateLimits.MaxReadsPerSecond, cfg.RateLimitScale),
				MaxWritesPerSecond:      applyRateLimitScale(backendCfg.RateLimits.MaxWritesPerSecond, cfg.RateLimitScale),
				MaxDeletesPerSecond:     applyRateLimitScale(backendCfg.RateLimits.MaxDeletesPerSecond, cfg.RateLimitScale),
				MaxRequestsPerMinute:    applyRateLimitScale(backendCfg.RateLimits.MaxRequestsPerMinute, cfg.RateLimitScale),
				MaxDailyUploadBytes:     applyRateLimitScaleInt(backendCfg.RateLimits.MaxDailyUploadBytes, cfg.RateLimitScale),
				MaxDailyDownloadBytes:   applyRateLimitScaleInt(backendCfg.RateLimits.MaxDailyDownloadBytes, cfg.RateLimitScale),
				StopWhenBudgetExhausted: backendCfg.RateLimits.StopWhenBudgetExhausted,
			},
		})

		handle := &storage.BackendHandle{
			Name:    backendCfg.Name,
			Weight:  backendCfg.Weight,
			Backend: gb,
		}
		gb.SetHealthSink(handle.SetHealth)

		if err := gb.Login(ctx); err != nil {
			handle.SetHealth(storage.HealthAuthFailed)
			log.Printf("warning: backend %s login failed and will be skipped: %v", backendCfg.Name, err)
			failed = append(failed, fmt.Sprintf("%s: %v", backendCfg.Name, err))
			continue
		}

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

		log.Printf("backend %s ready folder=%s weight=%d", backendCfg.Name, backends[i].FolderID, backendCfg.Weight)
		handles = append(handles, handle)
	}

	if updatedConfig {
		if err := cfg.Save(configPath); err != nil {
			log.Printf("warning: failed to persist auto-discovered folder IDs: %v", err)
		}
	}
	if len(handles) == 0 {
		if len(failed) > 0 {
			return nil, fmt.Errorf("all enabled backends failed to initialize: %s", strings.Join(failed, "; "))
		}
		return nil, fmt.Errorf("no enabled google backends initialized")
	}

	return storage.NewBackendPool(handles...), nil
}

func trackFolderAssignment(foldersByID map[string]string, backendName, folderID string) error {
	if folderID == "" {
		return nil
	}
	if other, exists := foldersByID[folderID]; exists {
		return fmt.Errorf("backend %s reuses folder_id %q already assigned to backend %s; each enabled backend must use a distinct Drive folder", backendName, folderID, other)
	}
	foldersByID[folderID] = backendName
	return nil
}

func googleTransports(cfg *config.AppConfig, backendCfg config.GoogleBackendConfig) (httpclient.TransportConfig, httpclient.TransportConfig) {
	apiTransport := overlayTransport(httpclient.TransportConfig{}, cfg.Transport)
	apiTransport = overlayTransport(apiTransport, backendCfg.Transport)
	apiTransport.ApplyDefaults()
	if apiTransport.SNI == "" {
		apiTransport.SNI = "www.googleapis.com"
	}
	if apiTransport.HostHeader == "" {
		apiTransport.HostHeader = "www.googleapis.com"
	}

	tokenTransport := apiTransport
	tokenTransport = overlayTransport(tokenTransport, cfg.TokenTransport)
	tokenTransport = overlayTransport(tokenTransport, backendCfg.TokenTransport)
	tokenTransport.ApplyDefaults()
	if tokenTransport.SNI == "" {
		tokenTransport.SNI = apiTransport.SNI
	}
	if tokenTransport.HostHeader == "" {
		tokenTransport.HostHeader = apiTransport.HostHeader
	}
	return apiTransport, tokenTransport
}

func googleTokenURL(cfg *config.AppConfig, backendCfg config.GoogleBackendConfig, tokenTransport httpclient.TransportConfig) string {
	if backendCfg.TokenURL != "" {
		return backendCfg.TokenURL
	}
	if cfg.TokenURL != "" {
		return cfg.TokenURL
	}

	host := tokenTransport.HostHeader
	if host == "" {
		host = tokenTransport.SNI
	}

	switch host {
	case "www.googleapis.com":
		return "https://www.googleapis.com/oauth2/v4/token"
	case "oauth2.googleapis.com":
		return "https://oauth2.googleapis.com/token"
	default:
		return ""
	}
}

func overlayTransport(base, override httpclient.TransportConfig) httpclient.TransportConfig {
	if override.TargetIP != "" {
		base.TargetIP = override.TargetIP
	}
	if override.SNI != "" {
		base.SNI = override.SNI
	}
	if override.HostHeader != "" {
		base.HostHeader = override.HostHeader
	}
	if override.InsecureSkipVerify {
		base.InsecureSkipVerify = true
	}
	if override.ConnectTimeoutMs > 0 {
		base.ConnectTimeoutMs = override.ConnectTimeoutMs
	}
	if override.TLSHandshakeTimeoutMs > 0 {
		base.TLSHandshakeTimeoutMs = override.TLSHandshakeTimeoutMs
	}
	if override.RequestTimeoutMs > 0 {
		base.RequestTimeoutMs = override.RequestTimeoutMs
	}
	if override.IdleConnTimeoutMs > 0 {
		base.IdleConnTimeoutMs = override.IdleConnTimeoutMs
	}
	if override.MaxIdleConns > 0 {
		base.MaxIdleConns = override.MaxIdleConns
	}
	if override.ForceHTTP2 != nil {
		base.ForceHTTP2 = override.ForceHTTP2
	}
	return base
}

func warnGoogleTransport(name string, apiTransport httpclient.TransportConfig, tokenURL string, tokenTransport httpclient.TransportConfig) {
	if host := strings.ToLower(strings.TrimSpace(apiTransport.HostHeader)); host != "" && !strings.Contains(host, "googleapis.com") {
		log.Printf("warning: backend %s transport.host_header=%q but Drive API requests go to www.googleapis.com; this often causes HTML 404 responses. For fronted setups use sni=google.com (or another fronting hostname) with host_header=www.googleapis.com", name, apiTransport.HostHeader)
	}

	if tokenURL == "" {
		return
	}
	u, err := url.Parse(tokenURL)
	if err != nil {
		return
	}
	tokenHost := strings.ToLower(u.Hostname())
	hostHeader := strings.ToLower(strings.TrimSpace(tokenTransport.HostHeader))
	if strings.Contains(tokenHost, "googleapis.com") && hostHeader != "" && !strings.Contains(hostHeader, "googleapis.com") {
		log.Printf("warning: backend %s token_url=%q but token_transport.host_header=%q; token host header usually needs to stay on a googleapis.com hostname", name, tokenURL, tokenTransport.HostHeader)
	}
}

func applyRateLimitScale(value float64, scale float64) float64 {
	if scale <= 0 || scale == 1.0 {
		return value
	}
	return value * scale
}

func applyRateLimitScaleInt(value int64, scale float64) int64 {
	if scale <= 0 || scale == 1.0 {
		return value
	}
	return int64(float64(value) * scale)
}
