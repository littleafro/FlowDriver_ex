package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/NullLatency/flow-driver/internal/httpclient"
)

const (
	DefaultListenAddr                    = "127.0.0.1:1080"
	DefaultStorageType                   = "google"
	DefaultDataDir                       = "./data"
	DefaultTunnelID                      = "main"
	DefaultSessionIdleTimeoutMs          = 300000
	DefaultStaleUnackedFileTTLms         = 1800000
	DefaultTombstoneTTLms                = 300000
	DefaultCleanupIntervalMs             = 30000
	DefaultHeartbeatIntervalMs           = 30000
	DefaultPollRateMs                    = 750
	DefaultFlushRateMs                   = 500
	DefaultActivePollRateMs              = 500
	DefaultIdlePollRateMs                = 3000
	DefaultSegmentBytes                  = 65536
	DefaultMaxSegmentBytes               = 262144
	DefaultMaxMuxSegments                = 32
	DefaultMaxConcurrentUploads          = 4
	DefaultMaxConcurrentDownloads        = 4
	DefaultMaxConcurrentDeletes          = 2
	DefaultMaxPendingSegmentsPerSession  = 256
	DefaultMaxTxBufferBytesPerSession    = 4 * 1024 * 1024
	DefaultMaxOutOfOrderSegments         = 64
	DefaultAckEverySegments              = 8
	DefaultAckIntervalMs                 = 1000
	DefaultCompression                   = "off"
	DefaultCompressionMinBytes           = 4096
	DefaultDialTimeoutMs                 = 30000
	DefaultTCPKeepAliveMs                = 30000
	DefaultWarnRawIP                     = true
	DefaultBlockPrivateIPs               = true
	DefaultRetryMinBackoffMs             = 500
	DefaultRetryMaxBackoffMs             = 30000
	DefaultRetryBackoffMultiplier        = 2.0
	DefaultRetryJitterPercent            = 25
	DefaultRetryMaxRetriesPerOperation   = 6
	DefaultRetryForeverForPendingUploads = true
	DefaultBackendWeight                 = 1
)

type RetryConfig struct {
	MinBackoffMs                  int     `json:"min_backoff_ms,omitempty"`
	MaxBackoffMs                  int     `json:"max_backoff_ms,omitempty"`
	BackoffMultiplier             float64 `json:"backoff_multiplier,omitempty"`
	JitterPercent                 int     `json:"jitter_percent,omitempty"`
	MaxRetriesPerOperation        int     `json:"max_retries_per_operation,omitempty"`
	RetryForeverForPendingUploads *bool   `json:"retry_forever_for_pending_uploads,omitempty"`
}

type RateLimitConfig struct {
	MaxReadsPerSecond       float64 `json:"max_reads_per_second,omitempty"`
	MaxWritesPerSecond      float64 `json:"max_writes_per_second,omitempty"`
	MaxDeletesPerSecond     float64 `json:"max_deletes_per_second,omitempty"`
	MaxRequestsPerMinute    float64 `json:"max_requests_per_minute,omitempty"`
	MaxDailyUploadBytes     int64   `json:"max_daily_upload_bytes,omitempty"`
	MaxDailyDownloadBytes   int64   `json:"max_daily_download_bytes,omitempty"`
	StopWhenBudgetExhausted bool    `json:"stop_when_budget_exhausted,omitempty"`
}

type ReliabilityConfig struct {
	SessionIdleTimeoutMs  int `json:"session_idle_timeout_ms,omitempty"`
	StaleUnackedFileTTLms int `json:"stale_unacked_file_ttl_ms,omitempty"`
	TombstoneTTLms        int `json:"tombstone_ttl_ms,omitempty"`
	CleanupIntervalMs     int `json:"cleanup_interval_ms,omitempty"`
	HeartbeatIntervalMs   int `json:"heartbeat_interval_ms,omitempty"`
	AckEverySegments      int `json:"ack_every_segments,omitempty"`
	AckIntervalMs         int `json:"ack_interval_ms,omitempty"`
	MaxOutOfOrderSegments int `json:"max_out_of_order_segments,omitempty"`
}

type PerformanceConfig struct {
	PollRateMs                   int    `json:"poll_rate_ms,omitempty"`
	FlushRateMs                  int    `json:"flush_rate_ms,omitempty"`
	ActivePollRateMs             int    `json:"active_poll_rate_ms,omitempty"`
	IdlePollRateMs               int    `json:"idle_poll_rate_ms,omitempty"`
	SegmentBytes                 int    `json:"segment_bytes,omitempty"`
	MaxSegmentBytes              int    `json:"max_segment_bytes,omitempty"`
	MaxMuxSegments               int    `json:"max_mux_segments,omitempty"`
	MaxConcurrentUploads         int    `json:"max_concurrent_uploads,omitempty"`
	MaxConcurrentDownloads       int    `json:"max_concurrent_downloads,omitempty"`
	MaxConcurrentDeletes         int    `json:"max_concurrent_deletes,omitempty"`
	MaxPendingSegmentsPerSession int    `json:"max_pending_segments_per_session,omitempty"`
	MaxTxBufferBytesPerSession   int    `json:"max_tx_buffer_bytes_per_session,omitempty"`
	Compression                  string `json:"compression,omitempty"`
	CompressionMinBytes          int    `json:"compression_min_bytes,omitempty"`
}

type SecurityConfig struct {
	RejectRawIP       *bool    `json:"reject_raw_ip,omitempty"`
	WarnRawIP         *bool    `json:"warn_raw_ip,omitempty"`
	AllowedRawIPCidrs []string `json:"allowed_raw_ip_cidrs,omitempty"`
	DialTimeoutMs     int      `json:"dial_timeout_ms,omitempty"`
	TCPKeepAliveMs    int      `json:"tcp_keepalive_ms,omitempty"`
	BlockPrivateIPs   *bool    `json:"block_private_ips,omitempty"`
	AllowCIDRs        []string `json:"allow_cidrs,omitempty"`
	DenyCIDRs         []string `json:"deny_cidrs,omitempty"`
}

type GoogleBackendConfig struct {
	Name            string                     `json:"name,omitempty"`
	CredentialsPath string                     `json:"credentials_path,omitempty"`
	FolderID        string                     `json:"folder_id,omitempty"`
	TokenURL        string                     `json:"token_url,omitempty"`
	Weight          int                        `json:"weight,omitempty"`
	Enabled         *bool                      `json:"enabled,omitempty"`
	Transport       httpclient.TransportConfig `json:"transport,omitempty"`
	TokenTransport  httpclient.TransportConfig `json:"token_transport,omitempty"`
	RateLimits      RateLimitConfig            `json:"rate_limits,omitempty"`
}

// AppConfig defines the application-level configuration while staying backward
// compatible with the original flat config shape.
type AppConfig struct {
	ListenAddr     string `json:"listen_addr,omitempty"`
	ClientID       string `json:"client_id,omitempty"`
	ServerID       string `json:"server_id,omitempty"`
	TunnelID       string `json:"tunnel_id,omitempty"`
	StorageType    string `json:"storage_type,omitempty"`
	LocalDir       string `json:"local_dir,omitempty"`
	DataDir        string `json:"data_dir,omitempty"`
	GoogleFolderID string `json:"google_folder_id,omitempty"`

	RefreshRateMs int `json:"refresh_rate_ms,omitempty"`
	FlushRateMs   int `json:"flush_rate_ms,omitempty"`
	PollRateMs    int `json:"poll_rate_ms,omitempty"`

	SessionIdleTimeoutMs  int `json:"session_idle_timeout_ms,omitempty"`
	StaleUnackedFileTTLms int `json:"stale_unacked_file_ttl_ms,omitempty"`
	TombstoneTTLms        int `json:"tombstone_ttl_ms,omitempty"`
	CleanupIntervalMs     int `json:"cleanup_interval_ms,omitempty"`
	HeartbeatIntervalMs   int `json:"heartbeat_interval_ms,omitempty"`
	ActivePollRateMs      int `json:"active_poll_rate_ms,omitempty"`
	IdlePollRateMs        int `json:"idle_poll_rate_ms,omitempty"`
	SegmentBytes          int `json:"segment_bytes,omitempty"`
	MaxSegmentBytes       int `json:"max_segment_bytes,omitempty"`
	MaxMuxSegments        int `json:"max_mux_segments,omitempty"`

	MaxConcurrentUploads         int `json:"max_concurrent_uploads,omitempty"`
	MaxConcurrentDownloads       int `json:"max_concurrent_downloads,omitempty"`
	MaxConcurrentDeletes         int `json:"max_concurrent_deletes,omitempty"`
	MaxPendingSegmentsPerSession int `json:"max_pending_segments_per_session,omitempty"`
	MaxTxBufferBytesPerSession   int `json:"max_tx_buffer_bytes_per_session,omitempty"`
	MaxOutOfOrderSegments        int `json:"max_out_of_order_segments,omitempty"`
	AckEverySegments             int `json:"ack_every_segments,omitempty"`
	AckIntervalMs                int `json:"ack_interval_ms,omitempty"`

	Compression         string `json:"compression,omitempty"`
	CompressionMinBytes int    `json:"compression_min_bytes,omitempty"`

	RejectRawIP       *bool    `json:"reject_raw_ip,omitempty"`
	WarnRawIP         *bool    `json:"warn_raw_ip,omitempty"`
	AllowedRawIPCidrs []string `json:"allowed_raw_ip_cidrs,omitempty"`
	DialTimeoutMs     int      `json:"dial_timeout_ms,omitempty"`
	TCPKeepAliveMs    int      `json:"tcp_keepalive_ms,omitempty"`
	BlockPrivateIPs   *bool    `json:"block_private_ips,omitempty"`
	AllowCIDRs        []string `json:"allow_cidrs,omitempty"`
	DenyCIDRs         []string `json:"deny_cidrs,omitempty"`

	Transport      httpclient.TransportConfig `json:"transport,omitempty"`
	TokenTransport httpclient.TransportConfig `json:"token_transport,omitempty"`
	TokenURL       string                     `json:"token_url,omitempty"`
	Retry          RetryConfig                `json:"retry,omitempty"`
	RateLimits     RateLimitConfig            `json:"rate_limits,omitempty"`
	Reliability    ReliabilityConfig          `json:"reliability,omitempty"`
	Performance    PerformanceConfig          `json:"performance,omitempty"`
	Security       SecurityConfig             `json:"security,omitempty"`

	GoogleBackends []GoogleBackendConfig `json:"google_backends,omitempty"`
}

func boolPtr(v bool) *bool {
	return &v
}

// Save writes the config back to a JSON file.
func (c *AppConfig) Save(path string) error {
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0644)
}

// Load reads, parses, and normalizes a JSON config file.
func Load(path string) (*AppConfig, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	var cfg AppConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config JSON: %w", err)
	}
	cfg.Normalize()
	return &cfg, nil
}

// Normalize folds legacy fields into the newer config surface and applies sane defaults.
func (c *AppConfig) Normalize() {
	if c.StorageType == "" {
		c.StorageType = DefaultStorageType
	}
	if c.ListenAddr == "" {
		c.ListenAddr = DefaultListenAddr
	}
	if c.DataDir == "" {
		if c.LocalDir != "" {
			c.DataDir = c.LocalDir
		} else {
			c.DataDir = DefaultDataDir
		}
	}
	if c.TunnelID == "" {
		c.TunnelID = DefaultTunnelID
	}

	if c.PollRateMs == 0 {
		if c.RefreshRateMs > 0 {
			c.PollRateMs = c.RefreshRateMs
		} else if c.Performance.PollRateMs > 0 {
			c.PollRateMs = c.Performance.PollRateMs
		}
	}
	if c.FlushRateMs == 0 && c.Performance.FlushRateMs > 0 {
		c.FlushRateMs = c.Performance.FlushRateMs
	}

	if c.SessionIdleTimeoutMs == 0 && c.Reliability.SessionIdleTimeoutMs > 0 {
		c.SessionIdleTimeoutMs = c.Reliability.SessionIdleTimeoutMs
	}
	if c.StaleUnackedFileTTLms == 0 && c.Reliability.StaleUnackedFileTTLms > 0 {
		c.StaleUnackedFileTTLms = c.Reliability.StaleUnackedFileTTLms
	}
	if c.TombstoneTTLms == 0 && c.Reliability.TombstoneTTLms > 0 {
		c.TombstoneTTLms = c.Reliability.TombstoneTTLms
	}
	if c.CleanupIntervalMs == 0 && c.Reliability.CleanupIntervalMs > 0 {
		c.CleanupIntervalMs = c.Reliability.CleanupIntervalMs
	}
	if c.HeartbeatIntervalMs == 0 && c.Reliability.HeartbeatIntervalMs > 0 {
		c.HeartbeatIntervalMs = c.Reliability.HeartbeatIntervalMs
	}
	if c.AckEverySegments == 0 && c.Reliability.AckEverySegments > 0 {
		c.AckEverySegments = c.Reliability.AckEverySegments
	}
	if c.AckIntervalMs == 0 && c.Reliability.AckIntervalMs > 0 {
		c.AckIntervalMs = c.Reliability.AckIntervalMs
	}
	if c.MaxOutOfOrderSegments == 0 && c.Reliability.MaxOutOfOrderSegments > 0 {
		c.MaxOutOfOrderSegments = c.Reliability.MaxOutOfOrderSegments
	}

	if c.ActivePollRateMs == 0 && c.Performance.ActivePollRateMs > 0 {
		c.ActivePollRateMs = c.Performance.ActivePollRateMs
	}
	if c.IdlePollRateMs == 0 && c.Performance.IdlePollRateMs > 0 {
		c.IdlePollRateMs = c.Performance.IdlePollRateMs
	}
	if c.SegmentBytes == 0 && c.Performance.SegmentBytes > 0 {
		c.SegmentBytes = c.Performance.SegmentBytes
	}
	if c.MaxSegmentBytes == 0 && c.Performance.MaxSegmentBytes > 0 {
		c.MaxSegmentBytes = c.Performance.MaxSegmentBytes
	}
	if c.MaxMuxSegments == 0 && c.Performance.MaxMuxSegments > 0 {
		c.MaxMuxSegments = c.Performance.MaxMuxSegments
	}
	if c.MaxConcurrentUploads == 0 && c.Performance.MaxConcurrentUploads > 0 {
		c.MaxConcurrentUploads = c.Performance.MaxConcurrentUploads
	}
	if c.MaxConcurrentDownloads == 0 && c.Performance.MaxConcurrentDownloads > 0 {
		c.MaxConcurrentDownloads = c.Performance.MaxConcurrentDownloads
	}
	if c.MaxConcurrentDeletes == 0 && c.Performance.MaxConcurrentDeletes > 0 {
		c.MaxConcurrentDeletes = c.Performance.MaxConcurrentDeletes
	}
	if c.MaxPendingSegmentsPerSession == 0 && c.Performance.MaxPendingSegmentsPerSession > 0 {
		c.MaxPendingSegmentsPerSession = c.Performance.MaxPendingSegmentsPerSession
	}
	if c.MaxTxBufferBytesPerSession == 0 && c.Performance.MaxTxBufferBytesPerSession > 0 {
		c.MaxTxBufferBytesPerSession = c.Performance.MaxTxBufferBytesPerSession
	}
	if c.Compression == "" && c.Performance.Compression != "" {
		c.Compression = c.Performance.Compression
	}
	if c.CompressionMinBytes == 0 && c.Performance.CompressionMinBytes > 0 {
		c.CompressionMinBytes = c.Performance.CompressionMinBytes
	}

	if c.Security.RejectRawIP != nil && c.RejectRawIP == nil {
		c.RejectRawIP = c.Security.RejectRawIP
	}
	if c.Security.WarnRawIP != nil && c.WarnRawIP == nil {
		c.WarnRawIP = c.Security.WarnRawIP
	}
	if len(c.AllowedRawIPCidrs) == 0 && len(c.Security.AllowedRawIPCidrs) > 0 {
		c.AllowedRawIPCidrs = append([]string(nil), c.Security.AllowedRawIPCidrs...)
	}
	if c.DialTimeoutMs == 0 && c.Security.DialTimeoutMs > 0 {
		c.DialTimeoutMs = c.Security.DialTimeoutMs
	}
	if c.TCPKeepAliveMs == 0 && c.Security.TCPKeepAliveMs > 0 {
		c.TCPKeepAliveMs = c.Security.TCPKeepAliveMs
	}
	if c.Security.BlockPrivateIPs != nil && c.BlockPrivateIPs == nil {
		c.BlockPrivateIPs = c.Security.BlockPrivateIPs
	}
	if len(c.AllowCIDRs) == 0 && len(c.Security.AllowCIDRs) > 0 {
		c.AllowCIDRs = append([]string(nil), c.Security.AllowCIDRs...)
	}
	if len(c.DenyCIDRs) == 0 && len(c.Security.DenyCIDRs) > 0 {
		c.DenyCIDRs = append([]string(nil), c.Security.DenyCIDRs...)
	}

	if c.SessionIdleTimeoutMs <= 0 {
		c.SessionIdleTimeoutMs = DefaultSessionIdleTimeoutMs
	}
	if c.StaleUnackedFileTTLms <= 0 {
		c.StaleUnackedFileTTLms = DefaultStaleUnackedFileTTLms
	}
	if c.TombstoneTTLms <= 0 {
		c.TombstoneTTLms = DefaultTombstoneTTLms
	}
	if c.CleanupIntervalMs <= 0 {
		c.CleanupIntervalMs = DefaultCleanupIntervalMs
	}
	if c.HeartbeatIntervalMs <= 0 {
		c.HeartbeatIntervalMs = DefaultHeartbeatIntervalMs
	}
	if c.PollRateMs <= 0 {
		c.PollRateMs = DefaultPollRateMs
	}
	if c.FlushRateMs <= 0 {
		c.FlushRateMs = DefaultFlushRateMs
	}
	if c.ActivePollRateMs <= 0 {
		c.ActivePollRateMs = DefaultActivePollRateMs
	}
	if c.IdlePollRateMs <= 0 {
		c.IdlePollRateMs = DefaultIdlePollRateMs
	}
	if c.SegmentBytes <= 0 {
		c.SegmentBytes = DefaultSegmentBytes
	}
	if c.MaxSegmentBytes <= 0 {
		c.MaxSegmentBytes = DefaultMaxSegmentBytes
	}
	if c.MaxMuxSegments <= 0 {
		c.MaxMuxSegments = DefaultMaxMuxSegments
	}
	if c.MaxConcurrentUploads <= 0 {
		c.MaxConcurrentUploads = DefaultMaxConcurrentUploads
	}
	if c.MaxConcurrentDownloads <= 0 {
		c.MaxConcurrentDownloads = DefaultMaxConcurrentDownloads
	}
	if c.MaxConcurrentDeletes <= 0 {
		c.MaxConcurrentDeletes = DefaultMaxConcurrentDeletes
	}
	if c.MaxPendingSegmentsPerSession <= 0 {
		c.MaxPendingSegmentsPerSession = DefaultMaxPendingSegmentsPerSession
	}
	if c.MaxTxBufferBytesPerSession <= 0 {
		c.MaxTxBufferBytesPerSession = DefaultMaxTxBufferBytesPerSession
	}
	if c.MaxOutOfOrderSegments <= 0 {
		c.MaxOutOfOrderSegments = DefaultMaxOutOfOrderSegments
	}
	if c.AckEverySegments <= 0 {
		c.AckEverySegments = DefaultAckEverySegments
	}
	if c.AckIntervalMs <= 0 {
		c.AckIntervalMs = DefaultAckIntervalMs
	}
	if strings.TrimSpace(c.Compression) == "" {
		c.Compression = DefaultCompression
	}
	c.Compression = strings.ToLower(c.Compression)
	if c.CompressionMinBytes <= 0 {
		c.CompressionMinBytes = DefaultCompressionMinBytes
	}
	if c.RejectRawIP == nil {
		c.RejectRawIP = boolPtr(false)
	}
	if c.WarnRawIP == nil {
		c.WarnRawIP = boolPtr(DefaultWarnRawIP)
	}
	if c.DialTimeoutMs <= 0 {
		c.DialTimeoutMs = DefaultDialTimeoutMs
	}
	if c.TCPKeepAliveMs <= 0 {
		c.TCPKeepAliveMs = DefaultTCPKeepAliveMs
	}
	if c.BlockPrivateIPs == nil {
		c.BlockPrivateIPs = boolPtr(DefaultBlockPrivateIPs)
	}

	if c.Retry.MinBackoffMs <= 0 {
		c.Retry.MinBackoffMs = DefaultRetryMinBackoffMs
	}
	if c.Retry.MaxBackoffMs <= 0 {
		c.Retry.MaxBackoffMs = DefaultRetryMaxBackoffMs
	}
	if c.Retry.BackoffMultiplier <= 0 {
		c.Retry.BackoffMultiplier = DefaultRetryBackoffMultiplier
	}
	if c.Retry.JitterPercent <= 0 {
		c.Retry.JitterPercent = DefaultRetryJitterPercent
	}
	if c.Retry.MaxRetriesPerOperation <= 0 {
		c.Retry.MaxRetriesPerOperation = DefaultRetryMaxRetriesPerOperation
	}
	if c.Retry.RetryForeverForPendingUploads == nil {
		c.Retry.RetryForeverForPendingUploads = boolPtr(DefaultRetryForeverForPendingUploads)
	}

	c.Transport.ApplyDefaults()
	c.TokenTransport.ApplyDefaults()

	for i := range c.GoogleBackends {
		be := &c.GoogleBackends[i]
		if be.Name == "" {
			be.Name = fmt.Sprintf("g%d", i+1)
		}
		if be.Weight <= 0 {
			be.Weight = DefaultBackendWeight
		}
		if be.Enabled == nil {
			be.Enabled = boolPtr(true)
		}
		be.Transport.ApplyDefaults()
		be.TokenTransport.ApplyDefaults()
		if be.RateLimits.MaxRequestsPerMinute == 0 && c.RateLimits.MaxRequestsPerMinute > 0 {
			be.RateLimits.MaxRequestsPerMinute = c.RateLimits.MaxRequestsPerMinute
		}
		if be.RateLimits.MaxReadsPerSecond == 0 && c.RateLimits.MaxReadsPerSecond > 0 {
			be.RateLimits.MaxReadsPerSecond = c.RateLimits.MaxReadsPerSecond
		}
		if be.RateLimits.MaxWritesPerSecond == 0 && c.RateLimits.MaxWritesPerSecond > 0 {
			be.RateLimits.MaxWritesPerSecond = c.RateLimits.MaxWritesPerSecond
		}
		if be.RateLimits.MaxDeletesPerSecond == 0 && c.RateLimits.MaxDeletesPerSecond > 0 {
			be.RateLimits.MaxDeletesPerSecond = c.RateLimits.MaxDeletesPerSecond
		}
		if be.RateLimits.MaxDailyUploadBytes == 0 && c.RateLimits.MaxDailyUploadBytes > 0 {
			be.RateLimits.MaxDailyUploadBytes = c.RateLimits.MaxDailyUploadBytes
		}
		if be.RateLimits.MaxDailyDownloadBytes == 0 && c.RateLimits.MaxDailyDownloadBytes > 0 {
			be.RateLimits.MaxDailyDownloadBytes = c.RateLimits.MaxDailyDownloadBytes
		}
		if !be.RateLimits.StopWhenBudgetExhausted {
			be.RateLimits.StopWhenBudgetExhausted = c.RateLimits.StopWhenBudgetExhausted
		}
	}
}

func (c *AppConfig) RawIPRejected() bool {
	return c.RejectRawIP != nil && *c.RejectRawIP
}

func (c *AppConfig) RawIPWarned() bool {
	return c.WarnRawIP == nil || *c.WarnRawIP
}

func (c *AppConfig) PrivateIPsBlocked() bool {
	return c.BlockPrivateIPs == nil || *c.BlockPrivateIPs
}

func (c *AppConfig) RetryForeverForPendingUploads() bool {
	return c.Retry.RetryForeverForPendingUploads == nil || *c.Retry.RetryForeverForPendingUploads
}

func (c *AppConfig) EnabledGoogleBackends() []GoogleBackendConfig {
	if len(c.GoogleBackends) == 0 {
		if c.StorageType != "google" {
			return nil
		}
		return []GoogleBackendConfig{{
			Name:            "g1",
			CredentialsPath: "",
			FolderID:        c.GoogleFolderID,
			TokenURL:        c.TokenURL,
			Weight:          DefaultBackendWeight,
			Enabled:         boolPtr(true),
			Transport:       c.Transport,
			TokenTransport:  c.TokenTransport,
			RateLimits:      c.RateLimits,
		}}
	}

	out := make([]GoogleBackendConfig, 0, len(c.GoogleBackends))
	for _, be := range c.GoogleBackends {
		if be.Enabled != nil && !*be.Enabled {
			continue
		}
		out = append(out, be)
	}
	return out
}
