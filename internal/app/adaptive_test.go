package app

import (
	"testing"

	"github.com/NullLatency/flow-driver/internal/config"
	"github.com/NullLatency/flow-driver/internal/httpclient"
)

func TestAdaptiveInitialEngineOptionsIgnoreManualTuning(t *testing.T) {
	t.Parallel()

	controller, err := NewAdaptiveController(t.TempDir())
	if err != nil {
		t.Fatalf("NewAdaptiveController: %v", err)
	}

	cfg := &config.AppConfig{
		DataDir:                      t.TempDir(),
		ClientID:                     "c1",
		ServerID:                     "s1",
		TunnelID:                     "main",
		PollRateMs:                   9999,
		ActivePollRateMs:             9999,
		IdlePollRateMs:               9999,
		FlushRateMs:                  9999,
		SegmentBytes:                 999999,
		MaxSegmentBytes:              999999,
		MaxMuxSegments:               1,
		MaxConcurrentUploads:         1,
		MaxConcurrentDownloads:       1,
		MaxConcurrentDeletes:         1,
		MaxPendingSegmentsPerSession: 1,
		MaxTxBufferBytesPerSession:   1024,
		MaxOutOfOrderSegments:        1,
		Compression:                  "gzip",
		CompressionMinBytes:          1,
		UploadIntervalMs:             9999,
	}

	opts := controller.InitialEngineOptions(cfg)
	if opts.DataDir != cfg.DataDir || opts.ClientID != cfg.ClientID || opts.ServerID != cfg.ServerID || opts.TunnelID != cfg.TunnelID {
		t.Fatalf("adaptive mode should preserve authoritative identity fields")
	}
	if opts.PollRate.Milliseconds() == int64(cfg.PollRateMs) || opts.SegmentBytes == cfg.SegmentBytes || opts.MaxConcurrentUploads == cfg.MaxConcurrentUploads {
		t.Fatalf("adaptive mode should ignore manual performance tuning fields")
	}
}

func TestAdaptiveTransportCandidatesPreferPersistedProfile(t *testing.T) {
	t.Parallel()

	profile := AdaptiveTransportProfile{
		TargetIP:   "1.2.3.4:443",
		SNI:        "google.com",
		HostHeader: "www.googleapis.com",
		TokenURL:   "https://www.googleapis.com/oauth2/v4/token",
	}
	candidates := adaptiveTransportCandidates(profile)
	if len(candidates) == 0 {
		t.Fatalf("expected candidates")
	}
	if candidates[0] != profile {
		t.Fatalf("expected persisted profile to be first candidate")
	}
}

func TestAdaptiveTransportPinnedByExplicitFronting(t *testing.T) {
	t.Parallel()

	cfg := &config.AppConfig{
	}
	backendCfg := config.GoogleBackendConfig{
		Transport: httpclient.TransportConfig{
			TargetIP:   "216.239.38.120:443",
			SNI:        "www.google.com",
			HostHeader: "www.googleapis.com",
		},
		TokenURL: "https://www.googleapis.com/oauth2/v4/token",
	}
	if !adaptiveTransportPinned(cfg, backendCfg) {
		t.Fatalf("expected explicit backend fronting settings to pin adaptive transport")
	}
	profile := adaptiveConfiguredProfile(cfg, backendCfg)
	if profile.TargetIP != "216.239.38.120:443" || profile.SNI != "www.google.com" || profile.HostHeader != "www.googleapis.com" {
		t.Fatalf("unexpected pinned profile: %+v", profile)
	}
	if profile.TokenURL != "https://www.googleapis.com/oauth2/v4/token" {
		t.Fatalf("unexpected token URL %q", profile.TokenURL)
	}
}
