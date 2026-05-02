package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadBackwardCompatibleConfig(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.json")
	payload := []byte(`{
  "listen_addr": "127.0.0.1:1081",
  "storage_type": "google",
  "google_folder_id": "folder-1",
  "refresh_rate_ms": 1234,
  "flush_rate_ms": 4321,
  "transport": {
    "TargetIP": "216.239.38.120:443",
    "SNI": "www.googleapis.com",
    "HostHeader": "www.googleapis.com"
  }
}`)
	if err := os.WriteFile(configPath, payload, 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.PollRateMs != 1234 {
		t.Fatalf("PollRateMs = %d, want 1234", cfg.PollRateMs)
	}
	if cfg.ActivePollRateMs != 1234 {
		t.Fatalf("ActivePollRateMs = %d, want 1234", cfg.ActivePollRateMs)
	}
	if cfg.IdlePollRateMs != 1234 {
		t.Fatalf("IdlePollRateMs = %d, want 1234", cfg.IdlePollRateMs)
	}
	if cfg.FlushRateMs != 4321 {
		t.Fatalf("FlushRateMs = %d, want 4321", cfg.FlushRateMs)
	}
	if cfg.Transport.TargetIP != "216.239.38.120:443" {
		t.Fatalf("TargetIP = %q", cfg.Transport.TargetIP)
	}
	backends := cfg.EnabledGoogleBackends()
	if len(backends) != 1 {
		t.Fatalf("expected 1 google backend, got %d", len(backends))
	}
	if backends[0].FolderID != "folder-1" {
		t.Fatalf("FolderID = %q, want folder-1", backends[0].FolderID)
	}
}
