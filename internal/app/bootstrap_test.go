package app

import (
	"strings"
	"testing"

	"github.com/NullLatency/flow-driver/internal/config"
	"github.com/NullLatency/flow-driver/internal/httpclient"
)

func TestGoogleTokenTransportFallsBackToAPITransport(t *testing.T) {
	cfg := &config.AppConfig{
		StorageType: "google",
		Transport: httpclient.TransportConfig{
			TargetIP:   "216.239.38.120:443",
			SNI:        "www.googleapis.com",
			HostHeader: "www.googleapis.com",
		},
	}
	cfg.Normalize()

	backends := cfg.EnabledGoogleBackends()
	if len(backends) != 1 {
		t.Fatalf("expected one backend, got %d", len(backends))
	}

	apiTransport, tokenTransport := googleTransports(cfg, backends[0])
	if tokenTransport.TargetIP != apiTransport.TargetIP {
		t.Fatalf("expected token transport target IP %q, got %q", apiTransport.TargetIP, tokenTransport.TargetIP)
	}
	if tokenTransport.SNI != "www.googleapis.com" {
		t.Fatalf("expected token transport SNI www.googleapis.com, got %q", tokenTransport.SNI)
	}
	if tokenTransport.HostHeader != "www.googleapis.com" {
		t.Fatalf("expected token transport host header www.googleapis.com, got %q", tokenTransport.HostHeader)
	}

	tokenURL := googleTokenURL(cfg, backends[0], tokenTransport)
	if tokenURL != "https://www.googleapis.com/oauth2/v4/token" {
		t.Fatalf("expected legacy fronted token URL, got %q", tokenURL)
	}
}

func TestGoogleTokenURLAllowsExplicitOverride(t *testing.T) {
	cfg := &config.AppConfig{
		StorageType: "google",
		TokenURL:    "https://example.invalid/custom-token",
		Transport: httpclient.TransportConfig{
			TargetIP:   "216.239.38.120:443",
			SNI:        "www.googleapis.com",
			HostHeader: "www.googleapis.com",
		},
	}
	cfg.Normalize()

	backends := cfg.EnabledGoogleBackends()
	if len(backends) != 1 {
		t.Fatalf("expected one backend, got %d", len(backends))
	}

	_, tokenTransport := googleTransports(cfg, backends[0])
	tokenURL := googleTokenURL(cfg, backends[0], tokenTransport)
	if tokenURL != "https://example.invalid/custom-token" {
		t.Fatalf("expected explicit token URL override, got %q", tokenURL)
	}
}

func TestTrackFolderAssignmentRejectsDuplicates(t *testing.T) {
	foldersByID := map[string]string{
		"shared-folder": "g1",
	}

	err := trackFolderAssignment(foldersByID, "g2", "shared-folder")
	if err == nil {
		t.Fatalf("expected duplicate folder_id error")
	}
	if !strings.Contains(err.Error(), "reuses folder_id") {
		t.Fatalf("unexpected error: %v", err)
	}
}
