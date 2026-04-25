package app

import (
	"testing"

	"github.com/NullLatency/flow-driver/internal/config"
	"github.com/NullLatency/flow-driver/internal/netutil"
)

func TestEvaluateRawIPPolicyRejects(t *testing.T) {
	t.Parallel()

	reject := true
	cfg := &config.AppConfig{RejectRawIP: &reject}
	cidrs, err := netutil.ParseCIDRs([]string{"149.154.160.0/20"})
	if err != nil {
		t.Fatalf("parse cidrs: %v", err)
	}
	policy := &netutil.DialPolicy{AllowedRawCIDRs: cidrs}

	if _, _, err := EvaluateRawIPPolicy(cfg, policy, "8.8.8.8:443"); err == nil {
		t.Fatalf("expected raw IP rejection")
	}
	if isRaw, allowed, err := EvaluateRawIPPolicy(cfg, policy, "149.154.167.51:443"); err != nil || !isRaw || !allowed {
		t.Fatalf("expected allowed raw IP, got isRaw=%v allowed=%v err=%v", isRaw, allowed, err)
	}
}
