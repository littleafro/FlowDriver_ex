package app

import (
	"fmt"
	"net"

	"github.com/NullLatency/flow-driver/internal/config"
	"github.com/NullLatency/flow-driver/internal/netutil"
)

func EvaluateRawIPPolicy(cfg *config.AppConfig, policy *netutil.DialPolicy, addr string) (isRaw bool, allowed bool, err error) {
	host, _, splitErr := net.SplitHostPort(addr)
	if splitErr != nil {
		return false, false, splitErr
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false, false, nil
	}
	allowed = policy != nil && policy.RawIPAllowed(ip)
	if cfg.RawIPRejected() && !allowed {
		return true, allowed, fmt.Errorf("raw IP targets rejected by policy: %s", addr)
	}
	return true, allowed, nil
}
