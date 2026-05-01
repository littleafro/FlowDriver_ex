package netutil

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"time"
)

const (
	maxDialCandidates        = 6
	minPerAttemptDialTimeout = 2 * time.Second
	maxPerAttemptDialTimeout = 5 * time.Second
)

type DialPolicy struct {
	AllowCIDRs      []*net.IPNet
	DenyCIDRs       []*net.IPNet
	AllowedRawCIDRs []*net.IPNet
	BlockPrivateIPs bool
	DialTimeout     time.Duration
	KeepAlive       time.Duration
	mu              sync.RWMutex
}

func ParseCIDRs(values []string) ([]*net.IPNet, error) {
	if len(values) == 0 {
		return nil, nil
	}
	result := make([]*net.IPNet, 0, len(values))
	for _, value := range values {
		_, network, err := net.ParseCIDR(value)
		if err != nil {
			return nil, fmt.Errorf("invalid CIDR %q: %w", value, err)
		}
		result = append(result, network)
	}
	return result, nil
}

func (p *DialPolicy) ValidateIP(ip net.IP) error {
	if ip == nil {
		return fmt.Errorf("nil ip")
	}
	if p.BlockPrivateIPs && isBlockedPrivate(ip) {
		return fmt.Errorf("ip %s blocked by private-ip policy", ip)
	}
	if len(p.DenyCIDRs) > 0 && containsIP(p.DenyCIDRs, ip) {
		return fmt.Errorf("ip %s denied by policy", ip)
	}
	if len(p.AllowCIDRs) > 0 && !containsIP(p.AllowCIDRs, ip) {
		return fmt.Errorf("ip %s not in allowed CIDRs", ip)
	}
	return nil
}

func (p *DialPolicy) RawIPAllowed(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if len(p.AllowedRawCIDRs) == 0 {
		return false
	}
	return containsIP(p.AllowedRawCIDRs, ip)
}

func (p *DialPolicy) DialContext(ctx context.Context, targetAddr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(targetAddr)
	if err != nil {
		return nil, err
	}
	dialTimeout, keepAlive := p.timing()
	dialer := &net.Dialer{
		Timeout:   dialTimeout,
		KeepAlive: keepAlive,
	}

	if ip := net.ParseIP(host); ip != nil {
		if err := p.ValidateIP(ip); err != nil {
			return nil, err
		}
		return dialer.DialContext(ctx, "tcp", net.JoinHostPort(ip.String(), port))
	}

	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	var candidates []string
	for _, addr := range addrs {
		if err := p.ValidateIP(addr.IP); err == nil {
			candidates = append(candidates, net.JoinHostPort(addr.IP.String(), port))
		}
	}
	slices.Sort(candidates)
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no allowed IPs for %s", targetAddr)
	}
	totalCandidates := len(candidates)
	if totalCandidates > maxDialCandidates {
		candidates = candidates[:maxDialCandidates]
	}
	perAttempt := dialAttemptTimeout(dialTimeout, len(candidates))
	errs := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		attemptCtx := ctx
		cancel := func() {}
		if perAttempt > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, perAttempt)
		}
		conn, err := dialer.DialContext(attemptCtx, "tcp", candidate)
		cancel()
		if err == nil {
			return conn, nil
		}
		errs = append(errs, fmt.Sprintf("%s: %v", candidate, err))
	}
	return nil, fmt.Errorf("failed to dial allowed IPs for %s (attempted=%d/%d per_attempt_timeout=%s): %s",
		targetAddr, len(candidates), totalCandidates, perAttempt, summarizeDialErrors(errs))
}

func (p *DialPolicy) SetTiming(dialTimeout, keepAlive time.Duration) {
	p.mu.Lock()
	if dialTimeout > 0 {
		p.DialTimeout = dialTimeout
	}
	if keepAlive > 0 {
		p.KeepAlive = keepAlive
	}
	p.mu.Unlock()
}

func (p *DialPolicy) timing() (time.Duration, time.Duration) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.DialTimeout, p.KeepAlive
}

func dialAttemptTimeout(total time.Duration, attempts int) time.Duration {
	if attempts <= 0 {
		return 0
	}
	if total <= 0 {
		total = 30 * time.Second
	}
	perAttempt := total / time.Duration(attempts)
	if perAttempt < minPerAttemptDialTimeout {
		perAttempt = minPerAttemptDialTimeout
	}
	if perAttempt > maxPerAttemptDialTimeout {
		perAttempt = maxPerAttemptDialTimeout
	}
	return perAttempt
}

func summarizeDialErrors(errs []string) string {
	if len(errs) == 0 {
		return "no dial attempts recorded"
	}
	const preview = 3
	if len(errs) <= preview {
		return strings.Join(errs, "; ")
	}
	return strings.Join(errs[:preview], "; ") + fmt.Sprintf("; ... (%d more)", len(errs)-preview)
}

func containsIP(networks []*net.IPNet, ip net.IP) bool {
	for _, network := range networks {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

func isBlockedPrivate(ip net.IP) bool {
	return ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalMulticast() || ip.IsLinkLocalUnicast() || ip.IsMulticast() || ip.IsUnspecified()
}
