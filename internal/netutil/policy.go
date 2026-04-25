package netutil

import (
	"context"
	"fmt"
	"net"
	"slices"
	"time"
)

type DialPolicy struct {
	AllowCIDRs      []*net.IPNet
	DenyCIDRs       []*net.IPNet
	AllowedRawCIDRs []*net.IPNet
	BlockPrivateIPs bool
	DialTimeout     time.Duration
	KeepAlive       time.Duration
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
	dialer := &net.Dialer{
		Timeout:   p.DialTimeout,
		KeepAlive: p.KeepAlive,
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
	for _, candidate := range candidates {
		conn, err := dialer.DialContext(ctx, "tcp", candidate)
		if err == nil {
			return conn, nil
		}
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no allowed IPs for %s", targetAddr)
	}
	return nil, fmt.Errorf("failed to dial allowed IPs for %s", targetAddr)
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
