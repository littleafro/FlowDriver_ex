package httpclient

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net"
	"net/http"
	"time"
)

const (
	DefaultConnectTimeoutMs      = 30000
	DefaultTLSHandshakeTimeoutMs = 10000
	DefaultRequestTimeoutMs      = 60000
	DefaultIdleConnTimeoutMs     = 90000
	DefaultMaxIdleConns          = 100
)

// TransportConfig defines how we reach the upstream Google endpoints while
// staying compatible with both the old field names and the newer snake_case shape.
type TransportConfig struct {
	TargetIP              string `json:"target_ip,omitempty"`
	SNI                   string `json:"sni,omitempty"`
	HostHeader            string `json:"host_header,omitempty"`
	InsecureSkipVerify    bool   `json:"insecure_skip_verify,omitempty"`
	ConnectTimeoutMs      int    `json:"connect_timeout_ms,omitempty"`
	TLSHandshakeTimeoutMs int    `json:"tls_handshake_timeout_ms,omitempty"`
	RequestTimeoutMs      int    `json:"request_timeout_ms,omitempty"`
	IdleConnTimeoutMs     int    `json:"idle_conn_timeout_ms,omitempty"`
	MaxIdleConns          int    `json:"max_idle_conns,omitempty"`
	ForceHTTP2            *bool  `json:"force_http2,omitempty"`
}

func boolPtr(v bool) *bool {
	return &v
}

func (c *TransportConfig) ApplyDefaults() {
	if c.ConnectTimeoutMs <= 0 {
		c.ConnectTimeoutMs = DefaultConnectTimeoutMs
	}
	if c.TLSHandshakeTimeoutMs <= 0 {
		c.TLSHandshakeTimeoutMs = DefaultTLSHandshakeTimeoutMs
	}
	if c.RequestTimeoutMs <= 0 {
		c.RequestTimeoutMs = DefaultRequestTimeoutMs
	}
	if c.IdleConnTimeoutMs <= 0 {
		c.IdleConnTimeoutMs = DefaultIdleConnTimeoutMs
	}
	if c.MaxIdleConns <= 0 {
		c.MaxIdleConns = DefaultMaxIdleConns
	}
	if c.ForceHTTP2 == nil {
		c.ForceHTTP2 = boolPtr(true)
	}
}

func (c TransportConfig) HTTP2Enabled() bool {
	return c.ForceHTTP2 == nil || *c.ForceHTTP2
}

// UnmarshalJSON accepts both the new snake_case fields and the original
// exported Go-style field names used by the existing examples.
func (c *TransportConfig) UnmarshalJSON(data []byte) error {
	type alias TransportConfig
	var modern alias
	if err := json.Unmarshal(data, &modern); err != nil {
		return err
	}

	var legacy struct {
		TargetIP              string `json:"TargetIP"`
		SNI                   string `json:"SNI"`
		HostHeader            string `json:"HostHeader"`
		InsecureSkipVerify    *bool  `json:"InsecureSkipVerify"`
		ConnectTimeoutMs      int    `json:"ConnectTimeoutMs"`
		TLSHandshakeTimeoutMs int    `json:"TLSHandshakeTimeoutMs"`
		RequestTimeoutMs      int    `json:"RequestTimeoutMs"`
		IdleConnTimeoutMs     int    `json:"IdleConnTimeoutMs"`
		MaxIdleConns          int    `json:"MaxIdleConns"`
		ForceHTTP2            *bool  `json:"ForceHTTP2"`
	}
	if err := json.Unmarshal(data, &legacy); err != nil {
		return err
	}

	*c = TransportConfig(modern)
	if c.TargetIP == "" {
		c.TargetIP = legacy.TargetIP
	}
	if c.SNI == "" {
		c.SNI = legacy.SNI
	}
	if c.HostHeader == "" {
		c.HostHeader = legacy.HostHeader
	}
	if !c.InsecureSkipVerify && legacy.InsecureSkipVerify != nil {
		c.InsecureSkipVerify = *legacy.InsecureSkipVerify
	}
	if c.ConnectTimeoutMs == 0 {
		c.ConnectTimeoutMs = legacy.ConnectTimeoutMs
	}
	if c.TLSHandshakeTimeoutMs == 0 {
		c.TLSHandshakeTimeoutMs = legacy.TLSHandshakeTimeoutMs
	}
	if c.RequestTimeoutMs == 0 {
		c.RequestTimeoutMs = legacy.RequestTimeoutMs
	}
	if c.IdleConnTimeoutMs == 0 {
		c.IdleConnTimeoutMs = legacy.IdleConnTimeoutMs
	}
	if c.MaxIdleConns == 0 {
		c.MaxIdleConns = legacy.MaxIdleConns
	}
	if c.ForceHTTP2 == nil {
		c.ForceHTTP2 = legacy.ForceHTTP2
	}
	c.ApplyDefaults()
	return nil
}

type hostRewriteTransport struct {
	Transport  http.RoundTripper
	HostHeader string
}

func (t *hostRewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.HostHeader == "" {
		return t.Transport.RoundTrip(req)
	}
	clone := req.Clone(req.Context())
	clone.Host = t.HostHeader
	return t.Transport.RoundTrip(clone)
}

// NewCustomClient creates an http.Client configured to bypass DNS and to
// manipulate TLS/HTTP headers as specified in the transport config.
func NewCustomClient(cfg TransportConfig) *http.Client {
	cfg.ApplyDefaults()

	dialer := &net.Dialer{
		Timeout:   time.Duration(cfg.ConnectTimeoutMs) * time.Millisecond,
		KeepAlive: 30 * time.Second,
	}

	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if cfg.TargetIP != "" {
				return dialer.DialContext(ctx, "tcp", cfg.TargetIP)
			}
			return dialer.DialContext(ctx, network, addr)
		},
		TLSClientConfig: &tls.Config{
			ServerName:         cfg.SNI,
			InsecureSkipVerify: cfg.InsecureSkipVerify,
		},
		ForceAttemptHTTP2:     cfg.HTTP2Enabled(),
		MaxIdleConns:          cfg.MaxIdleConns,
		IdleConnTimeout:       time.Duration(cfg.IdleConnTimeoutMs) * time.Millisecond,
		TLSHandshakeTimeout:   time.Duration(cfg.TLSHandshakeTimeoutMs) * time.Millisecond,
		ExpectContinueTimeout: 1 * time.Second,
	}

	var rt http.RoundTripper = transport
	if cfg.HostHeader != "" {
		rt = &hostRewriteTransport{
			Transport:  transport,
			HostHeader: cfg.HostHeader,
		}
	}

	return &http.Client{
		Transport: rt,
		Timeout:   time.Duration(cfg.RequestTimeoutMs) * time.Millisecond,
	}
}
