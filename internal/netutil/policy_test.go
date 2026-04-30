package netutil

import (
	"context"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestDialAttemptTimeoutBounds(t *testing.T) {
	t.Parallel()

	if got := dialAttemptTimeout(30*time.Second, 1); got != 5*time.Second {
		t.Fatalf("single-attempt timeout = %s, want 5s", got)
	}
	if got := dialAttemptTimeout(30*time.Second, 100); got != 2*time.Second {
		t.Fatalf("many-attempt timeout = %s, want 2s", got)
	}
	if got := dialAttemptTimeout(0, 2); got != 5*time.Second {
		t.Fatalf("default timeout = %s, want 5s", got)
	}
}

func TestDialContextHostnameSuccess(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer ln.Close()

	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		_ = conn.Close()
	}()

	port := ln.Addr().(*net.TCPAddr).Port
	target := net.JoinHostPort("localhost", strconv.Itoa(port))

	policy := &DialPolicy{
		BlockPrivateIPs: false,
		DialTimeout:     30 * time.Second,
		KeepAlive:       30 * time.Second,
	}
	conn, err := policy.DialContext(context.Background(), target)
	if err != nil {
		t.Fatalf("DialContext failed: %v", err)
	}
	_ = conn.Close()

	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for accepted connection")
	}
}

func TestDialContextFailureIncludesAttemptMetadata(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()

	target := net.JoinHostPort("localhost", strconv.Itoa(port))
	policy := &DialPolicy{
		BlockPrivateIPs: false,
		DialTimeout:     30 * time.Second,
		KeepAlive:       30 * time.Second,
	}

	_, err = policy.DialContext(context.Background(), target)
	if err == nil {
		t.Fatalf("expected dial failure")
	}
	msg := err.Error()
	if !strings.Contains(msg, "failed to dial allowed IPs for") {
		t.Fatalf("expected failure prefix, got %q", msg)
	}
	if !strings.Contains(msg, "attempted=") {
		t.Fatalf("expected attempted count in error, got %q", msg)
	}
	if !strings.Contains(msg, "per_attempt_timeout=") {
		t.Fatalf("expected per-attempt timeout in error, got %q", msg)
	}
}
