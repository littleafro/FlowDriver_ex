package main

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/NullLatency/flow-driver/internal/netutil"
	"github.com/NullLatency/flow-driver/internal/transport"
)

func TestHandleServerConnKeepsResponsePathOpenAfterClientEOF(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer ln.Close()

	requestCh := make(chan []byte, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		req, _ := io.ReadAll(conn)
		requestCh <- req
		_, _ = conn.Write([]byte("pong"))
	}()

	session := transport.NewSession("s1")
	session.TargetAddr = ln.Addr().String()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		handleServerConn(ctx, session, &netutil.DialPolicy{
			BlockPrivateIPs: false,
			DialTimeout:     time.Second,
			KeepAlive:       time.Second,
		})
		close(done)
	}()

	session.RxChan <- []byte("ping")
	close(session.RxChan)

	select {
	case got := <-requestCh:
		if string(got) != "ping" {
			t.Fatalf("unexpected upstream request %q", got)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for upstream request")
	}

	select {
	case <-done:
	case <-time.After(1500 * time.Millisecond):
		t.Fatalf("timed out waiting for proxy shutdown")
	}

	payload := collectPreparedPayloads(t, session)
	if string(payload) != "pong" {
		t.Fatalf("expected upstream response to reach session, got %q", payload)
	}
}

func collectPreparedPayloads(t *testing.T, session *transport.Session) []byte {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if session.PendingBytes() > 0 || session.HasCloseQueued() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	var payload []byte
	for {
		batch, err := session.PrepareEnvelopeBatch(time.Now(), 64*1024, 64*1024, 8, true, false)
		if err != nil {
			t.Fatalf("PrepareEnvelopeBatch failed: %v", err)
		}
		if len(batch) == 0 {
			break
		}
		for _, item := range batch {
			payload = append(payload, item.Env.Payload...)
			if err := session.CommitEnvelope(item.Env, item.Consumed); err != nil {
				t.Fatalf("CommitEnvelope failed: %v", err)
			}
		}
	}
	return payload
}
