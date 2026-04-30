package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/NullLatency/flow-driver/internal/storage"
)

func newTestEngine(t *testing.T, isClient bool) (*Engine, *storage.FakeBackend) {
	t.Helper()
	backend := storage.NewFakeBackend()
	opts := DefaultOptions()
	opts.DataDir = t.TempDir()
	opts.SessionIdleTimeout = 5 * time.Minute
	opts.StaleUnackedFileTTL = 200 * time.Millisecond
	opts.CleanupInterval = 50 * time.Millisecond
	opts.UploadInterval = 50 * time.Millisecond
	opts.FlushRate = 100 * time.Millisecond
	opts.PollRate = 200 * time.Millisecond
	opts.ActivePollRate = 100 * time.Millisecond
	engine := NewEngineWithPool(storage.NewSingleBackendPool("default", backend), isClient, "c1", opts)
	return engine, backend
}

func TestClientStreamAppendAndFlush(t *testing.T) {
	t.Parallel()

	engine, backend := newTestEngine(t, true)
	session := NewSession("s1")
	session.ClientID = "c1"
	engine.AddSession(session)
	session.EnqueueTx([]byte("hello"))

	engine.flushAll(true)

	engine.doUpload(context.Background())

	if !backend.HasFile("stream-req-" + safeID("c1") + ".bin") {
		t.Fatalf("expected stream file on backend")
	}

	session.mu.Lock()
	if len(session.txBuf) > 0 {
		t.Fatalf("expected tx buffer to be empty after flush, got %d bytes", len(session.txBuf))
	}
	session.mu.Unlock()
}

func TestClientStreamBatchesMultipleSessions(t *testing.T) {
	t.Parallel()

	engine, backend := newTestEngine(t, true)
	engine.opts.SegmentBytes = 4
	engine.opts.MaxSegmentBytes = 4
	engine.opts.MaxMuxSegments = 8

	session1 := NewSession("s1")
	session1.ClientID = "c1"
	engine.AddSession(session1)
	session1.EnqueueTx([]byte("ab"))

	session2 := NewSession("s2")
	session2.ClientID = "c1"
	engine.AddSession(session2)
	session2.EnqueueTx([]byte("cd"))

	engine.flushAll(true)

	engine.doUpload(context.Background())

	if !backend.HasFile("stream-req-" + safeID("c1") + ".bin") {
		t.Fatalf("expected stream file on backend")
	}
}

func TestServerStreamDownloadAndDeliver(t *testing.T) {
	t.Parallel()

	// Client uploads stream data
	clientEngine, clientBackend := newTestEngine(t, true)
	clientSession := NewSession("s1")
	clientSession.ClientID = "c1"
	clientSession.TargetAddr = "1.1.1.1:443"
	clientEngine.AddSession(clientSession)
	clientSession.EnqueueTx([]byte("hello"))
	clientEngine.flushAll(true)
	clientEngine.doUpload(context.Background())

	// Server downloads and processes from the same backend
	serverEngine := NewEngineWithPool(storage.NewSingleBackendPool("default", clientBackend), false, "c1", DefaultOptions())
	serverEngine.opts.PollRate = 100 * time.Millisecond
	serverEngine.opts.ActivePollRate = 50 * time.Millisecond

	serverEngine.doPoll(context.Background())

	time.Sleep(100 * time.Millisecond)

	session := serverEngine.GetSession("s1")
	if session == nil {
		t.Fatalf("expected session to be created")
	}

	select {
	case got := <-session.RxChan:
		if string(got) != "hello" {
			t.Fatalf("unexpected payload %q", got)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timed out waiting for payload delivery")
	}
}

func TestServerOffsetPersistsAcrossPolls(t *testing.T) {
	t.Parallel()

	// Client uploads stream data
	clientEngine, clientBackend := newTestEngine(t, true)
	clientSession := NewSession("s1")
	clientSession.ClientID = "c1"
	clientEngine.AddSession(clientSession)
	clientSession.EnqueueTx([]byte("hello"))
	clientEngine.flushAll(true)
	clientEngine.doUpload(context.Background())

	// Server downloads
	serverEngine := NewEngineWithPool(storage.NewSingleBackendPool("default", clientBackend), false, "c1", DefaultOptions())
	serverEngine.doPoll(context.Background())
	time.Sleep(50 * time.Millisecond)

	offset := serverEngine.streamOffset("default")
	if offset == 0 {
		t.Fatalf("expected non-zero offset after poll")
	}

	// Second poll - should not re-deliver
	serverEngine.doPoll(context.Background())
	time.Sleep(50 * time.Millisecond)

	session := serverEngine.GetSession("s1")
	if session == nil {
		t.Fatalf("expected session to exist")
	}

	select {
	case <-session.RxChan:
		select {
		case <-session.RxChan:
			t.Fatalf("expected only one message, got two")
		default:
		}
	default:
		t.Fatalf("expected at least one message")
	}
}

func TestServerLoadsOffsetFromDrive(t *testing.T) {
	t.Parallel()

	_, backend := newTestEngine(t, false)

	off := streamOffset{
		Dir:            DirReq,
		ClientID:       "c1",
		Offset:         100,
		LastUploadedMs: time.Now().UnixMilli(),
	}
	saveOffsetToDrive(off, backend, context.Background())

	engine := NewEngineWithPool(storage.NewSingleBackendPool("default", backend), false, "c1", DefaultOptions())
	engine.ensureStreamOffsetsLoaded(context.Background())

	offset := engine.streamOffset("default")
	if offset != 100 {
		t.Fatalf("expected offset 100, got %d", offset)
	}
}

func TestStreamFileUsesPutForUpdate(t *testing.T) {
	t.Parallel()

	engine, backend := newTestEngine(t, true)

	session := NewSession("s1")
	session.ClientID = "c1"
	engine.AddSession(session)
	session.EnqueueTx([]byte("first"))
	engine.flushAll(true)
	engine.doUpload(context.Background())

	putCalls := backend.PutCalls("stream-req-" + safeID("c1") + ".bin")
	if putCalls == 0 {
		t.Fatalf("expected stream to be uploaded via Put, got %d Put calls", putCalls)
	}

	session.EnqueueTx([]byte("second"))
	engine.flushAll(true)
	engine.doUpload(context.Background())

	putCalls2 := backend.PutCalls("stream-req-" + safeID("c1") + ".bin")
	if putCalls2 < 2 {
		t.Fatalf("expected at least 2 Put calls for stream updates, got %d", putCalls2)
	}
}

func TestMultipleEnvelopesInStream(t *testing.T) {
	t.Parallel()

	// Client uploads multiple envelopes with small segment size
	clientEngine, clientBackend := newTestEngine(t, true)
	clientEngine.opts.SegmentBytes = 4
	clientEngine.opts.MaxSegmentBytes = 4
	clientSession := NewSession("s1")
	clientSession.ClientID = "c1"
	clientSession.TargetAddr = "1.1.1.1:443"
	clientEngine.AddSession(clientSession)
	clientSession.EnqueueTx([]byte("aaa"))
	clientSession.EnqueueTx([]byte("bbb"))
	clientSession.EnqueueTx([]byte("ccc"))
	clientEngine.flushAll(true)
	clientEngine.doUpload(context.Background())

	// Server downloads
	serverEngine := NewEngineWithPool(storage.NewSingleBackendPool("default", clientBackend), false, "c1", DefaultOptions())
	serverEngine.doPoll(context.Background())
	time.Sleep(100 * time.Millisecond)

	session := serverEngine.GetSession("s1")
	if session == nil {
		t.Fatalf("expected session to be created")
	}

	// With SegmentBytes=4, data "aaabbbccc" is split into envelopes
	// First envelope: "aaab" (4 bytes), second: "bbcc" (4 bytes), third: "c" (1 byte)
	for _, expected := range []string{"aaab", "bbcc", "c"} {
		select {
		case got := <-session.RxChan:
			if string(got) != expected {
				t.Fatalf("expected %q, got %q", expected, got)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("timed out waiting for %q", expected)
		}
	}
}

func TestStreamUploadRetry(t *testing.T) {
	t.Parallel()

	engine, backend := newTestEngine(t, true)
	backend.SetUploadFailures(1)

	session := NewSession("s1")
	session.ClientID = "c1"
	engine.AddSession(session)
	session.EnqueueTx([]byte("hello"))
	engine.flushAll(true)

	engine.doUpload(context.Background())
	engine.doUpload(context.Background())

	if !backend.HasFile("stream-req-" + safeID("c1") + ".bin") {
		t.Fatalf("expected stream file on backend after retry")
	}
}

func TestClientFlushWithNoSessions(t *testing.T) {
	t.Parallel()

	engine, _ := newTestEngine(t, true)
	engine.flushAll(false)
	engine.flushAll(true)
}

func TestBackendSelectionPinsSessionsAcrossBackends(t *testing.T) {
	t.Parallel()

	opts := DefaultOptions()
	opts.DataDir = t.TempDir()
	pool := storage.NewBackendPool(
		&storage.BackendHandle{Name: "g1", Weight: 1, Backend: storage.NewFakeBackend()},
		&storage.BackendHandle{Name: "g2", Weight: 1, Backend: storage.NewFakeBackend()},
	)
	engine := NewEngineWithPool(pool, true, "c1", opts)

	byBackend := make(map[string]string)
	for i := 0; i < 1024 && len(byBackend) < 2; i++ {
		sessionID := fmt.Sprintf("s%d", i)
		backend, err := pool.Select("c1:" + sessionID)
		if err != nil {
			t.Fatalf("backend selection failed: %v", err)
		}
		if byBackend[backend.Name] == "" {
			byBackend[backend.Name] = sessionID
		}
	}
	if len(byBackend) != 2 {
		t.Fatalf("expected test keys for both backends, got %v", byBackend)
	}

	for backendName, sessionID := range byBackend {
		session := NewSession(sessionID)
		session.ClientID = "c1"
		engine.AddSession(session)
		if session.BackendName != backendName {
			t.Fatalf("session %s backend=%s, want %s", sessionID, session.BackendName, backendName)
		}
		session.EnqueueTx([]byte("test-" + backendName))
	}
	engine.flushAll(true)
	engine.doUpload(context.Background())

	backends := engine.pool.Backends()
	if len(backends) != 2 {
		t.Fatalf("expected 2 backends, got %d", len(backends))
	}
	for _, handle := range backends {
		fakeBackend, ok := handle.Backend.(*storage.FakeBackend)
		if !ok {
			t.Fatalf("expected FakeBackend")
		}
		if !fakeBackend.HasFile("stream-req-" + safeID("c1") + ".bin") {
			t.Fatalf("expected stream file on backend %s", handle.Name)
		}
	}
}

func TestServerDiscoversClientIDFromStreams(t *testing.T) {
	t.Parallel()

	clientEngine, backend := newTestEngine(t, true)
	clientSession := NewSession("s1")
	clientSession.ClientID = "c1"
	clientSession.TargetAddr = "1.1.1.1:443"
	clientEngine.AddSession(clientSession)
	clientSession.EnqueueTx([]byte("hello"))
	clientEngine.flushAll(true)
	clientEngine.doUpload(context.Background())

	serverEngine := NewEngineWithPool(storage.NewSingleBackendPool("g1", backend), false, "", DefaultOptions())
	serverEngine.doPoll(context.Background())

	if got := serverEngine.clientID(); got != "c1" {
		t.Fatalf("clientID = %q, want c1", got)
	}
	session := serverEngine.GetSession("s1")
	if session == nil {
		t.Fatalf("expected session to be created")
	}
	if session.BackendName != "g1" {
		t.Fatalf("backend = %q, want g1", session.BackendName)
	}
}

func TestStreamOffsetFlushLoop(t *testing.T) {
	t.Parallel()

	engine, backend := newTestEngine(t, false)
	engine.setStreamOffset("default", 500, true)

	engine.flushStreamOffsets(context.Background())

	rc, err := backend.Download(context.Background(), "offset-req-"+safeID("c1")+".json")
	if err != nil {
		t.Fatalf("expected offset file to be saved: %v", err)
	}
	defer rc.Close()

	var off streamOffset
	if err := json.NewDecoder(rc).Decode(&off); err != nil {
		t.Fatalf("failed to decode offset: %v", err)
	}
	if off.Offset != 500 {
		t.Fatalf("expected offset 500, got %d", off.Offset)
	}
}
