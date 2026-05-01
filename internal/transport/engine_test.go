package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/NullLatency/flow-driver/internal/storage"
)

func newTestEngine(t *testing.T, isClient bool) (*Engine, *storage.FakeBackend) {
	t.Helper()
	backend := storage.NewFakeBackend()
	opts := DefaultOptions()
	opts.DataDir = t.TempDir()
	opts.CleanupInterval = 50 * time.Millisecond
	opts.UploadInterval = 25 * time.Millisecond
	opts.FlushRate = 50 * time.Millisecond
	opts.PollRate = 150 * time.Millisecond
	opts.ActivePollRate = 80 * time.Millisecond
	opts.IdlePollRate = 200 * time.Millisecond
	opts.ChunkOrphanTTL = 50 * time.Millisecond
	engine := NewEngineWithPool(storage.NewSingleBackendPool("default", backend), isClient, "c1", opts)
	return engine, backend
}

func readManifestFromBackend(t *testing.T, backend *storage.FakeBackend, dir Direction, clientID string) *manifestFile {
	t.Helper()
	var rc io.ReadCloser
	var err error
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		rc, err = backend.Download(context.Background(), manifestFilename(dir, clientID))
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("download manifest: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	defer rc.Close()
	var manifest manifestFile
	if err := json.NewDecoder(rc).Decode(&manifest); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	return &manifest
}

func waitForManifestEpoch(t *testing.T, backend *storage.FakeBackend, dir Direction, clientID string, previous uint64) *manifestFile {
	t.Helper()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		manifest := readManifestFromBackend(t, backend, dir, clientID)
		if manifest.Epoch != previous {
			return manifest
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for manifest epoch change")
	return nil
}

func waitForSession(t *testing.T, engine *Engine, id string) *Session {
	t.Helper()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if session := engine.GetSession(id); session != nil {
			return session
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

func readPayload(t *testing.T, session *Session) string {
	t.Helper()
	select {
	case payload := <-session.RxChan:
		return string(payload)
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for session payload")
		return ""
	}
}

func TestClientFlushPublishesManifestAndChunk(t *testing.T) {
	t.Parallel()

	engine, backend := newTestEngine(t, true)
	session := NewSession("s1")
	session.ClientID = "c1"
	session.TargetAddr = "1.1.1.1:443"
	engine.AddSession(session)
	session.EnqueueTx([]byte("hello"))

	engine.flushAll(true)
	engine.doUpload(context.Background())

	manifest := readManifestFromBackend(t, backend, DirReq, "c1")
	if len(manifest.Chunks) != 1 {
		t.Fatalf("expected one chunk in manifest, got %d", len(manifest.Chunks))
	}
	filename := chunkFilename(DirReq, "c1", manifest.Epoch, manifest.Chunks[0].ID)
	if !backend.HasFile(filename) {
		t.Fatalf("expected chunk %s on backend", filename)
	}
}

func TestServerPollDeliversPayloadAndDeletesChunk(t *testing.T) {
	t.Parallel()

	clientEngine, backend := newTestEngine(t, true)
	clientSession := NewSession("s1")
	clientSession.ClientID = "c1"
	clientSession.TargetAddr = "1.1.1.1:443"
	clientEngine.AddSession(clientSession)
	clientSession.EnqueueTx([]byte("hello"))
	clientEngine.flushAll(true)
	clientEngine.doUpload(context.Background())

	manifest := readManifestFromBackend(t, backend, DirReq, "c1")
	chunkName := chunkFilename(DirReq, "c1", manifest.Epoch, manifest.Chunks[0].ID)

	serverEngine := NewEngineWithPool(storage.NewSingleBackendPool("default", backend), false, "c1", DefaultOptions())
	serverEngine.doPoll(context.Background())
	time.Sleep(100 * time.Millisecond)

	session := waitForSession(t, serverEngine, "s1")
	if session == nil {
		t.Fatalf("expected session to be created")
	}
	select {
	case got := <-session.RxChan:
		if string(got) != "hello" {
			t.Fatalf("unexpected payload %q", got)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatalf("timed out waiting for payload delivery")
	}
	if backend.DeleteCalls(chunkName) == 0 {
		t.Fatalf("expected processed chunk to be deleted")
	}
}

func TestChunkOrderDoesNotBreakSessionOrdering(t *testing.T) {
	t.Parallel()

	clientEngine, backend := newTestEngine(t, true)
	clientSession := NewSession("s1")
	clientSession.ClientID = "c1"
	clientSession.TargetAddr = "1.1.1.1:443"
	clientEngine.AddSession(clientSession)

	clientSession.EnqueueTx([]byte("one"))
	clientEngine.flushAll(true)
	clientEngine.doUpload(context.Background())

	clientSession.EnqueueTx([]byte("two"))
	clientEngine.flushAll(true)
	clientEngine.doUpload(context.Background())

	manifest := readManifestFromBackend(t, backend, DirReq, "c1")
	if len(manifest.Chunks) != 2 {
		t.Fatalf("expected two chunks, got %d", len(manifest.Chunks))
	}
	manifest.Chunks[0], manifest.Chunks[1] = manifest.Chunks[1], manifest.Chunks[0]
	payload, err := encodeManifest(*manifest)
	if err != nil {
		t.Fatalf("encode manifest: %v", err)
	}
	if err := backend.Put(context.Background(), manifestFilename(DirReq, "c1"), bytes.NewReader(payload)); err != nil {
		t.Fatalf("rewrite manifest: %v", err)
	}

	serverEngine := NewEngineWithPool(storage.NewSingleBackendPool("default", backend), false, "c1", DefaultOptions())
	serverEngine.doPoll(context.Background())
	time.Sleep(150 * time.Millisecond)

	session := waitForSession(t, serverEngine, "s1")
	if session == nil {
		t.Fatalf("expected session to exist")
	}
	got1 := readPayload(t, session)
	got2 := readPayload(t, session)
	if got1 != "one" || got2 != "two" {
		t.Fatalf("unexpected delivery order %q %q", got1, got2)
	}
}

func TestRepeatedManifestDoesNotRedeliverChunk(t *testing.T) {
	t.Parallel()

	clientEngine, backend := newTestEngine(t, true)
	clientSession := NewSession("s1")
	clientSession.ClientID = "c1"
	clientSession.TargetAddr = "1.1.1.1:443"
	clientEngine.AddSession(clientSession)
	clientSession.EnqueueTx([]byte("hello"))
	clientEngine.flushAll(true)
	clientEngine.doUpload(context.Background())

	serverEngine := NewEngineWithPool(storage.NewSingleBackendPool("default", backend), false, "c1", DefaultOptions())
	serverEngine.doPoll(context.Background())
	time.Sleep(100 * time.Millisecond)
	serverEngine.doPoll(context.Background())
	time.Sleep(100 * time.Millisecond)

	session := waitForSession(t, serverEngine, "s1")
	if session == nil {
		t.Fatalf("expected session to exist")
	}
	select {
	case got := <-session.RxChan:
		if string(got) != "hello" {
			t.Fatalf("unexpected payload %q", got)
		}
	default:
		t.Fatalf("expected first payload")
	}
	select {
	case dup := <-session.RxChan:
		t.Fatalf("unexpected duplicate delivery %q", dup)
	default:
	}
}

func TestServerDiscoversClientIDFromManifest(t *testing.T) {
	t.Parallel()

	clientEngine, backend := newTestEngine(t, true)
	clientSession := NewSession("s1")
	clientSession.ClientID = "c1"
	clientSession.TargetAddr = "1.1.1.1:443"
	clientEngine.AddSession(clientSession)
	clientSession.EnqueueTx([]byte("hello"))
	clientEngine.flushAll(true)
	clientEngine.doUpload(context.Background())

	serverEngine := NewEngineWithPool(storage.NewSingleBackendPool("default", backend), false, "", DefaultOptions())
	initialListCalls := backend.ListCalls()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		serverEngine.doPoll(context.Background())
		if serverEngine.clientID() == "c1" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := serverEngine.clientID(); got != "c1" {
		t.Fatalf("clientID = %q, want c1", got)
	}
	if backend.ListCalls() <= initialListCalls {
		t.Fatalf("expected manifest discovery to use ListQuery")
	}
}

func TestPollHotPathDoesNotListWhenClientIDKnown(t *testing.T) {
	t.Parallel()

	clientEngine, backend := newTestEngine(t, true)
	clientSession := NewSession("s1")
	clientSession.ClientID = "c1"
	clientSession.TargetAddr = "1.1.1.1:443"
	clientEngine.AddSession(clientSession)
	clientSession.EnqueueTx([]byte("hello"))
	clientEngine.flushAll(true)
	clientEngine.doUpload(context.Background())

	serverEngine := NewEngineWithPool(storage.NewSingleBackendPool("default", backend), false, "c1", DefaultOptions())
	before := backend.ListCalls()
	serverEngine.doPoll(context.Background())
	if backend.ListCalls() != before {
		t.Fatalf("expected hot-path poll to avoid ListQuery when clientID is known")
	}
}

func TestEpochRolloverResetsBackendSessions(t *testing.T) {
	t.Parallel()

	clientEngine, backend := newTestEngine(t, true)
	session1 := NewSession("s1")
	session1.ClientID = "c1"
	session1.TargetAddr = "1.1.1.1:443"
	clientEngine.AddSession(session1)
	session1.EnqueueTx([]byte("hello"))
	clientEngine.flushAll(true)
	clientEngine.doUpload(context.Background())
	initialManifest := readManifestFromBackend(t, backend, DirReq, "c1")

	serverEngine := NewEngineWithPool(storage.NewSingleBackendPool("default", backend), false, "c1", DefaultOptions())
	serverEngine.doPoll(context.Background())
	if waitForSession(t, serverEngine, "s1") == nil {
		t.Fatalf("expected original session to exist")
	}

	restartedOpts := DefaultOptions()
	restartedOpts.DataDir = t.TempDir()
	restartedClient := NewEngineWithPool(storage.NewSingleBackendPool("default", backend), true, "c1", restartedOpts)
	session2 := NewSession("s2")
	session2.ClientID = "c1"
	session2.TargetAddr = "1.1.1.1:443"
	restartedClient.AddSession(session2)
	session2.EnqueueTx([]byte("world"))
	restartedClient.flushAll(true)
	restartedClient.doUpload(context.Background())
	_ = waitForManifestEpoch(t, backend, DirReq, "c1", initialManifest.Epoch)

	serverEngine.doPoll(context.Background())
	time.Sleep(150 * time.Millisecond)
	if serverEngine.GetSession("s1") != nil {
		t.Fatalf("expected old epoch session to be removed")
	}
	if waitForSession(t, serverEngine, "s2") == nil {
		t.Fatalf("expected new epoch session to exist")
	}
}

func TestOrphanSweepDeletesOldChunks(t *testing.T) {
	t.Parallel()

	engine, backend := newTestEngine(t, true)
	oldID := fmt.Sprintf("%013d-deadbeef", time.Now().Add(-2*time.Second).UnixMilli())
	filename := chunkFilename(DirReq, "c1", engine.epoch-1, oldID)
	if err := backend.Upload(context.Background(), filename, bytes.NewReader([]byte("orphan"))); err != nil {
		t.Fatalf("seed orphan chunk: %v", err)
	}

	engine.sweepOrphanChunks(context.Background())
	if backend.HasFile(filename) {
		t.Fatalf("expected orphan chunk to be removed")
	}
}
