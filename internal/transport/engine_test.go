package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
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

func waitForManifestChunkCount(t *testing.T, backend *storage.FakeBackend, dir Direction, clientID string, want int) *manifestFile {
	t.Helper()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		manifest := readManifestFromBackend(t, backend, dir, clientID)
		if len(manifest.Chunks) == want {
			return manifest
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for manifest chunk count %d", want)
	return nil
}

func readSessionHeadFromBackend(t *testing.T, backend *storage.FakeBackend, dir Direction, sessionID string) *sessionHeadFile {
	t.Helper()
	var rc io.ReadCloser
	var err error
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		rc, err = backend.Download(context.Background(), sessionHeadFilename(dir, sessionID))
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("download session head: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	defer rc.Close()
	var head sessionHeadFile
	if err := json.NewDecoder(rc).Decode(&head); err != nil {
		t.Fatalf("decode session head: %v", err)
	}
	return &head
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

	clientSession.EnqueueTx([]byte("three"))
	clientEngine.flushAll(true)
	clientEngine.doUpload(context.Background())

	time.Sleep(metadataPublishMinInterval + 50*time.Millisecond)
	clientEngine.doUpload(context.Background())

	head := readSessionHeadFromBackend(t, backend, DirReq, "s1")
	if len(head.Chunks) != 2 {
		t.Fatalf("expected two direct chunks, got %d", len(head.Chunks))
	}
	head.Chunks[0], head.Chunks[1] = head.Chunks[1], head.Chunks[0]
	payload, err := encodeSessionHead(*head)
	if err != nil {
		t.Fatalf("encode session head: %v", err)
	}
	if err := backend.Put(context.Background(), sessionHeadFilename(DirReq, "s1"), bytes.NewReader(payload)); err != nil {
		t.Fatalf("rewrite session head: %v", err)
	}

	serverEngine := NewEngineWithPool(storage.NewSingleBackendPool("default", backend), false, "c1", DefaultOptions())
	for i := 0; i < 5; i++ {
		serverEngine.doPoll(context.Background())
		time.Sleep(50 * time.Millisecond)
	}

	session := waitForSession(t, serverEngine, "s1")
	if session == nil {
		t.Fatalf("expected session to exist")
	}
	got1 := readPayload(t, session)
	got2 := readPayload(t, session)
	got3 := readPayload(t, session)
	if got1 != "one" || got2 != "two" || got3 != "three" {
		t.Fatalf("unexpected delivery order %q %q %q", got1, got2, got3)
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

func TestEngineStartPrimesManifestAndSuppressesStaleBootstrap(t *testing.T) {
	t.Parallel()

	backend := storage.NewFakeBackend()
	oldTime := time.Now().Add(-time.Minute)
	oldChunkID := fmt.Sprintf("%013d-stale", oldTime.UnixMilli())
	oldEnv := &Envelope{
		SessionID:     "stale-session",
		Seq:           0,
		Kind:          KindOpen,
		TargetAddr:    "127.0.0.1:18181",
		Payload:       []byte("hello"),
		CreatedUnixMs: oldTime.UnixMilli(),
	}
	oldEnv.EnsureChecksum()
	payload, _, err := marshalChunkPayloads([]*Envelope{oldEnv}, "off", 0)
	if err != nil {
		t.Fatalf("marshalChunkPayloads: %v", err)
	}
	if err := backend.Upload(context.Background(), chunkFilename(DirReq, "c1", 1, oldChunkID), bytes.NewReader(payload)); err != nil {
		t.Fatalf("seed stale chunk: %v", err)
	}
	manifestPayload, err := json.MarshalIndent(manifestFile{
		Dir:           DirReq,
		ClientID:      "c1",
		Epoch:         1,
		UpdatedUnixMs: oldTime.UnixMilli(),
		Chunks: []manifestChunk{{
			ID:            oldChunkID,
			CreatedUnixMs: oldTime.UnixMilli(),
			SizeBytes:     len(payload),
		}},
	}, "", "  ")
	if err != nil {
		t.Fatalf("marshal stale manifest: %v", err)
	}
	if err := backend.Put(context.Background(), manifestFilename(DirReq, "c1"), bytes.NewReader(manifestPayload)); err != nil {
		t.Fatalf("seed stale manifest: %v", err)
	}

	clientOpts := DefaultOptions()
	clientOpts.DataDir = t.TempDir()
	client := NewEngineWithPool(storage.NewSingleBackendPool("default", backend), true, "c1", clientOpts)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client.Start(ctx)

	deadline := time.Now().Add(500 * time.Millisecond)
	var manifest *manifestFile
	for time.Now().Before(deadline) {
		manifest = readManifestFromBackend(t, backend, DirReq, "c1")
		if manifest.Epoch == client.epoch && len(manifest.Chunks) == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if manifest == nil || manifest.Epoch != client.epoch || len(manifest.Chunks) != 0 {
		if manifest == nil {
			t.Fatalf("manifest not primed")
		}
		t.Fatalf("manifest not primed, got epoch=%v chunks=%v", manifest.Epoch, len(manifest.Chunks))
	}

	server := NewEngineWithPool(storage.NewSingleBackendPool("default", backend), false, "c1", DefaultOptions())
	server.doPoll(context.Background())
	time.Sleep(100 * time.Millisecond)
	if got := server.GetSession("stale-session"); got != nil {
		t.Fatalf("expected stale session to stay suppressed")
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

func TestFirstPeerEpochBindsExistingLocalSession(t *testing.T) {
	t.Parallel()

	engine, _ := newTestEngine(t, true)
	session := NewSession("s1")
	session.ClientID = "c1"
	session.TargetAddr = "example.com:80"
	session.BackendName = "default"
	engine.AddSession(session)

	env := &Envelope{
		SessionID:     "s1",
		Seq:           0,
		Kind:          KindData,
		Payload:       []byte("hello"),
		CreatedUnixMs: time.Now().UnixMilli(),
	}
	env.EnsureChecksum()

	if err := engine.processEnvelope(env, "default", 12345); err != nil {
		t.Fatalf("processEnvelope failed: %v", err)
	}

	got := engine.GetSession("s1")
	if got == nil {
		t.Fatalf("expected existing session to remain bound")
	}
	if got != session {
		t.Fatalf("expected existing local session to be reused")
	}
	if got.BackendEpoch != 12345 {
		t.Fatalf("BackendEpoch = %d, want 12345", got.BackendEpoch)
	}
	if payload := readPayload(t, got); payload != "hello" {
		t.Fatalf("payload = %q, want hello", payload)
	}
}

func TestConcurrentProcessEnvelopeCreatesSessionOnce(t *testing.T) {
	t.Parallel()

	engine, _ := newTestEngine(t, false)
	var newSessions atomic.Int32
	engine.OnNewSession = func(sessionID, targetAddr string, s *Session) {
		newSessions.Add(1)
	}

	env0 := &Envelope{
		SessionID:     "s1",
		Seq:           0,
		Kind:          KindOpen,
		TargetAddr:    "127.0.0.1:18080",
		Payload:       []byte("one"),
		CreatedUnixMs: time.Now().UnixMilli(),
	}
	env0.EnsureChecksum()
	env1 := &Envelope{
		SessionID:     "s1",
		Seq:           1,
		Kind:          KindClose,
		TargetAddr:    "127.0.0.1:18080",
		CreatedUnixMs: time.Now().UnixMilli(),
	}
	env1.EnsureChecksum()

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		if err := engine.processEnvelope(env0, "default", 55); err != nil {
			t.Errorf("processEnvelope env0 failed: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		if err := engine.processEnvelope(env1, "default", 55); err != nil {
			t.Errorf("processEnvelope env1 failed: %v", err)
		}
	}()
	close(start)
	wg.Wait()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) && newSessions.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if got := newSessions.Load(); got != 1 {
		t.Fatalf("OnNewSession count = %d, want 1", got)
	}
	session := waitForSession(t, engine, "s1")
	if session == nil {
		t.Fatalf("expected session to exist")
	}
	if payload := readPayload(t, session); payload != "one" {
		t.Fatalf("payload = %q, want one", payload)
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
