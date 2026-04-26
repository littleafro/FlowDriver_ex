package transport

import (
	"bytes"
	"context"
	"encoding/json"
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
	engine := NewEngineWithPool(storage.NewSingleBackendPool("default", backend), isClient, "c1", opts)
	return engine, backend
}

func TestSessionDoesNotCloseBeforeConfiguredIdleTimeout(t *testing.T) {
	t.Parallel()

	engine, _ := newTestEngine(t, true)
	session := NewSession("s1")
	session.ClientID = "c1"
	engine.AddSession(session)

	session.mu.Lock()
	session.lastActivity = time.Now().Add(-20 * time.Second)
	session.mu.Unlock()

	engine.flushAll(true, false)
	if session.HasCloseQueued() {
		t.Fatalf("session should not close before configured idle timeout")
	}
}

func TestUploadedSegmentRetriedAfterUploadFailure(t *testing.T) {
	t.Parallel()

	engine, backend := newTestEngine(t, true)
	backend.SetUploadFailures(1)

	session := NewSession("s1")
	session.ClientID = "c1"
	engine.AddSession(session)
	session.EnqueueTx([]byte("hello"))
	engine.flushAll(true, false)

	segments := engine.pendingSnapshot()
	if len(segments) != 1 {
		t.Fatalf("expected 1 pending segment, got %d", len(segments))
	}
	seg := segments[0]
	handle := engine.pool.Get("default")

	engine.uploadSegment(context.Background(), handle, seg)
	if seg.Meta.Uploaded {
		t.Fatalf("segment should not be marked uploaded after failure")
	}

	data, err := seg.loadData()
	if err != nil {
		t.Fatalf("loadData failed: %v", err)
	}
	env, err := unmarshalSegmentPayload(data)
	if err != nil {
		t.Fatalf("decode spool payload failed: %v", err)
	}
	if string(env.Payload) != "hello" {
		t.Fatalf("payload lost after upload failure: %q", env.Payload)
	}

	engine.uploadSegment(context.Background(), handle, seg)
	if !seg.Meta.Uploaded {
		t.Fatalf("segment should upload on retry")
	}
}

func TestStaleUnackedFilesAreNotDeleted(t *testing.T) {
	t.Parallel()

	engine, backend := newTestEngine(t, true)

	session := NewSession("s1")
	session.ClientID = "c1"
	engine.AddSession(session)
	session.EnqueueTx([]byte("hello"))
	engine.flushAll(true, false)

	seg := engine.pendingSnapshot()[0]
	engine.uploadSegment(context.Background(), engine.pool.Get("default"), seg)
	if !backend.HasFile(seg.Meta.RemoteName) {
		t.Fatalf("expected uploaded segment on backend")
	}

	seg.Meta.CreatedUnixMs = time.Now().Add(-2 * time.Second).UnixMilli()
	if err := seg.saveMeta(); err != nil {
		t.Fatalf("saveMeta failed: %v", err)
	}

	engine.deleteAckedSegments(context.Background())
	time.Sleep(50 * time.Millisecond)

	if !backend.HasFile(seg.Meta.RemoteName) {
		t.Fatalf("stale unacked file should not be deleted")
	}
	if len(engine.pendingSnapshot()) != 1 {
		t.Fatalf("stale unacked segment should remain pending")
	}
}

func TestDeleteOnlyAfterAck(t *testing.T) {
	t.Parallel()

	engine, backend := newTestEngine(t, true)

	session := NewSession("s1")
	session.ClientID = "c1"
	engine.AddSession(session)
	session.EnqueueTx([]byte("hello"))
	engine.flushAll(true, false)

	seg := engine.pendingSnapshot()[0]
	engine.uploadSegment(context.Background(), engine.pool.Get("default"), seg)
	if !backend.HasFile(seg.Meta.RemoteName) {
		t.Fatalf("expected uploaded segment on backend")
	}

	engine.deleteAckedSegments(context.Background())
	time.Sleep(50 * time.Millisecond)
	if !backend.HasFile(seg.Meta.RemoteName) {
		t.Fatalf("segment deleted before ACK")
	}

	ack := AckState{
		ClientID:      "c1",
		SessionID:     "s1",
		Direction:     DirReq,
		AckedSeq:      seg.Meta.Seq,
		UpdatedUnixMs: time.Now().UnixMilli(),
	}
	payload, _ := json.Marshal(ack)
	ackName := ackFilename(DirReq, "c1", "s1")
	if err := backend.Put(context.Background(), ackName, bytes.NewReader(payload)); err != nil {
		t.Fatalf("failed to store ack file: %v", err)
	}
	engine.processAckFile(context.Background(), engine.pool.Get("default"), ackName)
	engine.deleteAckedSegments(context.Background())

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if !backend.HasFile(seg.Meta.RemoteName) && len(engine.pendingSnapshot()) == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("segment was not deleted after ACK")
}

func TestRecoveredCleanAckDoesNotReupload(t *testing.T) {
	t.Parallel()

	engine, backend := newTestEngine(t, true)
	ack := AckState{
		ClientID:      "c1",
		SessionID:     "s-clean",
		Direction:     DirRes,
		AckedSeq:      2,
		UpdatedUnixMs: time.Now().Add(-time.Minute).UnixMilli(),
	}
	filename := ackFilename(DirRes, "c1", "s-clean")
	if _, err := engine.spool.SaveAck("default", filename, ack); err != nil {
		t.Fatalf("SaveAck failed: %v", err)
	}
	if err := engine.spool.MarkAckUploaded("default", filename, ack.UpdatedUnixMs); err != nil {
		t.Fatalf("MarkAckUploaded failed: %v", err)
	}

	recovered := NewEngineWithPool(storage.NewSingleBackendPool("default", backend), true, "c1", engine.opts)
	if err := recovered.recoverLocalState(); err != nil {
		t.Fatalf("recoverLocalState failed: %v", err)
	}

	recovered.flushAckStates(context.Background())
	if backend.PutCalls(filename) != 0 {
		t.Fatalf("expected no ack reupload for clean recovered state, got %d puts", backend.PutCalls(filename))
	}
}

func TestRecoveredDirtyAckUploadsOnlyOnce(t *testing.T) {
	t.Parallel()

	engine, backend := newTestEngine(t, true)
	ack := AckState{
		ClientID:      "c1",
		SessionID:     "s-dirty",
		Direction:     DirRes,
		AckedSeq:      2,
		UpdatedUnixMs: time.Now().UnixMilli(),
	}
	filename := ackFilename(DirRes, "c1", "s-dirty")
	if _, err := engine.spool.SaveAck("default", filename, ack); err != nil {
		t.Fatalf("SaveAck failed: %v", err)
	}

	recovered := NewEngineWithPool(storage.NewSingleBackendPool("default", backend), true, "c1", engine.opts)
	if err := recovered.recoverLocalState(); err != nil {
		t.Fatalf("recoverLocalState failed: %v", err)
	}

	recovered.flushAckStates(context.Background())
	if backend.PutCalls(filename) != 1 {
		t.Fatalf("expected exactly one ack upload on recovery, got %d", backend.PutCalls(filename))
	}

	recovered.flushAckStates(context.Background())
	if backend.PutCalls(filename) != 1 {
		t.Fatalf("expected recovered ack upload to stop after success, got %d puts", backend.PutCalls(filename))
	}
}

func TestOrphanSegmentDiscardedAndAcked(t *testing.T) {
	t.Parallel()

	engine, backend := newTestEngine(t, true)
	env := &Envelope{
		SessionID:     "ghost-session",
		Seq:           3,
		Kind:          KindData,
		Payload:       []byte("orphan"),
		CreatedUnixMs: time.Now().UnixMilli(),
	}
	env.EnsureChecksum()
	payload, compression, err := marshalSegmentPayload(env, "off", 0)
	if err != nil {
		t.Fatalf("marshalSegmentPayload failed: %v", err)
	}
	meta := spoolSegmentMeta{
		BackendName: "default",
		Direction:   DirRes,
		ClientID:    "c1",
		SessionID:   "ghost-session",
		Seq:         env.Seq,
		RemoteName:  segmentFilename(DirRes, "c1", "ghost-session", env.Seq),
		Compression: compression,
	}
	seg, err := engine.spool.SaveSegment(meta, payload)
	if err != nil {
		t.Fatalf("SaveSegment failed: %v", err)
	}
	data, err := seg.loadData()
	if err != nil {
		t.Fatalf("loadData failed: %v", err)
	}
	if err := backend.Upload(context.Background(), meta.RemoteName, bytes.NewReader(data)); err != nil {
		t.Fatalf("backend upload failed: %v", err)
	}

	engine.processSegmentFile(context.Background(), engine.pool.Get("default"), meta.RemoteName)
	engine.flushAckStates(context.Background())

	ackName := ackFilename(DirRes, "c1", "ghost-session")
	if backend.PutCalls(ackName) != 1 {
		t.Fatalf("expected orphan segment to produce one ack upload, got %d", backend.PutCalls(ackName))
	}
}

func TestResetRecoveredStateDoesNotReuploadDeadSegments(t *testing.T) {
	t.Parallel()

	engine, backend := newTestEngine(t, true)
	session := NewSession("dead-session")
	session.ClientID = "c1"
	engine.AddSession(session)
	session.EnqueueTx([]byte("hello"))
	engine.flushAll(true, false)

	segments := engine.pendingSnapshot()
	if len(segments) != 1 {
		t.Fatalf("expected 1 pending segment, got %d", len(segments))
	}
	original := segments[0]
	originalName := original.Meta.RemoteName

	restart := NewEngineWithPool(storage.NewSingleBackendPool("default", backend), true, "c1", engine.opts)
	if err := restart.resetRecoveredState(context.Background()); err != nil {
		t.Fatalf("resetRecoveredState failed: %v", err)
	}
	if got := len(restart.pendingSnapshot()); got != 0 {
		t.Fatalf("expected no pending uploads after reset, got %d", got)
	}
	if _, err := restart.spool.LoadSegments(); err != nil {
		t.Fatalf("LoadSegments failed: %v", err)
	}
	if backend.UploadCalls(originalName) != 0 {
		t.Fatalf("expected no remote upload retries after reset, got %d", backend.UploadCalls(originalName))
	}
}
