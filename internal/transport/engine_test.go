package transport

import (
	"bytes"
	"context"
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
	engine := NewEngineWithPool(storage.NewSingleBackendPool("default", backend), isClient, "c1", opts)
	return engine, backend
}

func drainUploadSignals(ch chan struct{}) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
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
	uploadKey := engine.pendingKey(seg.Meta.BackendName, seg.Meta.RemoteName)

	engine.uploadSegment(context.Background(), handle, seg, uploadKey)
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

	engine.uploadSegment(context.Background(), handle, seg, uploadKey)
	if !seg.Meta.Uploaded {
		t.Fatalf("segment should upload on retry")
	}
}

func TestFlushAllBatchesContiguousEnvelopesIntoSingleRemoteSegment(t *testing.T) {
	t.Parallel()

	engine, _ := newTestEngine(t, true)
	engine.opts.SegmentBytes = 4
	engine.opts.MaxSegmentBytes = 4
	engine.opts.MaxMuxSegments = 8

	session := NewSession("s-batch")
	session.ClientID = "c1"
	engine.AddSession(session)
	session.EnqueueTx([]byte("abcdefghijkl"))

	engine.flushAll(true, false)

	segments := engine.pendingSnapshot()
	if len(segments) != 1 {
		t.Fatalf("expected 1 batched pending segment, got %d", len(segments))
	}
	if segments[0].Meta.EndSeq <= segments[0].Meta.Seq {
		t.Fatalf("expected batched segment to span multiple seqs, got start=%d end=%d", segments[0].Meta.Seq, segments[0].Meta.EndSeq)
	}

	data, err := segments[0].loadData()
	if err != nil {
		t.Fatalf("loadData failed: %v", err)
	}
	envs, err := unmarshalSegmentPayloads(data)
	if err != nil {
		t.Fatalf("unmarshalSegmentPayloads failed: %v", err)
	}
	if len(envs) < 2 {
		t.Fatalf("expected multiple envelopes in batch, got %d", len(envs))
	}
	if envs[0].Kind != KindOpen {
		t.Fatalf("expected first envelope to be open, got %s", envs[0].Kind)
	}
}

func TestUploadedMuxFileRemainsRemoteUntilReceiverDeletes(t *testing.T) {
	t.Parallel()

	engine, backend := newTestEngine(t, true)

	session := NewSession("s1")
	session.ClientID = "c1"
	engine.AddSession(session)
	session.EnqueueTx([]byte("hello"))
	engine.flushAll(true, false)

	seg := engine.pendingSnapshot()[0]
	engine.uploadSegment(context.Background(), engine.pool.Get("default"), seg, engine.pendingKey(seg.Meta.BackendName, seg.Meta.RemoteName))
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
		t.Fatalf("uploaded mux file should remain on backend until receiver cleanup")
	}
	if len(engine.pendingSnapshot()) != 0 {
		t.Fatalf("uploaded mux file should be removed from local pending spool after successful upload")
	}
}

func TestProcessMuxFileDeletesRemoteObjectAfterDelivery(t *testing.T) {
	t.Parallel()

	engine, backend := newTestEngine(t, false)

	openEnv := &Envelope{
		SessionID:     "s1",
		Seq:           0,
		Kind:          KindOpen,
		TargetAddr:    "1.1.1.1:443",
		Payload:       []byte("hello"),
		CreatedUnixMs: time.Now().UnixMilli(),
	}
	openEnv.EnsureChecksum()
	dataEnv := &Envelope{
		SessionID:     "s1",
		Seq:           1,
		Kind:          KindData,
		Payload:       []byte("world"),
		CreatedUnixMs: time.Now().UnixMilli(),
	}
	dataEnv.EnsureChecksum()

	payload, _, err := marshalSegmentPayloads([]*Envelope{openEnv, dataEnv}, "off", 0)
	if err != nil {
		t.Fatalf("marshalSegmentPayloads failed: %v", err)
	}
	name := muxFilename(DirReq, "c1", time.Now().Add(-time.Second).UnixNano())
	if err := backend.Upload(context.Background(), name, bytes.NewReader(payload)); err != nil {
		t.Fatalf("backend upload failed: %v", err)
	}

	engine.processMuxFile(context.Background(), engine.pool.Get("default"), name)

	deadline := time.Now().Add(2 * time.Second)
	for backend.HasFile(name) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if backend.HasFile(name) {
		t.Fatalf("receiver should delete mux file after processing")
	}
	session := engine.GetSession("s1")
	if session == nil {
		t.Fatalf("expected session to be created")
	}

	got1 := <-session.RxChan
	got2 := <-session.RxChan
	if string(got1) != "hello" || string(got2) != "world" {
		t.Fatalf("unexpected payload order %q %q", got1, got2)
	}

	engine.flushAckStates(context.Background())
	if got := backend.PutCalls(ackFilename(DirReq, "c1", "s1")); got != 0 {
		t.Fatalf("mux delivery should not upload ack files, got %d puts", got)
	}
}

func TestProcessMuxFileRetainsFileUntilAllEnvelopesCanBeApplied(t *testing.T) {
	t.Parallel()

	engine, backend := newTestEngine(t, false)
	session := NewSession("s1")
	engine.AddSession(session)

	session.mu.Lock()
	session.maxOutOfOrder = 0
	session.mu.Unlock()

	dataEnv := &Envelope{
		SessionID:     "s1",
		Seq:           1,
		Kind:          KindData,
		Payload:       []byte("one"),
		CreatedUnixMs: time.Now().UnixMilli(),
	}
	dataEnv.EnsureChecksum()
	dataPayload, _, err := marshalSegmentPayloads([]*Envelope{dataEnv}, "off", 0)
	if err != nil {
		t.Fatalf("marshalSegmentPayloads(data) failed: %v", err)
	}
	dataName := muxFilename(DirReq, "c1", time.Now().Add(-2*time.Second).UnixNano())
	if err := backend.Upload(context.Background(), dataName, bytes.NewReader(dataPayload)); err != nil {
		t.Fatalf("backend upload(data) failed: %v", err)
	}

	engine.processMuxFile(context.Background(), engine.pool.Get("default"), dataName)

	if !backend.HasFile(dataName) {
		t.Fatalf("mux file with unapplied data should remain remote for retry")
	}

	engine.ackMu.Lock()
	_, seen := engine.seenRemote["default|"+dataName]
	engine.ackMu.Unlock()
	if seen {
		t.Fatalf("failed mux file must not be hidden behind seen cache")
	}

	openEnv := &Envelope{
		SessionID:     "s1",
		Seq:           0,
		Kind:          KindOpen,
		TargetAddr:    "1.1.1.1:443",
		Payload:       []byte("zero"),
		CreatedUnixMs: time.Now().UnixMilli(),
	}
	openEnv.EnsureChecksum()
	openPayload, _, err := marshalSegmentPayloads([]*Envelope{openEnv}, "off", 0)
	if err != nil {
		t.Fatalf("marshalSegmentPayloads(open) failed: %v", err)
	}
	openName := muxFilename(DirReq, "c1", time.Now().Add(-time.Second).UnixNano())
	if err := backend.Upload(context.Background(), openName, bytes.NewReader(openPayload)); err != nil {
		t.Fatalf("backend upload(open) failed: %v", err)
	}

	engine.processMuxFile(context.Background(), engine.pool.Get("default"), openName)
	engine.processMuxFile(context.Background(), engine.pool.Get("default"), dataName)

	deadline := time.Now().Add(2 * time.Second)
	for backend.HasFile(dataName) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if backend.HasFile(dataName) {
		t.Fatalf("mux file should delete after retry succeeds")
	}

	got0 := <-session.RxChan
	got1 := <-session.RxChan
	if string(got0) != "zero" || string(got1) != "one" {
		t.Fatalf("unexpected retried mux delivery order %q %q", got0, got1)
	}
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

func TestTriggerUploadsDoesNotDuplicateInFlight(t *testing.T) {
	t.Parallel()

	engine, backend := newTestEngine(t, true)
	backend.SetVisibilityDelay(300 * time.Millisecond)

	session := NewSession("s-upload")
	session.ClientID = "c1"
	engine.AddSession(session)
	session.EnqueueTx([]byte("hello"))
	engine.flushAll(true, false)

	segments := engine.pendingSnapshot()
	if len(segments) != 1 {
		t.Fatalf("expected one pending segment, got %d", len(segments))
	}
	name := segments[0].Meta.RemoteName

	for i := 0; i < 10; i++ {
		engine.triggerUploads(context.Background())
	}
	time.Sleep(150 * time.Millisecond)

	if got := backend.UploadCalls(name); got != 1 {
		t.Fatalf("expected single upload attempt for in-flight segment, got %d", got)
	}
}

func TestUploadSegmentSignalsUploadLoopAfterCompletion(t *testing.T) {
	t.Parallel()

	engine, _ := newTestEngine(t, true)
	session := NewSession("s-upload-signal")
	session.ClientID = "c1"
	engine.AddSession(session)
	session.EnqueueTx([]byte("hello"))
	engine.flushAll(true, false)

	segments := engine.pendingSnapshot()
	if len(segments) != 1 {
		t.Fatalf("expected one pending segment, got %d", len(segments))
	}
	seg := segments[0]
	uploadKey := engine.pendingKey(seg.Meta.BackendName, seg.Meta.RemoteName)

	drainUploadSignals(engine.uploadCh)
	engine.uploadSegment(context.Background(), engine.pool.Get("default"), seg, uploadKey)

	select {
	case <-engine.uploadCh:
	default:
		t.Fatalf("expected upload completion to signal upload loop")
	}
}

func TestForceFlushDebounceIntervalClamp(t *testing.T) {
	t.Parallel()

	engine, _ := newTestEngine(t, true)

	engine.opts.FlushRate = 5 * time.Millisecond
	if got := engine.forceFlushDebounceInterval(); got != 15*time.Millisecond {
		t.Fatalf("expected min debounce 15ms, got %s", got)
	}

	engine.opts.FlushRate = 10 * time.Second
	if got := engine.forceFlushDebounceInterval(); got != 75*time.Millisecond {
		t.Fatalf("expected max debounce 75ms, got %s", got)
	}

	engine.opts.FlushRate = 200 * time.Millisecond
	if got := engine.forceFlushDebounceInterval(); got != 50*time.Millisecond {
		t.Fatalf("expected debounce 50ms for 200ms flush, got %s", got)
	}
}

func TestFlushLoopCoalescesForceFlushBurst(t *testing.T) {
	t.Parallel()

	engine, _ := newTestEngine(t, true)
	engine.opts.FlushRate = time.Second
	engine.opts.SegmentBytes = 1024
	engine.opts.MaxSegmentBytes = 1
	engine.opts.MaxMuxSegments = 1

	session := NewSession("s-burst")
	session.ClientID = "c1"
	engine.AddSession(session)
	session.EnqueueTx([]byte("abcdef"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go engine.flushLoop(ctx)

	for i := 0; i < 32; i++ {
		engine.notifyForceFlush()
	}

	deadline := time.Now().Add(400 * time.Millisecond)
	for {
		if len(engine.pendingSnapshot()) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for coalesced force flush")
		}
		time.Sleep(5 * time.Millisecond)
	}

	if got := len(engine.pendingSnapshot()); got != 1 {
		t.Fatalf("expected one segment from coalesced force flush burst, got %d", got)
	}
}

func TestBackendForSessionPrefersLessLoadedBackend(t *testing.T) {
	t.Parallel()

	opts := DefaultOptions()
	opts.DataDir = t.TempDir()
	engine := NewEngineWithPool(storage.NewBackendPool(
		&storage.BackendHandle{Name: "g1", Weight: 1, Backend: storage.NewFakeBackend()},
		&storage.BackendHandle{Name: "g2", Weight: 1, Backend: storage.NewFakeBackend()},
	), true, "c1", opts)

	for i := 0; i < 3; i++ {
		s := NewSession(fmt.Sprintf("busy-%d", i))
		s.ClientID = "c1"
		s.BackendName = "g1"
		engine.AddSession(s)
	}

	newSession := NewSession("new-session")
	newSession.ClientID = "c1"
	backend, err := engine.backendForSession(newSession)
	if err != nil {
		t.Fatalf("backendForSession failed: %v", err)
	}
	if backend.Name != "g2" {
		t.Fatalf("expected less-loaded backend g2, got %s", backend.Name)
	}
}

func TestPollAllBackendsRunsConcurrently(t *testing.T) {
	t.Parallel()

	opts := DefaultOptions()
	opts.DataDir = t.TempDir()

	b1 := storage.NewFakeBackend()
	b2 := storage.NewFakeBackend()
	delay := 150 * time.Millisecond
	b1.SetListDelay(delay)
	b2.SetListDelay(delay)

	engine := NewEngineWithPool(storage.NewBackendPool(
		&storage.BackendHandle{Name: "g1", Weight: 1, Backend: b1},
		&storage.BackendHandle{Name: "g2", Weight: 1, Backend: b2},
	), true, "c1", opts)

	start := time.Now()
	engine.pollAllBackends(context.Background())
	elapsed := time.Since(start)
	if elapsed >= 250*time.Millisecond {
		t.Fatalf("expected concurrent polling (<250ms), got %s", elapsed)
	}
}

func TestClientOrphanSeqSkipsDownload(t *testing.T) {
	t.Parallel()

	engine, backend := newTestEngine(t, true)
	env := &Envelope{
		SessionID:     "orphan-client",
		Seq:           7,
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
		SessionID:   env.SessionID,
		Seq:         env.Seq,
		RemoteName:  segmentFilename(DirRes, "c1", env.SessionID, env.Seq),
		Compression: compression,
	}
	if err := backend.Upload(context.Background(), meta.RemoteName, bytes.NewReader(payload)); err != nil {
		t.Fatalf("backend upload failed: %v", err)
	}

	engine.processSegmentFile(context.Background(), engine.pool.Get("default"), meta.RemoteName)
	engine.flushAckStates(context.Background())

	if got := backend.DownloadCalls(meta.RemoteName); got != 0 {
		t.Fatalf("expected orphan seq>0 to be acked by metadata without download, got %d download calls", got)
	}
	ackName := ackFilename(DirRes, "c1", env.SessionID)
	if backend.PutCalls(ackName) != 1 {
		t.Fatalf("expected orphan metadata ack upload, got %d", backend.PutCalls(ackName))
	}
}

func TestServerBuffersPreOpenSegmentUntilOpenArrives(t *testing.T) {
	t.Parallel()

	engine, backend := newTestEngine(t, false)
	sessionID := "pre-open"
	ackName := ackFilename(DirReq, "c1", sessionID)

	dataEnv := &Envelope{
		SessionID:     sessionID,
		Seq:           1,
		Kind:          KindData,
		Payload:       []byte("hello"),
		CreatedUnixMs: time.Now().UnixMilli(),
	}
	dataEnv.EnsureChecksum()
	dataPayload, _, err := marshalSegmentPayload(dataEnv, "off", 0)
	if err != nil {
		t.Fatalf("marshalSegmentPayload(data) failed: %v", err)
	}
	dataName := segmentFilename(DirReq, "c1", sessionID, dataEnv.Seq)
	if err := backend.Upload(context.Background(), dataName, bytes.NewReader(dataPayload)); err != nil {
		t.Fatalf("backend upload(data) failed: %v", err)
	}

	engine.processSegmentFile(context.Background(), engine.pool.Get("default"), dataName)
	engine.flushAckStates(context.Background())

	if s := engine.GetSession(sessionID); s != nil {
		t.Fatalf("server should not create a session before open arrives")
	}
	if backend.PutCalls(ackName) != 0 {
		t.Fatalf("pre-open data must not be acked before open, got %d ack uploads", backend.PutCalls(ackName))
	}

	openEnv := &Envelope{
		SessionID:     sessionID,
		Seq:           0,
		Kind:          KindOpen,
		TargetAddr:    "1.1.1.1:443",
		CreatedUnixMs: time.Now().UnixMilli(),
	}
	openEnv.EnsureChecksum()
	openPayload, _, err := marshalSegmentPayload(openEnv, "off", 0)
	if err != nil {
		t.Fatalf("marshalSegmentPayload(open) failed: %v", err)
	}
	openName := segmentFilename(DirReq, "c1", sessionID, openEnv.Seq)
	if err := backend.Upload(context.Background(), openName, bytes.NewReader(openPayload)); err != nil {
		t.Fatalf("backend upload(open) failed: %v", err)
	}

	engine.processSegmentFile(context.Background(), engine.pool.Get("default"), openName)

	session := engine.GetSession(sessionID)
	if session == nil {
		t.Fatalf("server should create a session once open arrives")
	}

	select {
	case got := <-session.RxChan:
		if string(got) != "hello" {
			t.Fatalf("unexpected buffered payload %q", got)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timed out waiting for buffered payload delivery")
	}

	engine.flushAckStates(context.Background())

	key := engine.ackStateKey("default", ackName)
	engine.ackMu.Lock()
	ack := engine.ackStates[key]
	engine.ackMu.Unlock()
	if ack.AckedSeq != 1 {
		t.Fatalf("expected acked_seq=1 after replaying buffered data, got %d", ack.AckedSeq)
	}
}

func TestPollBackendProcessesSingleSessionInOrder(t *testing.T) {
	t.Parallel()

	engine, backend := newTestEngine(t, false)
	engine.opts.MaxConcurrentDownloads = 3
	engine.opts.MaxOutOfOrderSegments = 1

	sessionID := "ordered-poll"
	makeSegment := func(seq uint64, kind EnvelopeKind, target string, payload string) string {
		env := &Envelope{
			SessionID:     sessionID,
			Seq:           seq,
			Kind:          kind,
			TargetAddr:    target,
			Payload:       []byte(payload),
			CreatedUnixMs: time.Now().UnixMilli(),
		}
		env.EnsureChecksum()
		encoded, _, err := marshalSegmentPayload(env, "off", 0)
		if err != nil {
			t.Fatalf("marshalSegmentPayload seq=%d failed: %v", seq, err)
		}
		name := segmentFilename(DirReq, "c1", sessionID, seq)
		if err := backend.Upload(context.Background(), name, bytes.NewReader(encoded)); err != nil {
			t.Fatalf("backend upload seq=%d failed: %v", seq, err)
		}
		return name
	}

	name0 := makeSegment(0, KindOpen, "1.1.1.1:443", "zero")
	makeSegment(1, KindData, "", "one")
	makeSegment(2, KindData, "", "two")
	backend.SetDownloadDelay(name0, 150*time.Millisecond)

	engine.pollBackend(context.Background(), engine.pool.Get("default"))

	session := engine.GetSession(sessionID)
	if session == nil {
		t.Fatalf("expected session to be created")
	}

	got0 := <-session.RxChan
	got1 := <-session.RxChan
	got2 := <-session.RxChan
	if string(got0) != "zero" || string(got1) != "one" || string(got2) != "two" {
		t.Fatalf("unexpected delivery order %q %q %q", got0, got1, got2)
	}

	key := engine.ackStateKey("default", ackFilename(DirReq, "c1", sessionID))
	engine.ackMu.Lock()
	ack := engine.ackStates[key]
	engine.ackMu.Unlock()
	if ack.AckedSeq != 2 {
		t.Fatalf("expected acked_seq=2 after ordered poll processing, got %d", ack.AckedSeq)
	}
}

func TestPollBackendProcessesMuxFilesPerClientInOrder(t *testing.T) {
	t.Parallel()

	engine, backend := newTestEngine(t, false)
	engine.opts.MaxConcurrentDownloads = 3
	engine.opts.MaxOutOfOrderSegments = 1

	sessionID := "ordered-mux"
	makeMux := func(ts int64, env *Envelope) string {
		env.EnsureChecksum()
		payload, _, err := marshalSegmentPayloads([]*Envelope{env}, "off", 0)
		if err != nil {
			t.Fatalf("marshalSegmentPayloads seq=%d failed: %v", env.Seq, err)
		}
		name := muxFilename(DirReq, "c1", ts)
		if err := backend.Upload(context.Background(), name, bytes.NewReader(payload)); err != nil {
			t.Fatalf("backend upload seq=%d failed: %v", env.Seq, err)
		}
		return name
	}

	makeMux(1003, &Envelope{
		SessionID:     sessionID,
		Seq:           2,
		Kind:          KindData,
		Payload:       []byte("two"),
		CreatedUnixMs: time.Now().UnixMilli(),
	})
	makeMux(1002, &Envelope{
		SessionID:     sessionID,
		Seq:           1,
		Kind:          KindData,
		Payload:       []byte("one"),
		CreatedUnixMs: time.Now().UnixMilli(),
	})
	makeMux(1001, &Envelope{
		SessionID:     sessionID,
		Seq:           0,
		Kind:          KindOpen,
		TargetAddr:    "1.1.1.1:443",
		Payload:       []byte("zero"),
		CreatedUnixMs: time.Now().UnixMilli(),
	})
	backend.SetReverseList(true)

	engine.pollBackend(context.Background(), engine.pool.Get("default"))

	session := engine.GetSession(sessionID)
	if session == nil {
		t.Fatalf("expected session to be created")
	}

	got0 := <-session.RxChan
	got1 := <-session.RxChan
	got2 := <-session.RxChan
	if string(got0) != "zero" || string(got1) != "one" || string(got2) != "two" {
		t.Fatalf("unexpected mux delivery order %q %q %q", got0, got1, got2)
	}

	engine.flushAckStates(context.Background())
	if got := backend.PutCalls(ackFilename(DirReq, "c1", sessionID)); got != 0 {
		t.Fatalf("mux poll should not upload ack files, got %d puts", got)
	}
}

func TestDownloadNotFoundDebouncedBySeenCache(t *testing.T) {
	t.Parallel()

	engine, backend := newTestEngine(t, false)
	name := segmentFilename(DirReq, "c1", "ghost", 0)

	engine.processSegmentFile(context.Background(), engine.pool.Get("default"), name)
	engine.processSegmentFile(context.Background(), engine.pool.Get("default"), name)

	if got := backend.DownloadCalls(name); got != 1 {
		t.Fatalf("expected only one download attempt for not-found segment, got %d", got)
	}
}

func TestServerDropsStaleOpen(t *testing.T) {
	t.Parallel()

	engine, backend := newTestEngine(t, false)
	sessionID := "stale-open"
	env := &Envelope{
		SessionID:     sessionID,
		Seq:           0,
		Kind:          KindOpen,
		TargetAddr:    "1.1.1.1:443",
		CreatedUnixMs: time.Now().Add(-time.Second).UnixMilli(),
	}
	env.EnsureChecksum()
	payload, _, err := marshalSegmentPayload(env, "off", 0)
	if err != nil {
		t.Fatalf("marshalSegmentPayload failed: %v", err)
	}
	name := segmentFilename(DirReq, "c1", sessionID, 0)
	if err := backend.Upload(context.Background(), name, bytes.NewReader(payload)); err != nil {
		t.Fatalf("backend upload failed: %v", err)
	}

	engine.processSegmentFile(context.Background(), engine.pool.Get("default"), name)
	engine.flushAckStates(context.Background())

	if s := engine.GetSession(sessionID); s != nil {
		t.Fatalf("stale open should not create a session")
	}
	ackName := ackFilename(DirReq, "c1", sessionID)
	if backend.PutCalls(ackName) != 1 {
		t.Fatalf("stale open should be acked for cleanup, got %d ack uploads", backend.PutCalls(ackName))
	}
}

func TestConfigureBackendListCutoff(t *testing.T) {
	t.Parallel()

	engine, backend := newTestEngine(t, true)
	engine.startedAtUnixMs = time.Now().UnixMilli()

	configured := engine.configureBackendListCutoff()
	if _, ok := configured["default"]; !ok {
		t.Fatalf("expected cutoff to be configured for default backend")
	}

	got := backend.ListCreatedAfter()
	if got <= 0 {
		t.Fatalf("expected positive list cutoff, got %d", got)
	}
	if got > engine.startedAtUnixMs {
		t.Fatalf("cutoff must not be after startup time: cutoff=%d startup=%d", got, engine.startedAtUnixMs)
	}
}

func TestCleanupRemoteStaleMuxFilesDeletesOnlyExpired(t *testing.T) {
	t.Parallel()

	engine, backend := newTestEngine(t, true)
	oldName := muxFilename(DirReq, "c1", time.Now().Add(-2*engine.remoteStaleFileTTL()).UnixNano())
	freshName := muxFilename(DirReq, "c1", time.Now().Add(-engine.remoteStaleFileTTL()/4).UnixNano())
	legacyName := segmentFilename(DirReq, "c1", "s1", 0)

	for _, name := range []string{oldName, freshName, legacyName} {
		if err := backend.Upload(context.Background(), name, bytes.NewReader([]byte("x"))); err != nil {
			t.Fatalf("backend upload failed for %s: %v", name, err)
		}
	}

	engine.cleanupRemoteStaleFiles(context.Background())

	if backend.HasFile(oldName) {
		t.Fatalf("expired mux file should be deleted")
	}
	if !backend.HasFile(freshName) {
		t.Fatalf("fresh mux file should remain")
	}
	if !backend.HasFile(legacyName) {
		t.Fatalf("legacy segment file should remain")
	}
}
