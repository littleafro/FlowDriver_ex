package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/NullLatency/flow-driver/internal/storage"
)

type metrics struct {
	mu sync.Mutex

	activeSessions     int
	bytesC2S           uint64
	bytesS2C           uint64
	segmentsPending    int
	segmentsUploaded   uint64
	segmentsDownloaded uint64
	retries            uint64
	uploadErrors       uint64
	downloadErrors     uint64
	deleteErrors       uint64
}

type Engine struct {
	pool    *storage.BackendPool
	myDir   Direction
	peerDir Direction
	id      string
	opts    Options

	sessions  map[string]*Session
	sessionMu sync.RWMutex

	closedSessions   map[string]time.Time
	closedSessionsMu sync.Mutex

	spool *spoolStore

	pendingMu        sync.RWMutex
	pending          map[string]*spoolSegment
	uploading        map[string]struct{}
	recoveredNextSeq map[string]uint64

	ackMu           sync.Mutex
	ackStates       map[string]AckState
	ackLastUploaded map[string]time.Time
	ackDirtyCounts  map[string]int
	remoteAcked     map[string]uint64
	lastRx          time.Time
	lastMetricsLog  time.Time
	seenRemote      map[string]time.Time

	flushCh     chan bool
	uploadCh    chan struct{}
	uploadSem   chan struct{}
	downloadSem chan struct{}
	deleteSem   chan struct{}

	metrics         metrics
	startedAtUnixMs int64

	ackPollMu   sync.Mutex
	ackLastPoll map[string]time.Time

	OnNewSession func(sessionID, targetAddr string, s *Session)
}

func NewEngine(backend storage.Backend, isClient bool, clientID string) *Engine {
	return NewEngineWithPool(storage.NewSingleBackendPool("default", backend), isClient, clientID, DefaultOptions())
}

func NewEngineWithPool(pool *storage.BackendPool, isClient bool, clientID string, opts Options) *Engine {
	opts.ApplyDefaults()
	e := &Engine{
		pool:             pool,
		id:               clientID,
		opts:             opts,
		sessions:         make(map[string]*Session),
		closedSessions:   make(map[string]time.Time),
		spool:            newSpoolStore(opts.DataDir),
		pending:          make(map[string]*spoolSegment),
		uploading:        make(map[string]struct{}),
		recoveredNextSeq: make(map[string]uint64),
		ackStates:        make(map[string]AckState),
		ackLastUploaded:  make(map[string]time.Time),
		ackDirtyCounts:   make(map[string]int),
		remoteAcked:      make(map[string]uint64),
		seenRemote:       make(map[string]time.Time),
		flushCh:          make(chan bool, 4),
		uploadCh:         make(chan struct{}, 1),
		uploadSem:        make(chan struct{}, opts.MaxConcurrentUploads),
		downloadSem:      make(chan struct{}, opts.MaxConcurrentDownloads),
		deleteSem:        make(chan struct{}, opts.MaxConcurrentDeletes),
		lastRx:           time.Now(),
		lastMetricsLog:   time.Now(),
		ackLastPoll:      make(map[string]time.Time),
	}
	if isClient {
		e.myDir = DirReq
		e.peerDir = DirRes
	} else {
		e.myDir = DirRes
		e.peerDir = DirReq
	}
	return e
}

func (e *Engine) SetRefreshRate(ms int) {
	if ms > 0 {
		e.opts.PollRate = time.Duration(ms) * time.Millisecond
		e.opts.ActivePollRate = e.opts.PollRate
	}
}

func (e *Engine) SetPollRate(ms int) {
	if ms > 0 {
		e.opts.PollRate = time.Duration(ms) * time.Millisecond
		e.opts.ActivePollRate = e.opts.PollRate
	}
}

func (e *Engine) SetFlushRate(ms int) {
	if ms > 0 {
		e.opts.FlushRate = time.Duration(ms) * time.Millisecond
	}
}

func (e *Engine) Start(ctx context.Context) {
	e.startedAtUnixMs = time.Now().UnixMilli()
	if err := e.resetRecoveredState(ctx); err != nil {
		log.Printf("reset recovered state failed: %v", err)
	}
	e.discardStartupBacklog(ctx)
	e.applyBackendListCutoff()
	e.deleteAckedSegments(ctx)
	go e.flushLoop(ctx)
	go e.uploadLoop(ctx)
	go e.deleteLoop(ctx)
	go e.ackLoop(ctx)
	go e.pollLoop(ctx)
	go e.maintenanceLoop(ctx)
}

func (e *Engine) Shutdown(ctx context.Context) {
	e.sessionMu.RLock()
	sessions := make([]*Session, 0, len(e.sessions))
	for _, s := range e.sessions {
		sessions = append(sessions, s)
	}
	e.sessionMu.RUnlock()

	for _, s := range sessions {
		s.QueueClose()
	}

	e.flushAll(true, true)
	e.triggerUploads(ctx)
	e.flushAckStates(ctx)
	e.deleteAckedSegments(ctx)

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if e.shutdownDrained() {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.flushAll(true, true)
			e.triggerUploads(ctx)
			e.flushAckStates(ctx)
			e.deleteAckedSegments(ctx)
		}
	}
}

func (e *Engine) GetSession(id string) *Session {
	e.sessionMu.RLock()
	defer e.sessionMu.RUnlock()
	return e.sessions[id]
}

func (e *Engine) AddSession(s *Session) {
	if s == nil {
		return
	}
	s.Configure(e.opts.MaxTxBufferBytesPerSession, e.opts.MaxOutOfOrderSegments, e.opts.SegmentBytes, e.notifyFlush, e.notifyForceFlush)
	if s.ClientID == "" && e.myDir == DirReq {
		s.ClientID = e.id
	}
	if s.ClientID == "" {
		s.ClientID = e.id
	}

	e.sessionMu.Lock()
	if nextSeq := e.recoveredNextSeq[e.sessionScopedKey(s.BackendName, s.ClientID, s.ID)]; nextSeq > 0 {
		s.mu.Lock()
		if s.txSeq < nextSeq {
			s.txSeq = nextSeq
			s.openSent = nextSeq > 0
		}
		s.mu.Unlock()
	}
	e.sessions[s.ID] = s
	count := len(e.sessions)
	e.sessionMu.Unlock()

	e.metrics.mu.Lock()
	e.metrics.activeSessions = count
	e.metrics.mu.Unlock()
	log.Printf("session open %s client=%s backend=%s", s.ID, s.ClientID, s.BackendName)
	e.notifyForceFlush()
}

func (e *Engine) RemoveSession(id string) {
	e.sessionMu.Lock()
	delete(e.sessions, id)
	count := len(e.sessions)
	e.sessionMu.Unlock()

	e.metrics.mu.Lock()
	e.metrics.activeSessions = count
	e.metrics.mu.Unlock()

	e.closedSessionsMu.Lock()
	e.closedSessions[id] = time.Now()
	e.closedSessionsMu.Unlock()
	log.Printf("session close %s", id)
}

func (e *Engine) notifyFlush() {
	select {
	case e.flushCh <- false:
	default:
	}
}

func (e *Engine) notifyForceFlush() {
	select {
	case e.flushCh <- true:
	default:
	}
}

func (e *Engine) flushLoop(ctx context.Context) {
	ticker := time.NewTicker(e.opts.FlushRate)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case force := <-e.flushCh:
			e.flushAll(force, false)
		case <-ticker.C:
			e.flushAll(true, true)
		}
	}
}

func (e *Engine) flushAll(force, allowHeartbeat bool) {
	now := time.Now()

	e.sessionMu.RLock()
	sessions := make([]*Session, 0, len(e.sessions))
	for _, s := range e.sessions {
		sessions = append(sessions, s)
	}
	e.sessionMu.RUnlock()

	for _, s := range sessions {
		if now.Sub(s.LastActivity()) >= e.opts.SessionIdleTimeout && !s.HasCloseQueued() {
			log.Printf("session %s idle for %s, queuing close", s.ID, now.Sub(s.LastActivity()))
			s.QueueClose()
		}

		backend, err := e.backendForSession(s)
		if err != nil {
			log.Printf("backend selection failed for session %s: %v", s.ID, err)
			continue
		}
		s.BackendName = backend.Name

		sessionKey := e.sessionScopedKey(backend.Name, s.ClientID, s.ID)
		if e.pendingCountForSession(sessionKey) >= e.opts.MaxPendingSegmentsPerSession {
			continue
		}

		for i := 0; i < e.opts.MaxMuxSegments; i++ {
			sendHeartbeat := allowHeartbeat && s.CanHeartbeat(now, e.opts.HeartbeatInterval)
			env, consumed, err := s.PrepareEnvelope(now, e.opts.SegmentBytes, e.opts.MaxSegmentBytes, force || i > 0, sendHeartbeat)
			if err != nil {
				log.Printf("prepare envelope failed session=%s: %v", s.ID, err)
				break
			}
			if env == nil {
				break
			}

			clientID := s.ClientID
			if clientID == "" {
				clientID = e.id
			}

			payload, compression, err := marshalSegmentPayload(env, e.opts.Compression, e.opts.CompressionMinBytes)
			if err != nil {
				log.Printf("marshal segment failed session=%s seq=%d: %v", s.ID, env.Seq, err)
				break
			}

			meta := spoolSegmentMeta{
				BackendName: backend.Name,
				Direction:   e.myDir,
				ClientID:    clientID,
				SessionID:   s.ID,
				Seq:         env.Seq,
				RemoteName:  segmentFilename(e.myDir, clientID, s.ID, env.Seq),
				Compression: compression,
			}
			seg, err := e.spool.SaveSegment(meta, payload)
			if err != nil {
				log.Printf("spool save failed session=%s seq=%d: %v", s.ID, env.Seq, err)
				break
			}
			if err := s.CommitEnvelope(env, consumed); err != nil {
				log.Printf("commit envelope failed session=%s seq=%d: %v", s.ID, env.Seq, err)
				_ = e.spool.DeleteSegment(seg)
				break
			}
			e.addPending(seg)
			if env.Kind == KindOpen {
				log.Printf("selected backend %s for session %s", backend.Name, s.ID)
			}
			if env.Kind == KindClose {
				break
			}
			if e.pendingCountForSession(sessionKey) >= e.opts.MaxPendingSegmentsPerSession {
				break
			}
		}
	}
}

func (e *Engine) uploadLoop(ctx context.Context) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.uploadCh:
			e.triggerUploads(ctx)
		case <-ticker.C:
			e.triggerUploads(ctx)
		}
	}
}

func (e *Engine) deleteLoop(ctx context.Context) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.deleteAckedSegments(ctx)
		}
	}
}

func (e *Engine) triggerUploads(ctx context.Context) {
	for _, seg := range e.pendingSnapshot() {
		if seg.Meta.Abandoned {
			continue
		}
		if seg.Meta.Uploaded {
			continue
		}
		if seg.Meta.NextUploadUnixMs > time.Now().UnixMilli() {
			continue
		}
		backend := e.pool.Get(seg.Meta.BackendName)
		if backend == nil {
			continue
		}
		uploadKey := e.pendingKey(seg.Meta.BackendName, seg.Meta.RemoteName)
		if !e.tryStartUpload(uploadKey) {
			continue
		}
		select {
		case e.uploadSem <- struct{}{}:
			go e.uploadSegment(ctx, backend, seg, uploadKey)
		default:
			e.finishUpload(uploadKey)
			return
		}
	}
}

func (e *Engine) uploadSegment(ctx context.Context, backend *storage.BackendHandle, seg *spoolSegment, uploadKey string) {
	defer releaseSemaphore(e.uploadSem)
	defer e.finishUpload(uploadKey)

	data, err := seg.loadData()
	if err != nil {
		log.Printf("load spool data failed %s: %v", seg.Meta.RemoteName, err)
		e.metrics.mu.Lock()
		e.metrics.uploadErrors++
		e.metrics.mu.Unlock()
		return
	}

	if err := backend.Backend.Upload(ctx, seg.Meta.RemoteName, bytes.NewReader(data)); err != nil {
		seg.Meta.UploadAttempts++
		seg.Meta.NextUploadUnixMs = time.Now().Add(time.Second).UnixMilli()
		_ = seg.saveMeta()
		log.Printf("upload error backend=%s file=%s: %v", backend.Name, seg.Meta.RemoteName, err)
		e.metrics.mu.Lock()
		e.metrics.uploadErrors++
		e.metrics.retries++
		e.metrics.mu.Unlock()
		return
	}

	seg.Meta.Uploaded = true
	seg.Meta.NextUploadUnixMs = 0
	if err := seg.saveMeta(); err != nil {
		log.Printf("save upload metadata failed %s: %v", seg.Meta.RemoteName, err)
	}
	log.Printf("segment uploaded backend=%s file=%s", backend.Name, seg.Meta.RemoteName)
	e.metrics.mu.Lock()
	e.metrics.segmentsUploaded++
	e.metrics.mu.Unlock()
}

func (e *Engine) ackLoop(ctx context.Context) {
	interval := e.opts.AckInterval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.flushAckStates(ctx)
		}
	}
}

func (e *Engine) flushAckStates(ctx context.Context) {
	now := time.Now()

	e.ackMu.Lock()
	snapshot := make(map[string]AckState, len(e.ackStates))
	for key, ack := range e.ackStates {
		dirtyCount := e.ackDirtyCounts[key]
		if dirtyCount == 0 {
			continue
		}
		lastUploaded := e.ackLastUploaded[key]
		if dirtyCount < e.opts.AckEverySegments && now.Sub(lastUploaded) < e.opts.AckInterval {
			continue
		}
		snapshot[key] = ack
	}
	e.ackMu.Unlock()

	for key, ack := range snapshot {
		backend := e.pool.Get(ackBackendKey(key))
		if backend == nil {
			continue
		}
		filename := ackFilename(ack.Direction, ack.ClientID, ack.SessionID)
		payload, _ := json.MarshalIndent(ack, "", "  ")
		if err := backend.Backend.Put(ctx, filename, bytes.NewReader(payload)); err != nil {
			log.Printf("ack upload failed backend=%s file=%s: %v", backend.Name, filename, err)
			e.metrics.mu.Lock()
			e.metrics.retries++
			e.metrics.uploadErrors++
			e.metrics.mu.Unlock()
			continue
		}

		uploadedAt := time.Now()
		e.ackMu.Lock()
		e.ackLastUploaded[key] = uploadedAt
		e.ackDirtyCounts[key] = 0
		e.ackMu.Unlock()
		if err := e.spool.MarkAckUploaded(backend.Name, filename, uploadedAt.UnixMilli()); err != nil && !os.IsNotExist(err) {
			log.Printf("mark ack uploaded failed backend=%s file=%s: %v", backend.Name, filename, err)
		}
		log.Printf("ack update backend=%s file=%s acked_seq=%d", backend.Name, filename, ack.AckedSeq)
	}
}

func (e *Engine) pollLoop(ctx context.Context) {
	timer := time.NewTimer(e.currentPollInterval())
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			for _, backend := range e.pool.Backends() {
				e.pollBackend(ctx, backend)
			}
			timer.Reset(e.currentPollInterval())
		}
	}
}

func (e *Engine) pollBackend(ctx context.Context, backend *storage.BackendHandle) {
	segmentPrefix := string(e.peerDir) + "-"
	ackPrefix := "ack-" + string(e.myDir) + "-"
	if e.myDir == DirReq {
		idPrefix := safeID(e.id)
		segmentPrefix += idPrefix + "-"
		ackPrefix += idPrefix + "-"
	}

	segmentFiles, err := backend.Backend.ListQuery(ctx, segmentPrefix)
	if err != nil {
		log.Printf("poll segment list error backend=%s prefix=%s: %v", backend.Name, segmentPrefix, err)
		return
	}
	workerCount := e.opts.MaxConcurrentDownloads
	if workerCount < 1 {
		workerCount = 1
	}
	if workerCount > len(segmentFiles) {
		workerCount = len(segmentFiles)
	}
	if workerCount == 0 {
		workerCount = 1
	}
	segmentJobs := make(chan string, workerCount)
	var segmentWG sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		segmentWG.Add(1)
		go func() {
			defer segmentWG.Done()
			for name := range segmentJobs {
				e.processSegmentFile(ctx, backend, name)
			}
		}()
	}
	for _, name := range segmentFiles {
		segmentJobs <- name
	}
	close(segmentJobs)
	segmentWG.Wait()

	if !e.shouldPollAcks(backend.Name) {
		return
	}

	ackFiles, err := backend.Backend.ListQuery(ctx, ackPrefix)
	if err != nil {
		log.Printf("poll ack list error backend=%s prefix=%s: %v", backend.Name, ackPrefix, err)
		return
	}
	for _, name := range ackFiles {
		e.processAckFile(ctx, backend, name)
	}
}

func (e *Engine) processSegmentFile(ctx context.Context, backend *storage.BackendHandle, name string) {
	dir, clientID, sessionID, seq, ok := parseSegmentFilename(name)
	if !ok || dir != e.peerDir {
		return
	}
	locallyClosed := e.isClosedSession(sessionID)

	localAckKey := e.ackStateKey(backend.Name, ackFilename(e.peerDir, clientID, sessionID))
	e.ackMu.Lock()
	localAck, hasLocalAck := e.ackStates[localAckKey]
	seenAt := e.seenRemote[backend.Name+"|"+name]
	e.ackMu.Unlock()
	if hasLocalAck && seq <= localAck.AckedSeq {
		return
	}
	if !seenAt.IsZero() && time.Since(seenAt) < e.opts.StaleUnackedFileTTL {
		return
	}

	session := e.GetSession(sessionID)
	shouldAckWithoutDownload := false
	switch {
	case session == nil && e.myDir == DirReq:
		// Client side never accepts peer-initiated sessions.
		shouldAckWithoutDownload = true
	case session == nil && seq > 0:
		// If open(0) was never observed locally, higher seq segments are stale/orphaned.
		shouldAckWithoutDownload = true
	case session == nil && locallyClosed:
		shouldAckWithoutDownload = true
	}
	if shouldAckWithoutDownload {
		e.markSeenRemote(backend.Name, name)
		e.noteAck(backend.Name, clientID, sessionID, seq)
		if seq == 0 {
			e.markClosedSession(sessionID)
		}
		if e.shouldLogOrphan(seq) {
			log.Printf("discarded orphan segment backend=%s file=%s session=%s seq=%d closed=%t", backend.Name, name, sessionID, seq, locallyClosed)
		}
		return
	}

	select {
	case e.downloadSem <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-e.downloadSem }()

	rc, err := backend.Backend.Download(ctx, name)
	if err != nil {
		if storage.IsNotFoundError(err) {
			e.markSeenRemote(backend.Name, name)
			return
		}
		log.Printf("segment download error backend=%s file=%s: %v", backend.Name, name, err)
		e.metrics.mu.Lock()
		e.metrics.downloadErrors++
		e.metrics.mu.Unlock()
		return
	}
	defer rc.Close()

	payload, err := io.ReadAll(rc)
	if err != nil {
		log.Printf("segment read error backend=%s file=%s: %v", backend.Name, name, err)
		e.metrics.mu.Lock()
		e.metrics.downloadErrors++
		e.metrics.mu.Unlock()
		return
	}
	env, err := unmarshalSegmentPayload(payload)
	if err != nil {
		log.Printf("segment decode error backend=%s file=%s: %v", backend.Name, name, err)
		e.metrics.mu.Lock()
		e.metrics.downloadErrors++
		e.metrics.mu.Unlock()
		return
	}

	session = e.GetSession(sessionID)
	if session == nil && !locallyClosed && e.myDir == DirRes && env.Kind == KindOpen {
		if e.shouldDropStartupOpen(env.CreatedUnixMs) {
			e.markSeenRemote(backend.Name, name)
			e.markClosedSession(sessionID)
			e.noteAck(backend.Name, clientID, sessionID, seq)
			log.Printf("discarded startup stale open backend=%s file=%s session=%s seq=%d created=%d started=%d", backend.Name, name, sessionID, seq, env.CreatedUnixMs, e.startedAtUnixMs)
			return
		}
		if env.CreatedUnixMs > 0 && time.Since(time.UnixMilli(env.CreatedUnixMs)) > e.opts.StaleUnackedFileTTL {
			e.markSeenRemote(backend.Name, name)
			e.markClosedSession(sessionID)
			e.noteAck(backend.Name, clientID, sessionID, seq)
			log.Printf("discarded stale open segment backend=%s file=%s session=%s seq=%d age=%s", backend.Name, name, sessionID, seq, time.Since(time.UnixMilli(env.CreatedUnixMs)))
			return
		}
		session = NewSession(sessionID)
		session.ClientID = clientID
		session.BackendName = backend.Name
		session.TargetAddr = env.TargetAddr
		e.AddSession(session)
		if e.OnNewSession != nil {
			go e.OnNewSession(sessionID, env.TargetAddr, session)
		}
	}
	if session == nil {
		e.markSeenRemote(backend.Name, name)
		e.noteAck(backend.Name, clientID, sessionID, seq)
		if e.shouldLogOrphan(seq) {
			log.Printf("discarded orphan segment backend=%s file=%s session=%s seq=%d closed=%t", backend.Name, name, sessionID, seq, locallyClosed)
		}
		return
	}

	ackedSeq, advanced, closedNow, err := session.ProcessRx(env)
	if err != nil {
		log.Printf("process segment failed session=%s seq=%d: %v", sessionID, seq, err)
		return
	}
	e.markSeenRemote(backend.Name, name)
	if advanced {
		e.noteAck(backend.Name, clientID, sessionID, ackedSeq)
		e.ackMu.Lock()
		e.lastRx = time.Now()
		e.ackMu.Unlock()
		if closedNow {
			e.markClosedSession(sessionID)
		}
		e.metrics.mu.Lock()
		e.metrics.segmentsDownloaded++
		if e.peerDir == DirReq {
			e.metrics.bytesC2S += uint64(len(env.Payload))
		} else {
			e.metrics.bytesS2C += uint64(len(env.Payload))
		}
		e.metrics.mu.Unlock()
		log.Printf("segment downloaded backend=%s file=%s seq=%d", backend.Name, name, seq)
	}
}

func (e *Engine) processAckFile(ctx context.Context, backend *storage.BackendHandle, name string) {
	dir, clientID, sessionID, ok := parseAckFilename(name)
	if !ok || dir != e.myDir {
		return
	}
	if e.myDir == DirReq && clientID != e.id {
		return
	}

	rc, err := backend.Backend.Download(ctx, name)
	if err != nil {
		log.Printf("ack download error backend=%s file=%s: %v", backend.Name, name, err)
		return
	}
	defer rc.Close()

	var ack AckState
	if err := json.NewDecoder(rc).Decode(&ack); err != nil {
		log.Printf("ack decode error backend=%s file=%s: %v", backend.Name, name, err)
		return
	}

	key := e.remoteAckKey(backend.Name, clientID, sessionID)
	e.ackMu.Lock()
	if current, ok := e.remoteAcked[key]; ok && current >= ack.AckedSeq {
		e.ackMu.Unlock()
		return
	}
	e.remoteAcked[key] = ack.AckedSeq
	e.ackMu.Unlock()
	log.Printf("ack received backend=%s file=%s acked_seq=%d", backend.Name, name, ack.AckedSeq)
}

func (e *Engine) maintenanceLoop(ctx context.Context) {
	ticker := time.NewTicker(e.opts.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.cleanupClosedSessions()
			e.cleanupSeenRemote()
			e.deleteAckedSegments(ctx)
			e.cleanupSessions()
			e.logMetrics()
		}
	}
}

func (e *Engine) cleanupClosedSessions() {
	e.closedSessionsMu.Lock()
	for id, ts := range e.closedSessions {
		if time.Since(ts) >= e.opts.TombstoneTTL {
			delete(e.closedSessions, id)
		}
	}
	e.closedSessionsMu.Unlock()
}

func (e *Engine) cleanupSeenRemote() {
	e.ackMu.Lock()
	for key, ts := range e.seenRemote {
		if time.Since(ts) >= e.opts.StaleUnackedFileTTL {
			delete(e.seenRemote, key)
		}
	}
	e.ackMu.Unlock()
}

func (e *Engine) deleteAckedSegments(ctx context.Context) {
	for _, seg := range e.pendingSnapshot() {
		abandoned := seg.Meta.Abandoned
		if abandoned {
			if !seg.Meta.Uploaded || seg.Meta.DeletedRemote {
				if err := e.spool.DeleteSegment(seg); err != nil {
					log.Printf("delete abandoned local spool failed %s: %v", seg.Meta.RemoteName, err)
					continue
				}
				e.removePending(seg)
				log.Printf("cleanup action backend=%s abandoned_local=%s", seg.Meta.BackendName, seg.Meta.RemoteName)
				continue
			}
		}
		if !seg.Meta.Uploaded {
			continue
		}
		if seg.Meta.DeletedRemote {
			_ = e.spool.DeleteSegment(seg)
			e.removePending(seg)
			continue
		}
		if seg.Meta.NextDeleteUnixMs > time.Now().UnixMilli() {
			continue
		}
		if !abandoned && !e.isAcked(seg) {
			if time.Since(time.UnixMilli(seg.Meta.CreatedUnixMs)) >= e.opts.StaleUnackedFileTTL {
				log.Printf("stale unacked segment backend=%s file=%s age=%s", seg.Meta.BackendName, seg.Meta.RemoteName, time.Since(time.UnixMilli(seg.Meta.CreatedUnixMs)))
			}
			continue
		}

		backend := e.pool.Get(seg.Meta.BackendName)
		if backend == nil {
			continue
		}
		select {
		case e.deleteSem <- struct{}{}:
			go e.deleteSegment(ctx, backend, seg)
		default:
			return
		}
	}
}

func (e *Engine) deleteSegment(ctx context.Context, backend *storage.BackendHandle, seg *spoolSegment) {
	defer releaseSemaphore(e.deleteSem)
	if err := backend.Backend.Delete(ctx, seg.Meta.RemoteName); err != nil {
		seg.Meta.DeleteAttempts++
		seg.Meta.NextDeleteUnixMs = time.Now().Add(time.Second).UnixMilli()
		_ = seg.saveMeta()
		log.Printf("delete error backend=%s file=%s: %v", backend.Name, seg.Meta.RemoteName, err)
		e.metrics.mu.Lock()
		e.metrics.deleteErrors++
		e.metrics.retries++
		e.metrics.mu.Unlock()
		return
	}
	seg.Meta.DeletedRemote = true
	seg.Meta.NextDeleteUnixMs = 0
	if err := seg.saveMeta(); err != nil {
		log.Printf("save delete metadata failed %s: %v", seg.Meta.RemoteName, err)
	}
	if err := e.spool.DeleteSegment(seg); err != nil {
		log.Printf("delete local spool failed %s: %v", seg.Meta.RemoteName, err)
		return
	}
	e.removePending(seg)
	log.Printf("cleanup action backend=%s deleted=%s", backend.Name, seg.Meta.RemoteName)
}

func (e *Engine) cleanupSessions() {
	now := time.Now()
	e.sessionMu.RLock()
	sessions := make([]*Session, 0, len(e.sessions))
	for _, s := range e.sessions {
		sessions = append(sessions, s)
	}
	e.sessionMu.RUnlock()

	for _, s := range sessions {
		if s.PendingBytes() > 0 {
			continue
		}
		if e.pendingCountForSession(e.sessionScopedKey(s.BackendName, s.ClientID, s.ID)) > 0 {
			continue
		}
		if s.ReadyForTeardown(now, e.closeDrainTimeout()) {
			e.RemoveSession(s.ID)
		}
	}
}

func (e *Engine) logMetrics() {
	e.metrics.mu.Lock()
	defer e.metrics.mu.Unlock()
	if time.Since(e.lastMetricsLog) < e.opts.CleanupInterval {
		return
	}
	e.pendingMu.RLock()
	e.metrics.segmentsPending = len(e.pending)
	e.pendingMu.RUnlock()
	log.Printf("metrics active_sessions=%d bytes_c2s=%d bytes_s2c=%d segments_pending=%d segments_uploaded=%d segments_downloaded=%d retries=%d upload_errors=%d download_errors=%d delete_errors=%d",
		e.metrics.activeSessions,
		e.metrics.bytesC2S,
		e.metrics.bytesS2C,
		e.metrics.segmentsPending,
		e.metrics.segmentsUploaded,
		e.metrics.segmentsDownloaded,
		e.metrics.retries,
		e.metrics.uploadErrors,
		e.metrics.downloadErrors,
		e.metrics.deleteErrors,
	)
	e.lastMetricsLog = time.Now()
}

func (e *Engine) recoverLocalState() error {
	if err := os.MkdirAll(e.opts.DataDir, 0755); err != nil {
		return err
	}

	segments, err := e.spool.LoadSegments()
	if err != nil {
		return err
	}
	for _, seg := range segments {
		e.addPending(seg)
		key := e.sessionScopedKey(seg.Meta.BackendName, seg.Meta.ClientID, seg.Meta.SessionID)
		if next := seg.Meta.Seq + 1; next > e.recoveredNextSeq[key] {
			e.recoveredNextSeq[key] = next
		}
	}

	acks, err := e.spool.LoadAcks()
	if err != nil {
		return err
	}
	e.ackMu.Lock()
	for backendName, files := range acks {
		for filename, record := range files {
			key := e.ackStateKey(backendName, filename)
			e.ackStates[key] = record.AckState()
			if record.LastUploadedUnixMs > 0 {
				e.ackLastUploaded[key] = time.UnixMilli(record.LastUploadedUnixMs)
			}
			if record.LastUploadedUnixMs < record.UpdatedUnixMs {
				e.ackDirtyCounts[key] = 1
			}
		}
	}
	e.ackMu.Unlock()
	return nil
}

func (e *Engine) resetRecoveredState(ctx context.Context) error {
	if err := os.MkdirAll(e.opts.DataDir, 0755); err != nil {
		return err
	}

	segments, err := e.spool.LoadSegments()
	if err != nil {
		return err
	}

	abandonedUploads := 0
	removedLocal := 0
	for _, seg := range segments {
		if seg.Meta.Uploaded && !seg.Meta.DeletedRemote {
			seg.Meta.Abandoned = true
			seg.Meta.NextUploadUnixMs = 0
			if err := seg.saveMeta(); err != nil {
				return err
			}
			e.addPending(seg)
			abandonedUploads++
			continue
		}
		if err := e.spool.DeleteSegment(seg); err != nil && !os.IsNotExist(err) {
			return err
		}
		removedLocal++
	}

	acks, err := e.spool.LoadAcks()
	if err != nil {
		return err
	}
	removedAcks := 0
	for backendName, files := range acks {
		backend := e.pool.Get(backendName)
		for filename := range files {
			if backend != nil {
				if err := backend.Backend.Delete(ctx, filename); err != nil {
					log.Printf("startup ack cleanup delete failed backend=%s file=%s: %v", backendName, filename, err)
				}
			}
			if err := e.spool.DeleteAck(backendName, filename); err != nil && !os.IsNotExist(err) {
				return err
			}
			removedAcks++
		}
	}

	if abandonedUploads > 0 || removedLocal > 0 || removedAcks > 0 {
		log.Printf("startup cleanup removed_local_segments=%d abandoned_uploaded_segments=%d removed_acks=%d", removedLocal, abandonedUploads, removedAcks)
	}
	return nil
}

func (e *Engine) currentPollInterval() time.Duration {
	e.sessionMu.RLock()
	active := len(e.sessions)
	e.sessionMu.RUnlock()

	e.pendingMu.RLock()
	pending := len(e.pending)
	e.pendingMu.RUnlock()

	e.ackMu.Lock()
	lastRx := e.lastRx
	e.ackMu.Unlock()

	if time.Since(lastRx) <= 3*e.opts.ActivePollRate {
		return e.opts.ActivePollRate
	}
	if active > 0 || pending > 0 {
		return e.opts.PollRate
	}
	return e.opts.IdlePollRate
}

func (e *Engine) backendForSession(s *Session) (*storage.BackendHandle, error) {
	if s.BackendName != "" {
		if backend := e.pool.Get(s.BackendName); backend != nil {
			return backend, nil
		}
	}
	clientID := s.ClientID
	if clientID == "" {
		clientID = e.id
	}
	backend, err := e.pool.Select(fmt.Sprintf("%s/%s/%s", e.opts.TunnelID, clientID, s.ID))
	if err != nil {
		return nil, err
	}
	return backend, nil
}

func (e *Engine) addPending(seg *spoolSegment) {
	e.pendingMu.Lock()
	e.pending[e.pendingKey(seg.Meta.BackendName, seg.Meta.RemoteName)] = seg
	e.pendingMu.Unlock()
	select {
	case e.uploadCh <- struct{}{}:
	default:
	}
}

func (e *Engine) removePending(seg *spoolSegment) {
	e.pendingMu.Lock()
	key := e.pendingKey(seg.Meta.BackendName, seg.Meta.RemoteName)
	delete(e.pending, key)
	delete(e.uploading, key)
	e.pendingMu.Unlock()
}

func (e *Engine) pendingSnapshot() []*spoolSegment {
	e.pendingMu.RLock()
	defer e.pendingMu.RUnlock()
	out := make([]*spoolSegment, 0, len(e.pending))
	for _, seg := range e.pending {
		out = append(out, seg)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Meta.CreatedUnixMs != out[j].Meta.CreatedUnixMs {
			return out[i].Meta.CreatedUnixMs < out[j].Meta.CreatedUnixMs
		}
		return out[i].Meta.RemoteName < out[j].Meta.RemoteName
	})
	return out
}

func (e *Engine) pendingCountForSession(sessionKey string) int {
	e.pendingMu.RLock()
	defer e.pendingMu.RUnlock()
	count := 0
	for _, seg := range e.pending {
		if e.sessionScopedKey(seg.Meta.BackendName, seg.Meta.ClientID, seg.Meta.SessionID) == sessionKey {
			count++
		}
	}
	return count
}

func (e *Engine) noteAck(backendName, clientID, sessionID string, ackedSeq uint64) {
	filename := ackFilename(e.peerDir, clientID, sessionID)
	key := e.ackStateKey(backendName, filename)

	e.ackMu.Lock()
	current, ok := e.ackStates[key]
	if ok && current.AckedSeq >= ackedSeq {
		e.ackMu.Unlock()
		return
	}
	ack := AckState{
		ClientID:      clientID,
		SessionID:     sessionID,
		Direction:     e.peerDir,
		AckedSeq:      ackedSeq,
		UpdatedUnixMs: time.Now().UnixMilli(),
	}
	e.ackStates[key] = ack
	e.ackDirtyCounts[key]++
	e.ackMu.Unlock()

	if _, err := e.spool.SaveAck(backendName, filename, ack); err != nil {
		log.Printf("save ack state failed backend=%s file=%s: %v", backendName, filename, err)
	}
}

func (e *Engine) isAcked(seg *spoolSegment) bool {
	key := e.remoteAckKey(seg.Meta.BackendName, seg.Meta.ClientID, seg.Meta.SessionID)
	e.ackMu.Lock()
	defer e.ackMu.Unlock()
	ackedSeq, ok := e.remoteAcked[key]
	return ok && seg.Meta.Seq <= ackedSeq
}

func (e *Engine) isClosedSession(id string) bool {
	e.closedSessionsMu.Lock()
	defer e.closedSessionsMu.Unlock()
	_, ok := e.closedSessions[id]
	return ok
}

func (e *Engine) markClosedSession(id string) {
	e.closedSessionsMu.Lock()
	e.closedSessions[id] = time.Now()
	e.closedSessionsMu.Unlock()
}

func (e *Engine) tryStartUpload(key string) bool {
	e.pendingMu.Lock()
	defer e.pendingMu.Unlock()
	if _, ok := e.uploading[key]; ok {
		return false
	}
	e.uploading[key] = struct{}{}
	return true
}

func (e *Engine) finishUpload(key string) {
	e.pendingMu.Lock()
	delete(e.uploading, key)
	e.pendingMu.Unlock()
}

func (e *Engine) markSeenRemote(backendName, name string) {
	e.ackMu.Lock()
	e.seenRemote[backendName+"|"+name] = time.Now()
	e.lastRx = time.Now()
	e.ackMu.Unlock()
}

func (e *Engine) shouldLogOrphan(seq uint64) bool {
	return seq == 0 || seq%32 == 0
}

func (e *Engine) shouldPollAcks(backendName string) bool {
	e.pendingMu.RLock()
	hasPending := len(e.pending) > 0
	e.pendingMu.RUnlock()

	interval := e.opts.AckInterval
	if interval < 500*time.Millisecond {
		interval = 500 * time.Millisecond
	}
	if !hasPending {
		interval = 5 * time.Second
	}

	now := time.Now()
	e.ackPollMu.Lock()
	defer e.ackPollMu.Unlock()
	last := e.ackLastPoll[backendName]
	if !last.IsZero() && now.Sub(last) < interval {
		return false
	}
	e.ackLastPoll[backendName] = now
	return true
}

func (e *Engine) shouldDropStartupOpen(createdUnixMs int64) bool {
	if e.startedAtUnixMs == 0 || createdUnixMs <= 0 {
		return false
	}
	const startupGrace = int64(15 * time.Second / time.Millisecond)
	return createdUnixMs+startupGrace < e.startedAtUnixMs
}

func (e *Engine) discardStartupBacklog(ctx context.Context) {
	for _, backend := range e.pool.Backends() {
		segmentPrefix := string(e.peerDir) + "-"
		if e.myDir == DirReq {
			segmentPrefix += safeID(e.id) + "-"
		}

		files, err := backend.Backend.ListQuery(ctx, segmentPrefix)
		if err != nil {
			log.Printf("startup stale discard list error backend=%s prefix=%s: %v", backend.Name, segmentPrefix, err)
			continue
		}
		if len(files) == 0 {
			continue
		}

		maxSeqBySession := make(map[string]uint64)
		segmentCount := 0
		for _, name := range files {
			dir, clientID, sessionID, seq, ok := parseSegmentFilename(name)
			if !ok || dir != e.peerDir {
				continue
			}
			segmentCount++
			key := clientID + "|" + sessionID
			current, exists := maxSeqBySession[key]
			if !exists || seq > current {
				maxSeqBySession[key] = seq
			}
			e.markSeenRemote(backend.Name, name)
		}

		if len(maxSeqBySession) == 0 {
			continue
		}

		for key, maxSeq := range maxSeqBySession {
			clientID, sessionID, ok := strings.Cut(key, "|")
			if !ok {
				continue
			}
			e.markClosedSession(sessionID)
			e.noteAck(backend.Name, clientID, sessionID, maxSeq)
		}
		log.Printf("startup stale discard backend=%s sessions=%d segments=%d", backend.Name, len(maxSeqBySession), segmentCount)
	}
	e.flushAckStates(ctx)
}

func (e *Engine) applyBackendListCutoff() {
	cutoff := e.startedAtUnixMs - int64(5*time.Second/time.Millisecond)
	if cutoff < 0 {
		cutoff = 0
	}
	for _, backend := range e.pool.Backends() {
		filterable, ok := backend.Backend.(interface {
			SetListCreatedAfter(unixMs int64)
		})
		if !ok {
			continue
		}
		filterable.SetListCreatedAfter(cutoff)
		log.Printf("backend %s list cutoff applied created_after=%d", backend.Name, cutoff)
	}
}

func (e *Engine) pendingKey(backendName, remoteName string) string {
	return backendName + "|" + remoteName
}

func (e *Engine) sessionScopedKey(backendName, clientID, sessionID string) string {
	return backendName + "|" + clientID + "|" + sessionID
}

func (e *Engine) ackStateKey(backendName, filename string) string {
	return backendName + "|" + filename
}

func ackBackendKey(key string) string {
	parts := strings.SplitN(key, "|", 2)
	return parts[0]
}

func (e *Engine) remoteAckKey(backendName, clientID, sessionID string) string {
	return backendName + "|" + clientID + "|" + sessionID
}

func releaseSemaphore(sem chan struct{}) {
	select {
	case <-sem:
	default:
	}
}

func (e *Engine) closeDrainTimeout() time.Duration {
	timeout := e.opts.ActivePollRate * 2
	if timeout < e.opts.FlushRate*2 {
		timeout = e.opts.FlushRate * 2
	}
	if timeout < time.Second {
		timeout = time.Second
	}
	return timeout
}

func (e *Engine) shutdownDrained() bool {
	e.pendingMu.RLock()
	hasPending := len(e.pending) > 0
	e.pendingMu.RUnlock()
	if hasPending {
		return false
	}

	e.ackMu.Lock()
	defer e.ackMu.Unlock()
	for _, dirty := range e.ackDirtyCounts {
		if dirty > 0 {
			return false
		}
	}
	return true
}
