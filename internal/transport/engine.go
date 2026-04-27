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

type preOpenSegment struct {
	remoteName string
	envs       []*Envelope
	needsAck   bool
	bufferedAt time.Time
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
	deleting         map[string]struct{}
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
	startupCutoffMs int64

	ackPollMu   sync.Mutex
	ackLastPoll map[string]time.Time

	preOpenMu         sync.Mutex
	preOpen           map[string]map[uint64]*preOpenSegment
	remoteCleanupMu   sync.Mutex
	lastRemoteCleanup time.Time

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
		deleting:         make(map[string]struct{}),
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
		preOpen:          make(map[string]map[uint64]*preOpenSegment),
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
	cutoffBackends := e.configureBackendListCutoff()
	e.discardStartupBacklog(ctx, cutoffBackends)
	e.cleanupRemoteStaleFiles(ctx)
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

	debounce := e.forceFlushDebounceInterval()
	var forceTimer *time.Timer
	var forceTimerCh <-chan time.Time

	stopForceTimer := func() {
		if forceTimer == nil {
			return
		}
		if !forceTimer.Stop() {
			select {
			case <-forceTimer.C:
			default:
			}
		}
		forceTimer = nil
		forceTimerCh = nil
	}
	scheduleForceFlush := func() {
		if debounce <= 0 {
			e.flushAll(true, false)
			return
		}
		if forceTimer != nil {
			return
		}
		forceTimer = time.NewTimer(debounce)
		forceTimerCh = forceTimer.C
	}

	for {
		select {
		case <-ctx.Done():
			stopForceTimer()
			return
		case force := <-e.flushCh:
			if force {
				scheduleForceFlush()
				continue
			}
			e.flushAll(false, false)
		case <-forceTimerCh:
			stopForceTimer()
			e.flushAll(true, false)
		case <-ticker.C:
			stopForceTimer()
			e.flushAll(true, true)
		}
	}
}

func (e *Engine) flushAll(force, allowHeartbeat bool) {
	now := time.Now()

	type preparedGroup struct {
		backendName string
		clientID    string
		sessions    []*Session
		prepared    [][]PreparedEnvelope
		envs        []*Envelope
	}

	e.sessionMu.RLock()
	sessions := make([]*Session, 0, len(e.sessions))
	for _, s := range e.sessions {
		sessions = append(sessions, s)
	}
	e.sessionMu.RUnlock()

	groups := make(map[string]*preparedGroup)
	groupOrder := make([]string, 0)

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

		sendHeartbeat := allowHeartbeat && s.CanHeartbeat(now, e.opts.HeartbeatInterval)
		prepared, err := s.PrepareEnvelopeBatch(now, e.opts.SegmentBytes, e.opts.MaxSegmentBytes, e.opts.MaxMuxSegments, force, sendHeartbeat)
		if err != nil {
			log.Printf("prepare envelope batch failed session=%s: %v", s.ID, err)
			continue
		}
		if len(prepared) == 0 {
			continue
		}

		clientID := s.ClientID
		if clientID == "" {
			clientID = e.id
		}
		groupKey := backend.Name + "|" + clientID
		group := groups[groupKey]
		if group == nil {
			group = &preparedGroup{
				backendName: backend.Name,
				clientID:    clientID,
			}
			groups[groupKey] = group
			groupOrder = append(groupOrder, groupKey)
		}
		group.sessions = append(group.sessions, s)
		group.prepared = append(group.prepared, prepared)
		for _, item := range prepared {
			group.envs = append(group.envs, item.Env)
		}
	}

	for _, key := range groupOrder {
		group := groups[key]
		if group == nil || len(group.envs) == 0 {
			continue
		}

		payload, compression, err := marshalSegmentPayloads(group.envs, e.opts.Compression, e.opts.CompressionMinBytes)
		if err != nil {
			log.Printf("marshal mux payload failed backend=%s client=%s envs=%d: %v", group.backendName, group.clientID, len(group.envs), err)
			continue
		}

		remoteName := muxFilename(e.myDir, group.clientID, time.Now().UnixNano())
		meta := spoolSegmentMeta{
			BackendName: group.backendName,
			Direction:   e.myDir,
			ClientID:    group.clientID,
			SessionID:   "__mux__",
			Seq:         0,
			EndSeq:      uint64(len(group.envs) - 1),
			RemoteName:  remoteName,
			Compression: compression,
		}
		seg, err := e.spool.SaveSegment(meta, payload)
		if err != nil {
			log.Printf("spool save failed backend=%s client=%s envs=%d: %v", group.backendName, group.clientID, len(group.envs), err)
			continue
		}

		commitOK := true
		for i, session := range group.sessions {
			for _, item := range group.prepared[i] {
				if err := session.CommitEnvelope(item.Env, item.Consumed); err != nil {
					log.Printf("commit envelope failed session=%s seq=%d: %v", session.ID, item.Env.Seq, err)
					commitOK = false
					break
				}
			}
			if !commitOK {
				break
			}
		}
		if !commitOK {
			_ = e.spool.DeleteSegment(seg)
			continue
		}

		e.addPending(seg)
		for _, env := range group.envs {
			if env.Kind == KindOpen {
				log.Printf("selected backend %s for session %s", group.backendName, env.SessionID)
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
	defer e.signalUpload()
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

	if _, _, _, ok := parseMuxFilename(seg.Meta.RemoteName); ok {
		if err := e.spool.DeleteSegment(seg); err != nil {
			log.Printf("delete local spool failed %s: %v", seg.Meta.RemoteName, err)
			return
		}
		e.removePending(seg)
	}
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
	segmentFiles = e.filterSeenRemote(backend.Name, segmentFiles)
	segmentFiles = dedupeStrings(segmentFiles)
	muxFiles := make([]string, 0)
	legacyFiles := make([]string, 0, len(segmentFiles))
	for _, name := range segmentFiles {
		if dir, _, _, ok := parseMuxFilename(name); ok && dir == e.peerDir {
			muxFiles = append(muxFiles, name)
			continue
		}
		legacyFiles = append(legacyFiles, name)
	}

	if len(muxFiles) > 0 {
		sort.Strings(muxFiles)

		if e.opts.MaxOutOfOrderSegments <= 1 {
			// Strict ordering mode: preserve per-client file order when out-of-order
			// buffering is disabled/tight.
			clientBatches := make(map[string][]string)
			clientOrder := make([]string, 0)
			for _, name := range muxFiles {
				_, clientID, _, ok := parseMuxFilename(name)
				if !ok {
					continue
				}
				key := backend.Name + "|" + clientID
				if _, exists := clientBatches[key]; !exists {
					clientOrder = append(clientOrder, key)
				}
				clientBatches[key] = append(clientBatches[key], name)
			}

			for key := range clientBatches {
				sort.Strings(clientBatches[key])
			}

			muxWorkers := e.opts.MaxConcurrentDownloads
			if muxWorkers < 1 {
				muxWorkers = 1
			}
			if muxWorkers > len(clientOrder) {
				muxWorkers = len(clientOrder)
			}
			if muxWorkers == 0 {
				muxWorkers = 1
			}
			muxJobs := make(chan []string, muxWorkers)
			var muxWG sync.WaitGroup
			for i := 0; i < muxWorkers; i++ {
				muxWG.Add(1)
				go func() {
					defer muxWG.Done()
					for batch := range muxJobs {
						for _, name := range batch {
							e.processMuxFile(ctx, backend, name)
						}
					}
				}()
			}
			for _, key := range clientOrder {
				muxJobs <- clientBatches[key]
			}
			close(muxJobs)
			muxWG.Wait()
		} else {
			// Fast path: allow concurrent mux processing to avoid serializing all
			// files behind a single client stream.
			muxWorkers := e.opts.MaxConcurrentDownloads
			if muxWorkers < 1 {
				muxWorkers = 1
			}
			if muxWorkers > len(muxFiles) {
				muxWorkers = len(muxFiles)
			}
			if muxWorkers == 0 {
				muxWorkers = 1
			}
			muxJobs := make(chan string, muxWorkers)
			var muxWG sync.WaitGroup
			for i := 0; i < muxWorkers; i++ {
				muxWG.Add(1)
				go func() {
					defer muxWG.Done()
					for name := range muxJobs {
						e.processMuxFile(ctx, backend, name)
					}
				}()
			}
			for _, name := range muxFiles {
				muxJobs <- name
			}
			close(muxJobs)
			muxWG.Wait()
		}
	}

	if len(legacyFiles) == 0 {
		if !e.shouldPollAcks(backend.Name) {
			return
		}

		ackFiles, err := backend.Backend.ListQuery(ctx, ackPrefix)
		if err != nil {
			log.Printf("poll ack list error backend=%s prefix=%s: %v", backend.Name, ackPrefix, err)
			return
		}
		ackFiles = dedupeStrings(ackFiles)
		for _, name := range ackFiles {
			e.processAckFile(ctx, backend, name)
		}
		return
	}

	sessionBatches := make(map[string][]string)
	sessionOrder := make([]string, 0)
	for _, name := range legacyFiles {
		dir, clientID, sessionID, _, _, ok := parseSegmentFilename(name)
		if !ok || dir != e.peerDir {
			continue
		}
		key := backend.Name + "|" + clientID + "|" + sessionID
		if _, exists := sessionBatches[key]; !exists {
			sessionOrder = append(sessionOrder, key)
		}
		sessionBatches[key] = append(sessionBatches[key], name)
	}
	for key := range sessionBatches {
		sort.Strings(sessionBatches[key])
	}

	workerCount := e.opts.MaxConcurrentDownloads
	if workerCount < 1 {
		workerCount = 1
	}
	if workerCount > len(sessionOrder) {
		workerCount = len(sessionOrder)
	}
	if workerCount == 0 {
		workerCount = 1
	}
	segmentJobs := make(chan []string, workerCount)
	var segmentWG sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		segmentWG.Add(1)
		go func() {
			defer segmentWG.Done()
			for batch := range segmentJobs {
				for _, name := range batch {
					e.processSegmentFile(ctx, backend, name)
				}
			}
		}()
	}
	for _, key := range sessionOrder {
		segmentJobs <- sessionBatches[key]
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
	ackFiles = dedupeStrings(ackFiles)
	for _, name := range ackFiles {
		e.processAckFile(ctx, backend, name)
	}
}

func (e *Engine) processSegmentFile(ctx context.Context, backend *storage.BackendHandle, name string) {
	dir, clientID, sessionID, startSeq, endSeq, ok := parseSegmentFilename(name)
	if !ok || dir != e.peerDir {
		return
	}
	locallyClosed := e.isClosedSession(sessionID)

	localAckKey := e.ackStateKey(backend.Name, ackFilename(e.peerDir, clientID, sessionID))
	e.ackMu.Lock()
	localAck, hasLocalAck := e.ackStates[localAckKey]
	seenAt := e.seenRemote[backend.Name+"|"+name]
	e.ackMu.Unlock()
	if hasLocalAck && endSeq <= localAck.AckedSeq {
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
	case session == nil && locallyClosed:
		shouldAckWithoutDownload = true
	}
	if shouldAckWithoutDownload {
		e.markSeenRemote(backend.Name, name)
		e.noteAck(backend.Name, clientID, sessionID, endSeq)
		if startSeq == 0 {
			e.markClosedSession(sessionID)
		}
		if e.shouldLogOrphan(startSeq) {
			log.Printf("discarded orphan segment backend=%s file=%s session=%s seq=%d end_seq=%d closed=%t", backend.Name, name, sessionID, startSeq, endSeq, locallyClosed)
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
	envs, err := unmarshalSegmentPayloads(payload)
	if err != nil {
		log.Printf("segment decode error backend=%s file=%s: %v", backend.Name, name, err)
		e.metrics.mu.Lock()
		e.metrics.downloadErrors++
		e.metrics.mu.Unlock()
		return
	}

	if len(envs) == 0 {
		e.markSeenRemote(backend.Name, name)
		return
	}

	session = e.GetSession(sessionID)
	firstEnv := envs[0]
	if session == nil && !locallyClosed && e.myDir == DirRes && firstEnv.Kind == KindOpen {
		if e.shouldDropStartupOpen(firstEnv.CreatedUnixMs) {
			e.markSeenRemote(backend.Name, name)
			e.markClosedSession(sessionID)
			e.noteAck(backend.Name, clientID, sessionID, endSeq)
			log.Printf("discarded startup stale open backend=%s file=%s session=%s seq=%d end_seq=%d created=%d started=%d", backend.Name, name, sessionID, startSeq, endSeq, firstEnv.CreatedUnixMs, e.startedAtUnixMs)
			return
		}
		if firstEnv.CreatedUnixMs > 0 && time.Since(time.UnixMilli(firstEnv.CreatedUnixMs)) > e.opts.StaleUnackedFileTTL {
			e.markSeenRemote(backend.Name, name)
			e.markClosedSession(sessionID)
			e.noteAck(backend.Name, clientID, sessionID, endSeq)
			log.Printf("discarded stale open segment backend=%s file=%s session=%s seq=%d end_seq=%d age=%s", backend.Name, name, sessionID, startSeq, endSeq, time.Since(time.UnixMilli(firstEnv.CreatedUnixMs)))
			return
		}
		session = NewSession(sessionID)
		session.ClientID = clientID
		session.BackendName = backend.Name
		session.TargetAddr = firstEnv.TargetAddr
		e.AddSession(session)
		if e.OnNewSession != nil {
			go e.OnNewSession(sessionID, firstEnv.TargetAddr, session)
		}
	}
	if session == nil && !locallyClosed && e.myDir == DirRes && startSeq > 0 {
		if e.bufferPreOpenSegment(backend.Name, clientID, sessionID, name, envs, true) {
			e.markSeenRemote(backend.Name, name)
			log.Printf("buffered pre-open segment backend=%s file=%s session=%s seq=%d end_seq=%d", backend.Name, name, sessionID, startSeq, endSeq)
			return
		}
		log.Printf("pre-open buffer full backend=%s file=%s session=%s seq=%d end_seq=%d", backend.Name, name, sessionID, startSeq, endSeq)
		return
	}
	if session == nil {
		e.markSeenRemote(backend.Name, name)
		e.noteAck(backend.Name, clientID, sessionID, endSeq)
		if e.shouldLogOrphan(startSeq) {
			log.Printf("discarded orphan segment backend=%s file=%s session=%s seq=%d end_seq=%d closed=%t", backend.Name, name, sessionID, startSeq, endSeq, locallyClosed)
		}
		return
	}

	e.applyReceivedSegments(backend.Name, clientID, sessionID, name, session, envs, true, true)
	if firstEnv.Kind == KindOpen {
		for _, pending := range e.takePreOpenSegments(backend.Name, clientID, sessionID) {
			e.applyReceivedSegments(backend.Name, clientID, sessionID, pending.remoteName, session, pending.envs, pending.needsAck, true)
		}
	}
}

func (e *Engine) processMuxFile(ctx context.Context, backend *storage.BackendHandle, name string) {
	dir, clientID, _, ok := parseMuxFilename(name)
	if !ok || dir != e.peerDir {
		return
	}

	e.ackMu.Lock()
	seenAt := e.seenRemote[backend.Name+"|"+name]
	e.ackMu.Unlock()
	if !seenAt.IsZero() && time.Since(seenAt) < e.opts.StaleUnackedFileTTL {
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
		log.Printf("mux download error backend=%s file=%s: %v", backend.Name, name, err)
		e.metrics.mu.Lock()
		e.metrics.downloadErrors++
		e.metrics.mu.Unlock()
		return
	}
	defer rc.Close()

	payload, err := io.ReadAll(rc)
	if err != nil {
		log.Printf("mux read error backend=%s file=%s: %v", backend.Name, name, err)
		e.metrics.mu.Lock()
		e.metrics.downloadErrors++
		e.metrics.mu.Unlock()
		return
	}

	envs, err := unmarshalSegmentPayloads(payload)
	if err != nil {
		log.Printf("mux decode error backend=%s file=%s: %v", backend.Name, name, err)
		e.metrics.mu.Lock()
		e.metrics.downloadErrors++
		e.metrics.mu.Unlock()
		return
	}

	handledAll := true
	for _, env := range envs {
		locallyClosed := e.isClosedSession(env.SessionID)
		session := e.GetSession(env.SessionID)

		if session == nil && !locallyClosed && e.myDir == DirRes && env.Kind == KindOpen {
			if e.shouldDropStartupOpen(env.CreatedUnixMs) {
				e.markSeenRemote(backend.Name, name)
				e.markClosedSession(env.SessionID)
				continue
			}
			if env.CreatedUnixMs > 0 && time.Since(time.UnixMilli(env.CreatedUnixMs)) > e.opts.StaleUnackedFileTTL {
				e.markSeenRemote(backend.Name, name)
				e.markClosedSession(env.SessionID)
				continue
			}
			session = NewSession(env.SessionID)
			session.ClientID = clientID
			session.BackendName = backend.Name
			session.TargetAddr = env.TargetAddr
			e.AddSession(session)
			if e.OnNewSession != nil {
				go e.OnNewSession(env.SessionID, env.TargetAddr, session)
			}
		}

		if session == nil && !locallyClosed && e.myDir == DirRes && env.Seq > 0 {
			if e.bufferPreOpenSegment(backend.Name, clientID, env.SessionID, name, []*Envelope{env}, false) {
				continue
			}
			handledAll = false
			continue
		}

		if session == nil {
			continue
		}

		if !e.applyReceivedSegment(backend.Name, clientID, env.SessionID, name, session, env, false, false) {
			handledAll = false
			continue
		}
		if env.Kind == KindOpen {
			for _, pending := range e.takePreOpenSegments(backend.Name, clientID, env.SessionID) {
				if !e.applyReceivedSegments(backend.Name, clientID, env.SessionID, pending.remoteName, session, pending.envs, pending.needsAck, false) {
					handledAll = false
					break
				}
			}
		}
	}

	if !handledAll {
		log.Printf("mux deferred backend=%s file=%s waiting_for_retry=true", backend.Name, name)
		return
	}

	// Keep mux delivery hot: don't block further downloads on delete latency.
	e.markSeenRemote(backend.Name, name)
	e.deleteMuxFileAsync(ctx, backend, name)
}

func (e *Engine) deleteMuxFileAsync(ctx context.Context, backend *storage.BackendHandle, name string) {
	go func() {
		select {
		case e.deleteSem <- struct{}{}:
		case <-ctx.Done():
			return
		}
		defer releaseSemaphore(e.deleteSem)

		var lastErr error
		for attempt := 1; attempt <= 3; attempt++ {
			if err := backend.Backend.Delete(ctx, name); err != nil {
				lastErr = err
				if ctx.Err() != nil {
					return
				}
				time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
				continue
			}
			log.Printf("cleanup action backend=%s deleted=%s", backend.Name, name)
			return
		}

		if lastErr != nil {
			log.Printf("mux delete error backend=%s file=%s: %v", backend.Name, name, lastErr)
		}
	}()
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
			e.cleanupPreOpenSegments()
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
		deleteKey := e.pendingKey(seg.Meta.BackendName, seg.Meta.RemoteName)
		if !e.tryStartDelete(deleteKey) {
			continue
		}
		select {
		case e.deleteSem <- struct{}{}:
			go e.deleteSegment(ctx, backend, seg, deleteKey)
		default:
			e.finishDelete(deleteKey)
			return
		}
	}
}

func (e *Engine) deleteSegment(ctx context.Context, backend *storage.BackendHandle, seg *spoolSegment, deleteKey string) {
	defer releaseSemaphore(e.deleteSem)
	defer e.finishDelete(deleteKey)
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
		endSeq := seg.Meta.EndSeq
		if endSeq < seg.Meta.Seq {
			endSeq = seg.Meta.Seq
		}
		if next := endSeq + 1; next > e.recoveredNextSeq[key] {
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
	abandonedByBackend := make(map[string]struct{})
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
			abandonedByBackend[seg.Meta.BackendName] = struct{}{}
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
	ackBackends := make(map[string]struct{})
	for backendName, files := range acks {
		if len(files) > 0 {
			ackBackends[backendName] = struct{}{}
		}
	}
	e.warmRemoteDeleteCache(ctx, abandonedByBackend, ackBackends)

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

func (e *Engine) warmRemoteDeleteCache(ctx context.Context, segmentBackends, ackBackends map[string]struct{}) {
	segmentPrefix := string(e.myDir) + "-"
	ackPrefix := "ack-" + string(e.myDir) + "-"
	if e.myDir == DirReq {
		idPrefix := safeID(e.id) + "-"
		segmentPrefix += idPrefix
		ackPrefix += idPrefix
	}

	for backendName := range segmentBackends {
		backend := e.pool.Get(backendName)
		if backend == nil {
			continue
		}
		if _, err := backend.Backend.ListQuery(ctx, segmentPrefix); err != nil {
			log.Printf("startup delete-cache warmup failed backend=%s prefix=%s: %v", backendName, segmentPrefix, err)
		}
	}
	for backendName := range ackBackends {
		backend := e.pool.Get(backendName)
		if backend == nil {
			continue
		}
		if _, err := backend.Backend.ListQuery(ctx, ackPrefix); err != nil {
			log.Printf("startup ack-cache warmup failed backend=%s prefix=%s: %v", backendName, ackPrefix, err)
		}
	}
}

func (e *Engine) remoteStaleFileTTL() time.Duration {
	ttl := e.opts.StaleUnackedFileTTL
	if ttl < e.opts.SessionIdleTimeout {
		ttl = e.opts.SessionIdleTimeout
	}
	if ttl < 10*time.Minute {
		ttl = 10 * time.Minute
	}
	return ttl
}

func (e *Engine) cleanupRemoteStaleFiles(ctx context.Context) {
	ttl := e.remoteStaleFileTTL()
	now := time.Now()

	e.remoteCleanupMu.Lock()
	if !e.lastRemoteCleanup.IsZero() && now.Sub(e.lastRemoteCleanup) < ttl {
		e.remoteCleanupMu.Unlock()
		return
	}
	e.lastRemoteCleanup = now
	e.remoteCleanupMu.Unlock()

	prefix := string(e.myDir) + "-"
	if e.myDir == DirReq {
		prefix += safeID(e.id) + "-"
	}

	for _, backend := range e.pool.Backends() {
		files, err := backend.Backend.ListQuery(ctx, prefix)
		if err != nil {
			log.Printf("remote stale cleanup list error backend=%s prefix=%s: %v", backend.Name, prefix, err)
			continue
		}
		for _, name := range files {
			_, _, ts, ok := parseMuxFilename(name)
			if !ok {
				continue
			}
			if now.Sub(time.Unix(0, ts)) <= ttl {
				continue
			}
			if err := backend.Backend.Delete(ctx, name); err != nil {
				log.Printf("remote stale cleanup delete error backend=%s file=%s: %v", backend.Name, name, err)
				continue
			}
			log.Printf("startup/runtime stale cleanup backend=%s deleted=%s", backend.Name, name)
		}
	}
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

func (e *Engine) forceFlushDebounceInterval() time.Duration {
	debounce := e.opts.FlushRate / 4
	if debounce < 15*time.Millisecond {
		debounce = 15 * time.Millisecond
	}
	if debounce > 75*time.Millisecond {
		debounce = 75 * time.Millisecond
	}
	return debounce
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
	e.signalUpload()
}

func (e *Engine) signalUpload() {
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
	delete(e.deleting, key)
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

func (e *Engine) bufferPreOpenSegment(backendName, clientID, sessionID, remoteName string, envs []*Envelope, needsAck bool) bool {
	if len(envs) == 0 {
		return true
	}
	key := e.sessionScopedKey(backendName, clientID, sessionID)

	e.preOpenMu.Lock()
	defer e.preOpenMu.Unlock()

	bucket := e.preOpen[key]
	if bucket == nil {
		bucket = make(map[uint64]*preOpenSegment)
		e.preOpen[key] = bucket
	}
	if _, exists := bucket[envs[0].Seq]; exists {
		return true
	}
	if len(bucket) >= e.opts.MaxOutOfOrderSegments {
		return false
	}

	copies := make([]*Envelope, 0, len(envs))
	for _, env := range envs {
		copyEnv := *env
		copyEnv.Payload = append([]byte(nil), env.Payload...)
		copies = append(copies, &copyEnv)
	}
	bucket[envs[0].Seq] = &preOpenSegment{
		remoteName: remoteName,
		envs:       copies,
		needsAck:   needsAck,
		bufferedAt: time.Now(),
	}
	return true
}

func (e *Engine) takePreOpenSegments(backendName, clientID, sessionID string) []*preOpenSegment {
	key := e.sessionScopedKey(backendName, clientID, sessionID)

	e.preOpenMu.Lock()
	bucket := e.preOpen[key]
	delete(e.preOpen, key)
	e.preOpenMu.Unlock()

	if len(bucket) == 0 {
		return nil
	}

	out := make([]*preOpenSegment, 0, len(bucket))
	for _, seg := range bucket {
		out = append(out, seg)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].envs[0].Seq < out[j].envs[0].Seq
	})
	return out
}

func (e *Engine) cleanupPreOpenSegments() {
	ttl := e.opts.StaleUnackedFileTTL
	if ttl <= 0 {
		return
	}
	cutoff := time.Now().Add(-ttl)

	e.preOpenMu.Lock()
	defer e.preOpenMu.Unlock()

	for key, bucket := range e.preOpen {
		for seq, seg := range bucket {
			if seg.bufferedAt.Before(cutoff) {
				delete(bucket, seq)
			}
		}
		if len(bucket) == 0 {
			delete(e.preOpen, key)
		}
	}
}

func (e *Engine) applyReceivedSegments(backendName, clientID, sessionID, remoteName string, session *Session, envs []*Envelope, needsAck, markSeen bool) bool {
	if len(envs) == 0 {
		if markSeen {
			e.markSeenRemote(backendName, remoteName)
		}
		return true
	}
	for _, env := range envs {
		if !e.applyReceivedSegment(backendName, clientID, sessionID, remoteName, session, env, needsAck, markSeen) {
			return false
		}
	}
	return true
}

func (e *Engine) applyReceivedSegment(backendName, clientID, sessionID, remoteName string, session *Session, env *Envelope, needsAck, markSeen bool) bool {
	ackedSeq, advanced, closedNow, err := session.ProcessRx(env)
	if err != nil {
		log.Printf("process segment failed session=%s seq=%d: %v", sessionID, env.Seq, err)
		return false
	}
	if markSeen {
		e.markSeenRemote(backendName, remoteName)
	}
	if !advanced {
		return true
	}

	if needsAck {
		e.noteAck(backendName, clientID, sessionID, ackedSeq)
	}
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
	log.Printf("segment downloaded backend=%s file=%s session=%s seq=%d", backendName, remoteName, sessionID, env.Seq)
	return true
}

func (e *Engine) isAcked(seg *spoolSegment) bool {
	key := e.remoteAckKey(seg.Meta.BackendName, seg.Meta.ClientID, seg.Meta.SessionID)
	e.ackMu.Lock()
	defer e.ackMu.Unlock()
	ackedSeq, ok := e.remoteAcked[key]
	endSeq := seg.Meta.EndSeq
	if endSeq < seg.Meta.Seq {
		endSeq = seg.Meta.Seq
	}
	return ok && endSeq <= ackedSeq
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

func (e *Engine) tryStartDelete(key string) bool {
	e.pendingMu.Lock()
	defer e.pendingMu.Unlock()
	if _, ok := e.deleting[key]; ok {
		return false
	}
	e.deleting[key] = struct{}{}
	return true
}

func (e *Engine) finishDelete(key string) {
	e.pendingMu.Lock()
	delete(e.deleting, key)
	e.pendingMu.Unlock()
}

func (e *Engine) markSeenRemote(backendName, name string) {
	e.ackMu.Lock()
	e.seenRemote[backendName+"|"+name] = time.Now()
	e.lastRx = time.Now()
	e.ackMu.Unlock()
}

func (e *Engine) markSeenRemoteForever(backendName, name string) {
	e.ackMu.Lock()
	e.seenRemote[backendName+"|"+name] = time.Now().Add(100 * 365 * 24 * time.Hour)
	e.ackMu.Unlock()
}

func (e *Engine) filterSeenRemote(backendName string, names []string) []string {
	if len(names) == 0 {
		return names
	}

	cutoff := e.opts.StaleUnackedFileTTL
	out := names[:0]

	e.ackMu.Lock()
	for _, name := range names {
		seenAt := e.seenRemote[backendName+"|"+name]
		if !seenAt.IsZero() && time.Since(seenAt) < cutoff {
			continue
		}
		out = append(out, name)
	}
	e.ackMu.Unlock()

	return out
}

func (e *Engine) shouldLogOrphan(seq uint64) bool {
	return seq == 0 || seq%32 == 0
}

func (e *Engine) shouldPollAcks(backendName string) bool {
	if !e.hasLegacyPendingSegments(backendName) {
		return false
	}

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

func (e *Engine) hasLegacyPendingSegments(backendName string) bool {
	e.pendingMu.RLock()
	defer e.pendingMu.RUnlock()

	for _, seg := range e.pending {
		if seg.Meta.BackendName != backendName {
			continue
		}
		if _, _, _, ok := parseMuxFilename(seg.Meta.RemoteName); ok {
			continue
		}
		return true
	}
	return false
}

func (e *Engine) shouldDropStartupOpen(createdUnixMs int64) bool {
	if e.startedAtUnixMs == 0 || createdUnixMs <= 0 {
		return false
	}
	const startupGrace = int64(15 * time.Second / time.Millisecond)
	return createdUnixMs+startupGrace < e.startedAtUnixMs
}

func (e *Engine) configureBackendListCutoff() map[string]struct{} {
	const startupGrace = int64(15 * time.Second / time.Millisecond)
	cutoff := e.startedAtUnixMs - startupGrace
	if cutoff < 0 {
		cutoff = 0
	}
	e.startupCutoffMs = cutoff

	type listCreatedAfterSetter interface {
		SetListCreatedAfter(unixMs int64)
	}

	configured := make(map[string]struct{})
	for _, backend := range e.pool.Backends() {
		setter, ok := backend.Backend.(listCreatedAfterSetter)
		if !ok {
			continue
		}
		setter.SetListCreatedAfter(cutoff)
		configured[backend.Name] = struct{}{}
		log.Printf("startup list cutoff backend=%s created_after_ms=%d", backend.Name, cutoff)
	}
	return configured
}

func (e *Engine) discardStartupBacklog(ctx context.Context, cutoffConfigured map[string]struct{}) {
	for _, backend := range e.pool.Backends() {
		if _, ok := cutoffConfigured[backend.Name]; ok {
			continue
		}

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

		sessions := make(map[string]struct{})
		segmentCount := 0
		for _, name := range files {
			dir, clientID, sessionID, _, _, ok := parseSegmentFilename(name)
			if !ok || dir != e.peerDir {
				continue
			}
			segmentCount++
			sessions[clientID+"|"+sessionID] = struct{}{}
			e.markSeenRemoteForever(backend.Name, name)
		}

		if len(sessions) == 0 {
			continue
		}

		for key := range sessions {
			_, sessionID, ok := strings.Cut(key, "|")
			if !ok {
				continue
			}
			e.markClosedSession(sessionID)
		}
		log.Printf("startup stale discard backend=%s mode=local-ignore sessions=%d segments=%d", backend.Name, len(sessions), segmentCount)
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

func dedupeStrings(values []string) []string {
	if len(values) <= 1 {
		return values
	}
	seen := make(map[string]struct{}, len(values))
	out := values[:0]
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
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
