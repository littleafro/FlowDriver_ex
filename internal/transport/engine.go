package transport

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/NullLatency/flow-driver/internal/storage"
)

type RuntimeTuning struct {
	PollRate                   time.Duration
	ActivePollRate             time.Duration
	IdlePollRate               time.Duration
	FlushRate                  time.Duration
	UploadInterval             time.Duration
	SegmentBytes               int
	MaxSegmentBytes            int
	MaxMuxSegments             int
	MaxConcurrentUploads       int
	MaxConcurrentDownloads     int
	MaxConcurrentDeletes       int
	MaxPendingChunksPerBackend int
	MaxTxBufferBytesPerSession int
	MaxOutOfOrderSegments      int
	Compression                string
	CompressionMinBytes        int
	GapGracePeriod             time.Duration
}

type BackendStats struct {
	PendingChunks       int
	PublishedChunks     uint64
	ProcessedChunks     uint64
	UploadErrors        uint64
	DownloadErrors      uint64
	DeleteErrors        uint64
	ManifestErrors      uint64
	LastPublishLatency  time.Duration
	LastDownloadLatency time.Duration
	ManifestLag         time.Duration
}

type StatsSnapshot struct {
	ActiveSessions  int
	BytesC2S        uint64
	BytesS2C        uint64
	UploadErrors    uint64
	DownloadErrors  uint64
	DeleteErrors    uint64
	ManifestErrors  uint64
	PendingChunks   int
	LastMetricsTime time.Time
	Backends        map[string]BackendStats
}

type metrics struct {
	mu             sync.Mutex
	activeSessions int
	bytesC2S       uint64
	bytesS2C       uint64
	uploadErrors   uint64
	downloadErrors uint64
	deleteErrors   uint64
	manifestErrors uint64
	pendingChunks  int
	lastMetricsLog time.Time
	backends       map[string]*BackendStats
}

type sessionCommit struct {
	session  *Session
	env      *Envelope
	consumed int
}

type backendBatch struct {
	key         string
	backendName string
	compression string
	minBytes    int
	markDirect  bool
	envs    []*Envelope
	commits []sessionCommit
}

type sessionSendPlan struct {
	segmentBytes    int
	maxSegmentBytes int
	maxMuxSegments  int
	compression     string
	compressionMin  int
	bootstrap       bool
	mirror          bool
	force           bool
}

type chunkRoute uint8

const (
	routeBootstrap chunkRoute = iota + 1
	routeDirect
)

type pendingChunk struct {
	id         string
	filename   string
	backend    string
	epoch      uint64
	createdAt  time.Time
	payload    []byte
	sizeBytes  int
	route      chunkRoute
	sessionID  string
	targetAddr string
}

type sessionHeadState struct {
	head            sessionHeadFile
	dirty           bool
	publishing      bool
	version         uint64
	lastPublishedAt time.Time
}

type backendState struct {
	name string

	mu                  sync.Mutex
	queue               []*pendingChunk
	localManifest       manifestFile
	manifestDirty       bool
	manifestPublishing  bool
	manifestVersion     uint64
	manifestPublishedAt time.Time
	remoteEpoch         uint64
	seenChunks          *seenChunkCache
	inFlight            map[string]struct{}
	sessionHeads        map[string]*sessionHeadState
}

const metadataPublishMinInterval = 500 * time.Millisecond

type Engine struct {
	pool         *storage.BackendPool
	myDir        Direction
	peerDir      Direction
	idMu         sync.RWMutex
	id           string
	optsMu       sync.RWMutex
	opts         Options
	epoch        uint64
	OnNewSession func(sessionID, targetAddr string, s *Session)

	sessionMu sync.RWMutex
	sessions  map[string]*Session

	backendMu sync.Mutex
	backends  map[string]*backendState
	dequeueAt int

	metrics metrics

	flushCh  chan bool
	uploadCh chan struct{}

	uploadLimiter   *dynamicLimiter
	downloadLimiter *dynamicLimiter
	deleteLimiter   *dynamicLimiter

	selectorMu      sync.RWMutex
	backendSelector func(*Session, []*storage.BackendHandle) (*storage.BackendHandle, error)
}

func NewEngine(backend storage.Backend, isClient bool, clientID string) *Engine {
	return NewEngineWithPool(storage.NewSingleBackendPool("default", backend), isClient, clientID, DefaultOptions())
}

func NewEngineWithPool(pool *storage.BackendPool, isClient bool, clientID string, opts Options) *Engine {
	opts.ApplyDefaults()
	e := &Engine{
		pool:            pool,
		id:              clientID,
		opts:            opts,
		epoch:           uint64(time.Now().UnixNano()),
		sessions:        make(map[string]*Session),
		backends:        make(map[string]*backendState),
		metrics:         metrics{backends: make(map[string]*BackendStats)},
		flushCh:         make(chan bool, 4),
		uploadCh:        make(chan struct{}, 1),
		uploadLimiter:   newDynamicLimiter(opts.MaxConcurrentUploads),
		downloadLimiter: newDynamicLimiter(opts.MaxConcurrentDownloads),
		deleteLimiter:   newDynamicLimiter(opts.MaxConcurrentDeletes),
	}
	if isClient {
		e.myDir = DirReq
		e.peerDir = DirRes
	} else {
		e.myDir = DirRes
		e.peerDir = DirReq
	}
	for _, handle := range pool.Backends() {
		e.backends[handle.Name] = &backendState{
			name:         handle.Name,
			seenChunks:   newSeenChunkCache(opts.SeenChunkCacheSize, opts.ChunkOrphanTTL*3),
			inFlight:     make(map[string]struct{}),
			sessionHeads: make(map[string]*sessionHeadState),
		}
		e.metrics.backends[handle.Name] = &BackendStats{}
	}
	return e
}

func (e *Engine) SetPollRate(ms int) {
	if ms <= 0 {
		return
	}
	e.optsMu.Lock()
	e.opts.PollRate = time.Duration(ms) * time.Millisecond
	e.opts.ActivePollRate = e.opts.PollRate
	e.optsMu.Unlock()
}

func (e *Engine) SetFlushRate(ms int) {
	if ms <= 0 {
		return
	}
	e.optsMu.Lock()
	e.opts.FlushRate = time.Duration(ms) * time.Millisecond
	e.optsMu.Unlock()
}

func (e *Engine) SetBackendSelector(selector func(*Session, []*storage.BackendHandle) (*storage.BackendHandle, error)) {
	e.selectorMu.Lock()
	e.backendSelector = selector
	e.selectorMu.Unlock()
}

func (e *Engine) ApplyRuntimeTuning(t RuntimeTuning) {
	e.optsMu.Lock()
	if t.PollRate > 0 {
		e.opts.PollRate = t.PollRate
	}
	if t.ActivePollRate > 0 {
		e.opts.ActivePollRate = t.ActivePollRate
	}
	if t.IdlePollRate > 0 {
		e.opts.IdlePollRate = t.IdlePollRate
	}
	if t.FlushRate > 0 {
		e.opts.FlushRate = t.FlushRate
	}
	if t.UploadInterval > 0 {
		e.opts.UploadInterval = t.UploadInterval
	}
	if t.SegmentBytes > 0 {
		e.opts.SegmentBytes = t.SegmentBytes
	}
	if t.MaxSegmentBytes > 0 {
		e.opts.MaxSegmentBytes = t.MaxSegmentBytes
	}
	if t.MaxMuxSegments > 0 {
		e.opts.MaxMuxSegments = t.MaxMuxSegments
	}
	if t.MaxPendingChunksPerBackend > 0 {
		e.opts.MaxPendingSegmentsPerSession = t.MaxPendingChunksPerBackend
	}
	if t.MaxTxBufferBytesPerSession > 0 {
		e.opts.MaxTxBufferBytesPerSession = t.MaxTxBufferBytesPerSession
	}
	if t.MaxOutOfOrderSegments > 0 {
		e.opts.MaxOutOfOrderSegments = t.MaxOutOfOrderSegments
	}
	if t.Compression != "" {
		e.opts.Compression = t.Compression
	}
	if t.CompressionMinBytes > 0 {
		e.opts.CompressionMinBytes = t.CompressionMinBytes
	}
	if t.GapGracePeriod > 0 {
		e.opts.GapGracePeriod = t.GapGracePeriod
	}
	opts := e.opts
	e.optsMu.Unlock()

	if t.MaxConcurrentUploads > 0 {
		e.uploadLimiter.SetLimit(t.MaxConcurrentUploads)
		opts.MaxConcurrentUploads = t.MaxConcurrentUploads
	}
	if t.MaxConcurrentDownloads > 0 {
		e.downloadLimiter.SetLimit(t.MaxConcurrentDownloads)
		opts.MaxConcurrentDownloads = t.MaxConcurrentDownloads
	}
	if t.MaxConcurrentDeletes > 0 {
		e.deleteLimiter.SetLimit(t.MaxConcurrentDeletes)
		opts.MaxConcurrentDeletes = t.MaxConcurrentDeletes
	}

	e.sessionMu.RLock()
	sessions := make([]*Session, 0, len(e.sessions))
	for _, s := range e.sessions {
		sessions = append(sessions, s)
	}
	e.sessionMu.RUnlock()
	for _, s := range sessions {
		s.Configure(opts.MaxTxBufferBytesPerSession, opts.MaxOutOfOrderSegments, opts.SegmentBytes, e.notifyFlush, e.notifyForceFlush)
	}
}

func (e *Engine) StatsSnapshot() StatsSnapshot {
	e.metrics.mu.Lock()
	defer e.metrics.mu.Unlock()

	backends := make(map[string]BackendStats, len(e.metrics.backends))
	for name, stats := range e.metrics.backends {
		backends[name] = *stats
	}
	return StatsSnapshot{
		ActiveSessions:  e.metrics.activeSessions,
		BytesC2S:        e.metrics.bytesC2S,
		BytesS2C:        e.metrics.bytesS2C,
		UploadErrors:    e.metrics.uploadErrors,
		DownloadErrors:  e.metrics.downloadErrors,
		DeleteErrors:    e.metrics.deleteErrors,
		ManifestErrors:  e.metrics.manifestErrors,
		PendingChunks:   e.metrics.pendingChunks,
		LastMetricsTime: e.metrics.lastMetricsLog,
		Backends:        backends,
	}
}

func (e *Engine) primeBackends(ctx context.Context) {
	clientID := e.clientID()
	if clientID == "" {
		return
	}
	for _, handle := range e.pool.Backends() {
		state := e.backendState(handle.Name)
		state.mu.Lock()
		state.localManifest = manifestFile{
			Dir:      e.myDir,
			ClientID: clientID,
			Epoch:    e.epoch,
			Chunks:   nil,
		}
		state.manifestDirty = true
		state.manifestVersion++
		state.mu.Unlock()
		if err := e.publishManifest(ctx, handle); err != nil {
			log.Printf("manifest prime error backend=%s: %v", handle.Name, err)
		}
	}
}

func (e *Engine) Start(ctx context.Context) {
	e.primeBackends(ctx)
	go e.flushLoop(ctx)
	go e.uploadLoop(ctx)
	go e.pollLoop(ctx)
	go e.maintenanceLoop(ctx)
	go e.orphanSweepLoop(ctx)
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
	e.flushAll(true)
	e.doUpload(ctx)
	e.publishDirtyManifests(ctx)
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
	opts := e.currentOptions()
	s.Configure(opts.MaxTxBufferBytesPerSession, opts.MaxOutOfOrderSegments, opts.SegmentBytes, e.notifyFlush, e.notifyForceFlush)
	if s.ClientID == "" {
		s.ClientID = e.clientID()
	}
	if s.BackendName == "" {
		if backend, err := e.selectBackendForSession(s); err == nil {
			s.BackendName = backend.Name
		} else {
			log.Printf("backend selection failed session=%s: %v", s.ID, err)
		}
	}
	if e.myDir == DirReq && s.BackendEpoch == 0 {
		s.SetBootstrapPending(true)
	}

	e.sessionMu.Lock()
	e.sessions[s.ID] = s
	count := len(e.sessions)
	e.sessionMu.Unlock()

	e.metrics.mu.Lock()
	e.metrics.activeSessions = count
	e.metrics.mu.Unlock()
	log.Printf("session open %s client=%s backend=%s epoch=%d", s.ID, s.ClientID, s.BackendName, s.BackendEpoch)
}

func (e *Engine) RemoveSession(id string) {
	e.sessionMu.Lock()
	delete(e.sessions, id)
	count := len(e.sessions)
	e.sessionMu.Unlock()

	e.metrics.mu.Lock()
	e.metrics.activeSessions = count
	e.metrics.mu.Unlock()
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
	timer := time.NewTimer(e.currentOptions().FlushRate)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case force := <-e.flushCh:
			e.flushAll(force)
		case <-timer.C:
			e.flushAll(false)
		}
		timer.Reset(e.currentOptions().FlushRate)
	}
}

func (e *Engine) flushAll(force bool) {
	opts := e.currentOptions()

	e.sessionMu.RLock()
	sessions := make([]*Session, 0, len(e.sessions))
	for _, s := range e.sessions {
		sessions = append(sessions, s)
	}
	e.sessionMu.RUnlock()

	if len(sessions) == 0 && !force {
		return
	}

	now := time.Now()
	manifestBatches := make(map[string]*backendBatch)
	for _, s := range sessions {
		if now.Sub(s.LastActivity()) >= opts.SessionIdleTimeout && !s.HasCloseQueued() {
			log.Printf("session %s idle for %s, queuing close", s.ID, now.Sub(s.LastActivity()))
			s.QueueClose()
		}
		if s.BackendName == "" {
			backend, err := e.selectBackendForSession(s)
			if err != nil {
				log.Printf("backend selection failed session=%s: %v", s.ID, err)
				continue
			}
			s.BackendName = backend.Name
		}
		plan := e.sessionSendPlanFor(s, opts, force)
		allowHeartbeat := opts.HeartbeatInterval > 0 && s.CanHeartbeat(now, opts.HeartbeatInterval)
		envs, err := s.PrepareEnvelopeBatch(now, plan.segmentBytes, plan.maxSegmentBytes, plan.maxMuxSegments, plan.force, allowHeartbeat)
		if err != nil {
			log.Printf("prepare envelope batch failed session=%s: %v", s.ID, err)
			continue
		}
		if len(envs) == 0 {
			continue
		}
		chunkEnvs := make([]*Envelope, 0, len(envs))
		for i := range envs {
			chunkEnvs = append(chunkEnvs, envs[i].Env)
		}
		recipients := e.chunkRecipientsForSession(s, plan.mirror)
		manifestRoute := plan.bootstrap || e.shouldUseManifestRoute(s)
		if manifestRoute && !plan.mirror {
			if len(recipients) == 0 {
				continue
			}
			handle := recipients[0]
			batchKey := e.manifestBatchKey(handle.Name, plan.compression, plan.compressionMin, !plan.bootstrap)
			batch := manifestBatches[batchKey]
			if batch == nil {
				batch = &backendBatch{
					key:         batchKey,
					backendName: handle.Name,
					compression: plan.compression,
					minBytes:    plan.compressionMin,
					markDirect:  !plan.bootstrap,
				}
				manifestBatches[batchKey] = batch
			}
			for i := range envs {
				batch.envs = append(batch.envs, envs[i].Env)
				batch.commits = append(batch.commits, sessionCommit{
					session:  s,
					env:      envs[i].Env,
					consumed: envs[i].Consumed,
				})
			}
			continue
		}
		payload, _, err := marshalChunkPayloads(chunkEnvs, plan.compression, plan.compressionMin)
		if err != nil {
			log.Printf("marshal chunk payload failed session=%s: %v", s.ID, err)
			continue
		}
		enqueued := 0
		for _, handle := range recipients {
			if e.enqueueChunk(handle.Name, payload, now, plan.bootstrap, s.ID, s.TargetAddr) {
				enqueued++
			}
		}
		if enqueued == 0 {
			if len(recipients) > 0 {
				log.Printf("chunk queue full session=%s backend=%s queued=%d", s.ID, recipients[0].Name, e.pendingChunkDepth(recipients[0].Name))
			}
			continue
		}
		for i := range envs {
			if err := s.CommitEnvelope(envs[i].Env, envs[i].Consumed); err != nil {
				log.Printf("commit envelope failed session=%s seq=%d: %v", s.ID, envs[i].Env.Seq, err)
			}
		}
		if plan.bootstrap {
			s.MarkBootstrapped()
		} else {
			s.RecordDirectChunkPublished()
		}
	}
	for _, batch := range manifestBatches {
		if len(batch.envs) == 0 {
			continue
		}
		payload, _, err := marshalChunkPayloads(batch.envs, batch.compression, batch.minBytes)
		if err != nil {
			log.Printf("marshal manifest batch failed backend=%s envs=%d: %v", batch.backendName, len(batch.envs), err)
			continue
		}
		if !e.enqueueChunk(batch.backendName, payload, now, true, "", "") {
			log.Printf("chunk queue full backend=%s envs=%d queued=%d", batch.backendName, len(batch.envs), e.pendingChunkDepth(batch.backendName))
			continue
		}
		for _, commit := range batch.commits {
			if err := commit.session.CommitEnvelope(commit.env, commit.consumed); err != nil {
				log.Printf("commit bootstrap batch failed session=%s seq=%d: %v", commit.session.ID, commit.env.Seq, err)
				continue
			}
			if batch.markDirect {
				commit.session.RecordDirectChunkPublished()
			} else {
				commit.session.MarkBootstrapped()
			}
		}
	}
	e.triggerUpload()
}

func (e *Engine) shouldUseManifestRoute(s *Session) bool {
	if s == nil {
		return false
	}
	if e.myDir != DirRes {
		return false
	}
	if s.DirectChunksPublished() >= 2 {
		return false
	}
	return e.activeSessionCount() > 4 || e.totalPendingChunkDepth() > 4
}

func (e *Engine) manifestBatchKey(backendName, compression string, minBytes int, markDirect bool) string {
	return fmt.Sprintf("%s|%s|%d|%t", backendName, compression, minBytes, markDirect)
}

func (e *Engine) enqueueChunk(backendName string, payload []byte, createdAt time.Time, bootstrap bool, sessionID, targetAddr string) bool {
	clientID := e.clientID()
	if clientID == "" {
		return false
	}
	state := e.backendState(backendName)
	opts := e.currentOptions()

	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.queue) >= opts.MaxPendingSegmentsPerSession {
		return false
	}
	route := routeDirect
	filename := ""
	chunkID := newChunkID(createdAt)
	if bootstrap {
		route = routeBootstrap
		filename = chunkFilename(e.myDir, clientID, e.epoch, chunkID)
	}
	if route == routeBootstrap && state.localManifest.ClientID == "" {
		state.localManifest = manifestFile{
			Dir:      e.myDir,
			ClientID: clientID,
			Epoch:    e.epoch,
		}
	}
	if route == routeDirect {
		filename = sessionChunkFilename(e.myDir, sessionID, e.epoch, chunkID)
	}
	state.queue = append(state.queue, &pendingChunk{
		id:         chunkID,
		filename:   filename,
		backend:    backendName,
		epoch:      e.epoch,
		createdAt:  createdAt,
		payload:    payload,
		sizeBytes:  len(payload),
		route:      route,
		sessionID:  sessionID,
		targetAddr: targetAddr,
	})
	e.updatePendingChunkMetrics(backendName, len(state.queue))
	return true
}

func (e *Engine) pendingChunkDepth(backendName string) int {
	state := e.backendState(backendName)
	state.mu.Lock()
	defer state.mu.Unlock()
	return len(state.queue)
}

func (e *Engine) sessionSendPlanFor(s *Session, opts Options, force bool) sessionSendPlan {
	interactive := s.TxCommittedBytes() < 128*1024 && s.DirectChunksPublished() < 4
	plan := sessionSendPlan{
		segmentBytes:    opts.SegmentBytes,
		maxSegmentBytes: opts.MaxSegmentBytes,
		maxMuxSegments:  opts.MaxMuxSegments,
		compression:     opts.Compression,
		compressionMin:  opts.CompressionMinBytes,
		bootstrap:       s.BootstrapPending(),
		force:           force,
	}
	if interactive {
		if plan.segmentBytes > 16*1024 {
			plan.segmentBytes = 16 * 1024
		}
		if plan.maxSegmentBytes > 32*1024 {
			plan.maxSegmentBytes = 32 * 1024
		}
		if plan.maxMuxSegments > 4 {
			plan.maxMuxSegments = 4
		}
		plan.compression = "off"
		plan.force = true
	}
	if plan.bootstrap || s.DirectChunksPublished() < 1 {
		plan.mirror = len(e.pool.HealthyBackends()) > 1 &&
			e.activeSessionCount() <= 2 &&
			e.totalPendingChunkDepth() < 8
	}
	return plan
}

func (e *Engine) activeSessionCount() int {
	e.sessionMu.RLock()
	defer e.sessionMu.RUnlock()
	return len(e.sessions)
}

func (e *Engine) totalPendingChunkDepth() int {
	e.backendMu.Lock()
	states := make([]*backendState, 0, len(e.backends))
	for _, state := range e.backends {
		states = append(states, state)
	}
	e.backendMu.Unlock()

	total := 0
	for _, state := range states {
		state.mu.Lock()
		total += len(state.queue)
		state.mu.Unlock()
	}
	return total
}

func (e *Engine) chunkRecipientsForSession(s *Session, mirror bool) []*storage.BackendHandle {
	if mirror {
		backends := e.pool.HealthyBackends()
		if len(backends) > 0 {
			return backends
		}
	}
	if s.BackendName != "" {
		for _, handle := range e.pool.Backends() {
			if handle.Name == s.BackendName {
				return []*storage.BackendHandle{handle}
			}
		}
	}
	backends := e.pool.HealthyBackends()
	if len(backends) > 0 {
		return backends[:1]
	}
	if len(e.pool.Backends()) > 0 {
		return e.pool.Backends()[:1]
	}
	return nil
}

func (e *Engine) triggerUpload() {
	select {
	case e.uploadCh <- struct{}{}:
	default:
	}
}

func (e *Engine) uploadLoop(ctx context.Context) {
	timer := time.NewTimer(e.currentOptions().UploadInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.uploadCh:
			e.doUpload(ctx)
		case <-timer.C:
			e.doUpload(ctx)
		}
		timer.Reset(e.currentOptions().UploadInterval)
	}
}

func (e *Engine) doUpload(ctx context.Context) {
	for {
		if !e.uploadLimiter.TryAcquire() {
			break
		}
		handle, chunk := e.dequeuePendingChunk()
		if handle == nil || chunk == nil {
			e.uploadLimiter.Release()
			break
		}
		go e.uploadChunk(ctx, handle, chunk)
	}
	e.publishDirtyManifests(ctx)
}

func (e *Engine) dequeuePendingChunk() (*storage.BackendHandle, *pendingChunk) {
	backends := e.pool.Backends()
	if len(backends) == 0 {
		return nil, nil
	}

	e.backendMu.Lock()
	start := 0
	if len(backends) > 0 {
		start = e.dequeueAt % len(backends)
	}
	e.backendMu.Unlock()

	for offset := 0; offset < len(backends); offset++ {
		handle := backends[(start+offset)%len(backends)]
		state := e.backendState(handle.Name)
		state.mu.Lock()
		if len(state.queue) == 0 {
			state.mu.Unlock()
			continue
		}
		chunk := state.queue[0]
		state.queue = state.queue[1:]
		depth := len(state.queue)
		state.mu.Unlock()
		e.backendMu.Lock()
		e.dequeueAt = (start + offset + 1) % len(backends)
		e.backendMu.Unlock()
		e.updatePendingChunkMetrics(handle.Name, depth)
		return handle, chunk
	}
	return nil, nil
}

func (e *Engine) uploadChunk(ctx context.Context, handle *storage.BackendHandle, chunk *pendingChunk) {
	defer e.uploadLimiter.Release()

	started := time.Now()
	if err := handle.Backend.Upload(ctx, chunk.filename, bytes.NewReader(chunk.payload)); err != nil {
		log.Printf("chunk upload error backend=%s chunk=%s: %v", handle.Name, chunk.filename, err)
		e.metrics.mu.Lock()
		e.metrics.uploadErrors++
		e.metrics.backends[handle.Name].UploadErrors++
		e.metrics.mu.Unlock()
		return
	}
	latency := time.Since(started)

	state := e.backendState(handle.Name)
	state.mu.Lock()
	if chunk.route == routeBootstrap && state.localManifest.ClientID == "" {
		state.localManifest = manifestFile{
			Dir:      e.myDir,
			ClientID: e.clientID(),
			Epoch:    e.epoch,
		}
	}
	if chunk.route == routeBootstrap {
		state.localManifest.Chunks = append(state.localManifest.Chunks, manifestChunk{
			ID:            chunk.id,
			CreatedUnixMs: chunk.createdAt.UnixMilli(),
			SizeBytes:     chunk.sizeBytes,
		})
		if len(state.localManifest.Chunks) > e.currentOptions().ManifestChunkWindow {
			state.localManifest.Chunks = append([]manifestChunk(nil), state.localManifest.Chunks[len(state.localManifest.Chunks)-e.currentOptions().ManifestChunkWindow:]...)
		}
		state.manifestDirty = true
		state.manifestVersion++
	} else {
		canonicalBackend := chunk.backend
		if session := e.GetSession(chunk.sessionID); session != nil && session.BackendName != "" {
			canonicalBackend = session.BackendName
		}
		headState := state.sessionHeads[chunk.sessionID]
		if headState == nil {
			headState = &sessionHeadState{
				head: sessionHeadFile{
					Dir:         e.myDir,
					ClientID:    e.clientID(),
					SessionID:   chunk.sessionID,
					BackendName: canonicalBackend,
					TargetAddr:  chunk.targetAddr,
					Epoch:       e.epoch,
				},
			}
			state.sessionHeads[chunk.sessionID] = headState
		}
		headState.head.BackendName = canonicalBackend
		headState.head.Chunks = append(headState.head.Chunks, manifestChunk{
			ID:            chunk.id,
			CreatedUnixMs: chunk.createdAt.UnixMilli(),
			SizeBytes:     chunk.sizeBytes,
		})
		if len(headState.head.Chunks) > e.currentOptions().ManifestChunkWindow {
			headState.head.Chunks = append([]manifestChunk(nil), headState.head.Chunks[len(headState.head.Chunks)-e.currentOptions().ManifestChunkWindow:]...)
		}
		headState.dirty = true
		headState.version++
	}
	state.mu.Unlock()

	e.metrics.mu.Lock()
	e.metrics.backends[handle.Name].PublishedChunks++
	e.metrics.backends[handle.Name].LastPublishLatency = latency
	e.metrics.mu.Unlock()

	if chunk.route == routeBootstrap {
		if err := e.publishManifest(ctx, handle); err != nil {
			log.Printf("manifest publish error backend=%s: %v", handle.Name, err)
		}
	} else if err := e.publishSessionHead(ctx, handle, chunk.sessionID); err != nil {
		log.Printf("session head publish error backend=%s session=%s: %v", handle.Name, chunk.sessionID, err)
	}
}

func (e *Engine) publishDirtyManifests(ctx context.Context) {
	for _, handle := range e.pool.Backends() {
		if err := e.publishManifest(ctx, handle); err != nil {
			log.Printf("manifest publish retry error backend=%s: %v", handle.Name, err)
		}
		e.publishDirtySessionHeads(ctx, handle)
	}
}

func (e *Engine) publishManifest(ctx context.Context, handle *storage.BackendHandle) error {
	state := e.backendState(handle.Name)
	state.mu.Lock()
	if !state.manifestDirty || state.localManifest.ClientID == "" {
		state.mu.Unlock()
		return nil
	}
	if state.manifestPublishing {
		state.mu.Unlock()
		return nil
	}
	if !state.manifestPublishedAt.IsZero() && time.Since(state.manifestPublishedAt) < metadataPublishMinInterval {
		state.mu.Unlock()
		return nil
	}
	state.manifestPublishing = true
	state.localManifest.UpdatedUnixMs = time.Now().UnixMilli()
	version := state.manifestVersion
	payload, err := encodeManifest(state.localManifest)
	filename := manifestFilename(e.myDir, state.localManifest.ClientID)
	state.mu.Unlock()
	if err != nil {
		state.mu.Lock()
		state.manifestPublishing = false
		state.mu.Unlock()
		return err
	}

	if err := handle.Backend.Put(ctx, filename, bytes.NewReader(payload)); err != nil {
		state.mu.Lock()
		state.manifestPublishing = false
		state.mu.Unlock()
		e.metrics.mu.Lock()
		e.metrics.manifestErrors++
		e.metrics.backends[handle.Name].ManifestErrors++
		e.metrics.mu.Unlock()
		return err
	}

	state.mu.Lock()
	state.manifestPublishing = false
	if state.manifestVersion == version {
		state.manifestDirty = false
		state.manifestPublishedAt = time.Now()
	}
	state.mu.Unlock()
	return nil
}

func (e *Engine) publishSessionHead(ctx context.Context, handle *storage.BackendHandle, sessionID string) error {
	state := e.backendState(handle.Name)
	state.mu.Lock()
	headState := state.sessionHeads[sessionID]
	if headState == nil || !headState.dirty {
		state.mu.Unlock()
		return nil
	}
	if headState.publishing {
		state.mu.Unlock()
		return nil
	}
	if !headState.lastPublishedAt.IsZero() && time.Since(headState.lastPublishedAt) < metadataPublishMinInterval {
		state.mu.Unlock()
		return nil
	}
	headState.publishing = true
	version := headState.version
	payload, err := encodeSessionHead(headState.head)
	filename := sessionHeadFilename(e.myDir, sessionID)
	state.mu.Unlock()
	if err != nil {
		state.mu.Lock()
		if current := state.sessionHeads[sessionID]; current == headState {
			current.publishing = false
		}
		state.mu.Unlock()
		return err
	}
	if err := handle.Backend.Put(ctx, filename, bytes.NewReader(payload)); err != nil {
		state.mu.Lock()
		if current := state.sessionHeads[sessionID]; current == headState {
			current.publishing = false
		}
		state.mu.Unlock()
		e.metrics.mu.Lock()
		e.metrics.manifestErrors++
		e.metrics.backends[handle.Name].ManifestErrors++
		e.metrics.mu.Unlock()
		return err
	}
	state.mu.Lock()
	if current := state.sessionHeads[sessionID]; current == headState {
		current.publishing = false
		if current.version == version {
			current.dirty = false
			current.lastPublishedAt = time.Now()
		}
	}
	state.mu.Unlock()
	return nil
}

func (e *Engine) publishDirtySessionHeads(ctx context.Context, handle *storage.BackendHandle) {
	state := e.backendState(handle.Name)
	state.mu.Lock()
	sessionIDs := make([]string, 0, len(state.sessionHeads))
	for sessionID, headState := range state.sessionHeads {
		if headState != nil && headState.dirty {
			sessionIDs = append(sessionIDs, sessionID)
		}
	}
	state.mu.Unlock()
	for _, sessionID := range sessionIDs {
		if err := e.publishSessionHead(ctx, handle, sessionID); err != nil {
			log.Printf("session head publish retry error backend=%s session=%s: %v", handle.Name, sessionID, err)
		}
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
			e.doPoll(ctx)
			timer.Reset(e.currentPollInterval())
		}
	}
}

func (e *Engine) doPoll(ctx context.Context) {
	e.doDirectPoll(ctx)
	if !e.ensureManifestContext(ctx) {
		return
	}
	clientID := e.clientID()
	if clientID == "" {
		return
	}

	for _, handle := range e.pool.Backends() {
		manifest, err := e.downloadManifest(ctx, handle, clientID)
		if err != nil {
			if !storage.IsNotFoundError(err) {
				log.Printf("manifest download error backend=%s: %v", handle.Name, err)
				e.metrics.mu.Lock()
				e.metrics.downloadErrors++
				e.metrics.backends[handle.Name].DownloadErrors++
				e.metrics.mu.Unlock()
			}
			continue
		}
		if manifest == nil {
			continue
		}
		e.applyRemoteManifest(handle.Name, manifest)
		e.scheduleManifestChunks(ctx, handle, manifest, clientID)
	}
}

func (e *Engine) doDirectPoll(ctx context.Context) {
	e.sessionMu.RLock()
	sessions := make([]*Session, 0, len(e.sessions))
	for _, s := range e.sessions {
		sessions = append(sessions, s)
	}
	e.sessionMu.RUnlock()
	if len(sessions) == 0 {
		return
	}

	now := time.Now()
	backends := e.pool.Backends()
	for _, session := range sessions {
		if !session.ShouldPollHead(now) {
			continue
		}
		if session.BackendName != "" {
			for _, handle := range backends {
				if handle.Name == session.BackendName {
					e.pollSessionHead(ctx, handle, session)
					break
				}
			}
			continue
		}
		if len(backends) > 0 {
			e.pollSessionHead(ctx, backends[0], session)
		}
	}
}

func (e *Engine) pollSessionHead(ctx context.Context, handle *storage.BackendHandle, session *Session) {
	if !e.downloadLimiter.TryAcquire() {
		return
	}
	session.MarkHeadPollStarted()
	go e.downloadSessionHead(ctx, handle, session)
}

func (e *Engine) downloadSessionHead(ctx context.Context, handle *storage.BackendHandle, session *Session) {
	defer e.downloadLimiter.Release()

	filename := sessionHeadFilename(e.peerDir, session.ID)
	started := time.Now()
	rc, err := handle.Backend.Download(ctx, filename)
	if err != nil {
		session.MarkHeadPollFinished(time.Now(), false, false)
		if !storage.IsNotFoundError(err) {
			log.Printf("session head download error backend=%s session=%s: %v", handle.Name, session.ID, err)
			e.metrics.mu.Lock()
			e.metrics.downloadErrors++
			e.metrics.backends[handle.Name].DownloadErrors++
			e.metrics.mu.Unlock()
		}
		return
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		session.MarkHeadPollFinished(time.Now(), false, false)
		log.Printf("session head read error backend=%s session=%s: %v", handle.Name, session.ID, err)
		e.metrics.mu.Lock()
		e.metrics.downloadErrors++
		e.metrics.backends[handle.Name].DownloadErrors++
		e.metrics.mu.Unlock()
		return
	}
	head, err := decodeSessionHead(data)
	if err != nil {
		session.MarkHeadPollFinished(time.Now(), false, false)
		log.Printf("session head decode error backend=%s session=%s: %v", handle.Name, session.ID, err)
		e.metrics.mu.Lock()
		e.metrics.downloadErrors++
		e.metrics.backends[handle.Name].DownloadErrors++
		e.metrics.mu.Unlock()
		return
	}
	if head.BackendName != "" && session.BackendName != head.BackendName {
		session.SetBackendName(head.BackendName)
	}
	polledAt := time.Now()
	hadChunks := len(head.Chunks) > 0
	session.MarkHeadPollFinished(polledAt, true, hadChunks)
	for _, desc := range head.Chunks {
		cacheKey := session.ID + ":" + desc.ID
		state := e.backendState(handle.Name)
		if state.seenChunks.Seen(cacheKey) {
			continue
		}
		state.mu.Lock()
		inFlightKey := "direct:" + cacheKey
		if _, exists := state.inFlight[inFlightKey]; exists {
			state.mu.Unlock()
			continue
		}
		if !e.downloadLimiter.TryAcquire() {
			state.mu.Unlock()
			return
		}
		state.inFlight[inFlightKey] = struct{}{}
		state.mu.Unlock()
		go e.downloadDirectChunk(ctx, handle, head, desc, polledAt, inFlightKey)
	}
	e.metrics.mu.Lock()
	e.metrics.backends[handle.Name].LastDownloadLatency = time.Since(started)
	e.metrics.mu.Unlock()
}

func (e *Engine) downloadManifest(ctx context.Context, handle *storage.BackendHandle, clientID string) (*manifestFile, error) {
	filename := manifestFilename(e.peerDir, clientID)
	rc, err := handle.Backend.Download(ctx, filename)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return decodeManifest(data)
}

func (e *Engine) applyRemoteManifest(backendName string, manifest *manifestFile) {
	state := e.backendState(backendName)
	state.mu.Lock()
	oldEpoch := state.remoteEpoch
	if oldEpoch != 0 && manifest.Epoch != oldEpoch {
		state.seenChunks.Reset()
		state.inFlight = make(map[string]struct{})
	}
	state.remoteEpoch = manifest.Epoch
	state.mu.Unlock()

	if oldEpoch != 0 && manifest.Epoch != oldEpoch {
		e.abortBackendEpochSessions(backendName, oldEpoch)
	}
}

func (e *Engine) scheduleManifestChunks(ctx context.Context, handle *storage.BackendHandle, manifest *manifestFile, clientID string) {
	state := e.backendState(handle.Name)
	now := time.Now()
	for _, desc := range manifest.Chunks {
		if state.seenChunks.Seen(desc.ID) {
			continue
		}
		state.mu.Lock()
		if _, exists := state.inFlight[desc.ID]; exists {
			state.mu.Unlock()
			continue
		}
		if !e.downloadLimiter.TryAcquire() {
			state.mu.Unlock()
			return
		}
		state.inFlight[desc.ID] = struct{}{}
		state.mu.Unlock()
		go e.downloadChunk(ctx, handle, manifest.Epoch, clientID, desc, now)
	}
}

func (e *Engine) downloadChunk(ctx context.Context, handle *storage.BackendHandle, epoch uint64, clientID string, desc manifestChunk, polledAt time.Time) {
	defer e.downloadLimiter.Release()

	state := e.backendState(handle.Name)
	defer func() {
		state.mu.Lock()
		delete(state.inFlight, desc.ID)
		state.mu.Unlock()
	}()

	filename := chunkFilename(e.peerDir, clientID, epoch, desc.ID)
	started := time.Now()
	rc, err := handle.Backend.Download(ctx, filename)
	if err != nil {
		if !storage.IsNotFoundError(err) {
			log.Printf("chunk download error backend=%s chunk=%s: %v", handle.Name, filename, err)
			e.metrics.mu.Lock()
			e.metrics.downloadErrors++
			e.metrics.backends[handle.Name].DownloadErrors++
			e.metrics.mu.Unlock()
		}
		return
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		log.Printf("chunk read error backend=%s chunk=%s: %v", handle.Name, filename, err)
		e.metrics.mu.Lock()
		e.metrics.downloadErrors++
		e.metrics.backends[handle.Name].DownloadErrors++
		e.metrics.mu.Unlock()
		return
	}

	envs, err := unmarshalChunkPayloads(data)
	if err != nil {
		log.Printf("chunk decode error backend=%s chunk=%s: %v", handle.Name, filename, err)
		e.metrics.mu.Lock()
		e.metrics.downloadErrors++
		e.metrics.backends[handle.Name].DownloadErrors++
		e.metrics.mu.Unlock()
		return
	}

	for _, env := range envs {
		if err := e.processEnvelope(env, handle.Name, epoch); err != nil {
			log.Printf("process envelope failed backend=%s chunk=%s session=%s seq=%d: %v", handle.Name, filename, env.SessionID, env.Seq, err)
		}
	}

	latency := time.Since(started)
	manifestLag := time.Duration(0)
	if desc.CreatedUnixMs > 0 {
		manifestLag = polledAt.Sub(time.UnixMilli(desc.CreatedUnixMs))
		if manifestLag < 0 {
			manifestLag = 0
		}
	}
	state.seenChunks.Mark(desc.ID)
	e.metrics.mu.Lock()
	e.metrics.backends[handle.Name].ProcessedChunks++
	e.metrics.backends[handle.Name].LastDownloadLatency = latency
	e.metrics.backends[handle.Name].ManifestLag = manifestLag
	e.metrics.mu.Unlock()

	e.deleteChunkAsync(ctx, handle, filename)
}

func (e *Engine) downloadDirectChunk(ctx context.Context, handle *storage.BackendHandle, head *sessionHeadFile, desc manifestChunk, polledAt time.Time, inFlightKey string) {
	defer e.downloadLimiter.Release()

	state := e.backendState(handle.Name)
	defer func() {
		state.mu.Lock()
		delete(state.inFlight, inFlightKey)
		state.mu.Unlock()
	}()

	filename := sessionChunkFilename(e.peerDir, head.SessionID, head.Epoch, desc.ID)
	started := time.Now()
	rc, err := handle.Backend.Download(ctx, filename)
	if err != nil {
		if !storage.IsNotFoundError(err) {
			log.Printf("direct chunk download error backend=%s chunk=%s: %v", handle.Name, filename, err)
			e.metrics.mu.Lock()
			e.metrics.downloadErrors++
			e.metrics.backends[handle.Name].DownloadErrors++
			e.metrics.mu.Unlock()
		}
		return
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		log.Printf("direct chunk read error backend=%s chunk=%s: %v", handle.Name, filename, err)
		e.metrics.mu.Lock()
		e.metrics.downloadErrors++
		e.metrics.backends[handle.Name].DownloadErrors++
		e.metrics.mu.Unlock()
		return
	}

	envs, err := unmarshalChunkPayloads(data)
	if err != nil {
		log.Printf("direct chunk decode error backend=%s chunk=%s: %v", handle.Name, filename, err)
		e.metrics.mu.Lock()
		e.metrics.downloadErrors++
		e.metrics.backends[handle.Name].DownloadErrors++
		e.metrics.mu.Unlock()
		return
	}

	for _, env := range envs {
		if err := e.processEnvelope(env, handle.Name, head.Epoch); err != nil {
			log.Printf("process direct envelope failed backend=%s chunk=%s session=%s seq=%d: %v", handle.Name, filename, env.SessionID, env.Seq, err)
		}
	}

	cacheKey := head.SessionID + ":" + desc.ID
	state.seenChunks.Mark(cacheKey)
	latency := time.Since(started)
	manifestLag := time.Duration(0)
	if desc.CreatedUnixMs > 0 {
		manifestLag = polledAt.Sub(time.UnixMilli(desc.CreatedUnixMs))
		if manifestLag < 0 {
			manifestLag = 0
		}
	}
	e.metrics.mu.Lock()
	e.metrics.backends[handle.Name].ProcessedChunks++
	e.metrics.backends[handle.Name].LastDownloadLatency = latency
	e.metrics.backends[handle.Name].ManifestLag = manifestLag
	e.metrics.mu.Unlock()

	e.deleteChunkAsync(ctx, handle, filename)
}

func (e *Engine) deleteChunkAsync(ctx context.Context, handle *storage.BackendHandle, filename string) {
	if !e.deleteLimiter.TryAcquire() {
		return
	}
	go func() {
		defer e.deleteLimiter.Release()
		if err := handle.Backend.Delete(ctx, filename); err != nil {
			log.Printf("chunk delete error backend=%s chunk=%s: %v", handle.Name, filename, err)
			e.metrics.mu.Lock()
			e.metrics.deleteErrors++
			e.metrics.backends[handle.Name].DeleteErrors++
			e.metrics.mu.Unlock()
		}
	}()
}

func (e *Engine) processEnvelope(env *Envelope, backendName string, epoch uint64) error {
	session, created := e.ensureSessionForEnvelope(env, backendName, epoch)
	if created && e.OnNewSession != nil {
		go e.OnNewSession(env.SessionID, env.TargetAddr, session)
	}

	advanced, closedNow, err := session.ProcessRx(env)
	if err != nil {
		return err
	}
	if advanced {
		e.metrics.mu.Lock()
		if e.myDir == DirReq {
			e.metrics.bytesC2S += uint64(len(env.Payload))
		} else {
			e.metrics.bytesS2C += uint64(len(env.Payload))
		}
		e.metrics.mu.Unlock()
	}
	if closedNow {
		e.RemoveSession(env.SessionID)
	}
	return nil
}

func (e *Engine) ensureSessionForEnvelope(env *Envelope, backendName string, epoch uint64) (*Session, bool) {
	opts := e.currentOptions()

	var (
		session  *Session
		replaced *Session
		created  bool
		count    int
	)

	e.sessionMu.Lock()
	session = e.sessions[env.SessionID]
	switch {
	case session == nil:
		session = NewSession(env.SessionID)
		session.ClientID = e.clientID()
		session.TargetAddr = env.TargetAddr
		session.BackendName = backendName
		session.BackendEpoch = epoch
		session.Configure(opts.MaxTxBufferBytesPerSession, opts.MaxOutOfOrderSegments, opts.SegmentBytes, e.notifyFlush, e.notifyForceFlush)
		e.sessions[session.ID] = session
		created = true
	case session.BackendEpoch == 0:
		session.BackendEpoch = epoch
	case session.BackendEpoch != epoch:
		replaced = session
		session = NewSession(env.SessionID)
		session.ClientID = e.clientID()
		session.TargetAddr = env.TargetAddr
		session.BackendName = backendName
		session.BackendEpoch = epoch
		session.Configure(opts.MaxTxBufferBytesPerSession, opts.MaxOutOfOrderSegments, opts.SegmentBytes, e.notifyFlush, e.notifyForceFlush)
		e.sessions[session.ID] = session
		created = true
	default:
		if session.TargetAddr == "" {
			session.TargetAddr = env.TargetAddr
		}
		if session.BackendName == "" {
			session.BackendName = backendName
		}
	}
	count = len(e.sessions)
	e.sessionMu.Unlock()

	if replaced != nil {
		replaced.Abort()
	}
	if created {
		e.metrics.mu.Lock()
		e.metrics.activeSessions = count
		e.metrics.mu.Unlock()
		log.Printf("session open %s client=%s backend=%s epoch=%d", session.ID, session.ClientID, session.BackendName, session.BackendEpoch)
	}
	return session, created
}

func (e *Engine) currentPollInterval() time.Duration {
	opts := e.currentOptions()
	e.sessionMu.RLock()
	active := len(e.sessions)
	turbo := false
	now := time.Now()
	for _, session := range e.sessions {
		if session.NeedsTurboPoll(now) {
			turbo = true
			break
		}
	}
	e.sessionMu.RUnlock()
	if turbo {
		return 40 * time.Millisecond
	}
	if active > 0 {
		return opts.ActivePollRate
	}
	if opts.IdlePollRate > 0 {
		return opts.IdlePollRate
	}
	return opts.PollRate
}

func (e *Engine) maintenanceLoop(ctx context.Context) {
	timer := time.NewTimer(e.currentOptions().CleanupInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			e.cleanupSessions()
			e.publishDirtyManifests(ctx)
			e.logMetrics()
			timer.Reset(e.currentOptions().CleanupInterval)
		}
	}
}

func (e *Engine) cleanupSessions() {
	opts := e.currentOptions()
	now := time.Now()

	e.sessionMu.RLock()
	sessions := make([]*Session, 0, len(e.sessions))
	for _, s := range e.sessions {
		sessions = append(sessions, s)
	}
	e.sessionMu.RUnlock()

	for _, s := range sessions {
		if s.GapTimedOut(now, opts.GapGracePeriod) {
			log.Printf("session %s gap exceeded %s, aborting", s.ID, opts.GapGracePeriod)
			s.Abort()
			e.RemoveSession(s.ID)
			continue
		}
		if s.PendingBytes() > 0 {
			continue
		}
		if s.ReadyForTeardown(now, opts.SessionIdleTimeout) {
			e.RemoveSession(s.ID)
		}
	}
}

func (e *Engine) orphanSweepLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.sweepOrphanChunks(ctx)
		}
	}
}

func (e *Engine) sweepOrphanChunks(ctx context.Context) {
	clientID := e.clientID()
	if clientID == "" {
		return
	}
	opts := e.currentOptions()
	prefix := "chunk-" + string(e.myDir) + "-" + safeID(clientID) + "-"
	now := time.Now()

	for _, handle := range e.pool.Backends() {
		names, err := handle.Backend.ListQuery(ctx, prefix)
		if err != nil {
			log.Printf("chunk orphan sweep list error backend=%s: %v", handle.Name, err)
			continue
		}
		state := e.backendState(handle.Name)
		state.mu.Lock()
		keep := make(map[string]struct{}, len(state.localManifest.Chunks))
		for _, desc := range state.localManifest.Chunks {
			keep[chunkFilename(e.myDir, clientID, state.localManifest.Epoch, desc.ID)] = struct{}{}
		}
		state.mu.Unlock()

		for _, name := range names {
			if _, exists := keep[name]; exists {
				continue
			}
			dir, parsedClientID, _, _, createdUnixMs, ok := parseChunkFilename(name)
			if !ok || dir != e.myDir || parsedClientID != clientID || createdUnixMs == 0 {
				continue
			}
			if now.Sub(time.UnixMilli(createdUnixMs)) < opts.ChunkOrphanTTL {
				continue
			}
			if err := handle.Backend.Delete(ctx, name); err != nil {
				log.Printf("chunk orphan sweep delete error backend=%s chunk=%s: %v", handle.Name, name, err)
				continue
			}
		}
	}
}

func (e *Engine) logMetrics() {
	e.metrics.mu.Lock()
	defer e.metrics.mu.Unlock()
	if time.Since(e.metrics.lastMetricsLog) < e.currentOptions().CleanupInterval {
		return
	}
	e.metrics.lastMetricsLog = time.Now()
	log.Printf("metrics active_sessions=%d bytes_c2s=%d bytes_s2c=%d pending_chunks=%d upload_errors=%d download_errors=%d delete_errors=%d manifest_errors=%d",
		e.metrics.activeSessions,
		e.metrics.bytesC2S,
		e.metrics.bytesS2C,
		e.metrics.pendingChunks,
		e.metrics.uploadErrors,
		e.metrics.downloadErrors,
		e.metrics.deleteErrors,
		e.metrics.manifestErrors,
	)
}

func (e *Engine) clientID() string {
	e.idMu.RLock()
	defer e.idMu.RUnlock()
	return e.id
}

func (e *Engine) setClientID(id string) bool {
	if id == "" {
		return false
	}
	e.idMu.Lock()
	defer e.idMu.Unlock()
	if e.id != "" {
		return false
	}
	e.id = id
	return true
}

func (e *Engine) selectBackendForSession(s *Session) (*storage.BackendHandle, error) {
	clientID := s.ClientID
	if clientID == "" {
		clientID = e.clientID()
	}

	e.selectorMu.RLock()
	selector := e.backendSelector
	e.selectorMu.RUnlock()
	if selector != nil {
		return selector(s, e.pool.HealthyBackends())
	}
	return e.pool.Select(clientID + ":" + s.ID)
}

func (e *Engine) ensureManifestContext(ctx context.Context) bool {
	if e.clientID() == "" && !e.discoverClientID(ctx) {
		return false
	}
	return e.clientID() != ""
}

func (e *Engine) discoverClientID(ctx context.Context) bool {
	seen := make(map[string]struct{})
	for _, backend := range e.pool.Backends() {
		names, err := backend.Backend.ListQuery(ctx, "manifest-"+string(e.peerDir)+"-")
		if err != nil {
			log.Printf("manifest discovery error backend=%s: %v", backend.Name, err)
			continue
		}
		for _, name := range names {
			dir, clientID, ok := parseManifestFilename(name)
			if !ok || dir != e.peerDir || clientID == "" {
				continue
			}
			seen[clientID] = struct{}{}
		}
	}
	if len(seen) != 1 {
		if len(seen) > 1 {
			log.Printf("multiple client manifest IDs discovered; set client_id explicitly on this side")
		}
		return false
	}
	for clientID := range seen {
		if e.setClientID(clientID) {
			log.Printf("discovered client_id=%s from %s manifests", clientID, e.peerDir)
		}
		return true
	}
	return false
}

func (e *Engine) abortBackendEpochSessions(backendName string, epoch uint64) {
	e.sessionMu.RLock()
	sessions := make([]*Session, 0, len(e.sessions))
	for _, s := range e.sessions {
		if s.BackendName == backendName && s.BackendEpoch == epoch {
			sessions = append(sessions, s)
		}
	}
	e.sessionMu.RUnlock()
	for _, s := range sessions {
		s.Abort()
		e.RemoveSession(s.ID)
	}
}

func (e *Engine) backendState(name string) *backendState {
	e.backendMu.Lock()
	defer e.backendMu.Unlock()
	state := e.backends[name]
	if state != nil {
		return state
	}
	opts := e.currentOptions()
	state = &backendState{
		name:         name,
		seenChunks:   newSeenChunkCache(opts.SeenChunkCacheSize, opts.ChunkOrphanTTL*3),
		inFlight:     make(map[string]struct{}),
		sessionHeads: make(map[string]*sessionHeadState),
	}
	e.backends[name] = state
	e.metrics.mu.Lock()
	if e.metrics.backends[name] == nil {
		e.metrics.backends[name] = &BackendStats{}
	}
	e.metrics.mu.Unlock()
	return state
}

func (e *Engine) updatePendingChunkMetrics(backendName string, depth int) {
	e.metrics.mu.Lock()
	defer e.metrics.mu.Unlock()
	if e.metrics.backends[backendName] == nil {
		e.metrics.backends[backendName] = &BackendStats{}
	}
	e.metrics.backends[backendName].PendingChunks = depth
	total := 0
	for _, stats := range e.metrics.backends {
		total += stats.PendingChunks
	}
	e.metrics.pendingChunks = total
}

func (e *Engine) currentOptions() Options {
	e.optsMu.RLock()
	defer e.optsMu.RUnlock()
	return e.opts
}
