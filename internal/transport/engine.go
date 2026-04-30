package transport

import (
	"bytes"
	"context"
	"encoding/binary"
	"log"
	"sync"
	"time"

	"github.com/NullLatency/flow-driver/internal/storage"
)

type metrics struct {
	mu               sync.Mutex
	activeSessions   int
	bytesC2S         uint64
	bytesS2C         uint64
	retries          uint64
	uploadErrors     uint64
	downloadErrors   uint64
	uploadAttempts   uint64
	downloadAttempts uint64
	lastMetricsLog   time.Time
}

type Engine struct {
	pool         *storage.BackendPool
	myDir        Direction
	peerDir      Direction
	id           string
	opts         Options
	sessions     map[string]*Session
	sessionMu    sync.RWMutex
	closedSessions   map[string]time.Time
	closedSessionsMu sync.Mutex
	stream       *streamManager
	uploadSem    chan struct{}
	downloadSem  chan struct{}
	metrics      metrics
	startedAtUnixMs int64
	OnNewSession func(sessionID, targetAddr string, s *Session)
	// Server-side: track read offset for stream file
	serverOffset     int64
	serverOffsetMu   sync.Mutex
	serverOffsetDirty bool
}

func NewEngine(backend storage.Backend, isClient bool, clientID string) *Engine {
	return NewEngineWithPool(storage.NewSingleBackendPool("default", backend), isClient, clientID, DefaultOptions())
}

func NewEngineWithPool(pool *storage.BackendPool, isClient bool, clientID string, opts Options) *Engine {
	opts.ApplyDefaults()
	e := &Engine{
		pool:           pool,
		id:             clientID,
		opts:           opts,
		sessions:       make(map[string]*Session),
		closedSessions: make(map[string]time.Time),
		stream:         newStreamManager(opts.DataDir),
		uploadSem:      make(chan struct{}, opts.MaxConcurrentUploads),
		downloadSem:    make(chan struct{}, opts.MaxConcurrentDownloads),
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
	if e.myDir == DirRes {
		e.loadServerOffset(ctx)
	}
	go e.flushLoop(ctx)
	go e.uploadLoop(ctx)
	go e.pollLoop(ctx)
	go e.offsetFlushLoop(ctx)
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
	e.flushAll(true)
	e.triggerUpload()
	if e.myDir == DirRes {
		e.flushServerOffset(ctx)
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
	s.Configure(e.opts.MaxTxBufferBytesPerSession, e.opts.MaxOutOfOrderSegments, int(e.opts.FlushRate.Milliseconds()), e.notifyFlush, e.notifyForceFlush)
	if s.ClientID == "" {
		s.ClientID = e.id
	}
	e.sessionMu.Lock()
	e.sessions[s.ID] = s
	count := len(e.sessions)
	e.sessionMu.Unlock()
	e.metrics.mu.Lock()
	e.metrics.activeSessions = count
	e.metrics.mu.Unlock()
	log.Printf("session open %s client=%s", s.ID, s.ClientID)
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
	case flushCh <- false:
	default:
	}
}

func (e *Engine) notifyForceFlush() {
	select {
	case flushCh <- true:
	default:
	}
}

var flushCh = make(chan bool, 4)

func (e *Engine) flushLoop(ctx context.Context) {
	ticker := time.NewTicker(e.opts.FlushRate)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case force := <-flushCh:
			e.flushAll(force)
		case <-ticker.C:
			e.flushAll(false)
		}
	}
}

func (e *Engine) flushAll(force bool) {
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
	var allEnvelopes [][]byte

	for _, s := range sessions {
		if now.Sub(s.LastActivity()) >= e.opts.SessionIdleTimeout && !s.HasCloseQueued() {
			log.Printf("session %s idle for %s, queuing close", s.ID, now.Sub(s.LastActivity()))
			s.QueueClose()
		}
		envs, err := s.PrepareEnvelopeBatch(now, e.opts.SegmentBytes, e.opts.MaxSegmentBytes, e.opts.MaxMuxSegments, force, e.opts.HeartbeatInterval > 0)
		if err != nil {
			log.Printf("prepare envelope batch failed session=%s: %v", s.ID, err)
			continue
		}
		for i := range envs {
			data, err := envs[i].Env.MarshalBinary()
			if err != nil {
				log.Printf("marshal envelope failed session=%s seq=%d: %v", s.ID, envs[i].Env.Seq, err)
				continue
			}
			allEnvelopes = append(allEnvelopes, data)
			_ = s.CommitEnvelope(envs[i].Env, envs[i].Consumed)
		}
	}

	if len(allEnvelopes) == 0 {
		return
	}

	var payload bytes.Buffer
	for _, data := range allEnvelopes {
		payload.Write(data)
	}

	_, err := e.stream.Append(e.myDir, e.id, payload.Bytes())
	if err != nil {
		log.Printf("stream append failed: %v", err)
		return
	}
	log.Printf("flushed %d bytes to stream", len(payload.Bytes()))
	e.triggerUpload()
}

func (e *Engine) triggerUpload() {
	select {
	case uploadCh <- struct{}{}:
	default:
	}
}

var uploadCh = make(chan struct{}, 1)

func (e *Engine) uploadLoop(ctx context.Context) {
	ticker := time.NewTicker(e.opts.UploadInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-uploadCh:
			e.doUpload(ctx)
		case <-ticker.C:
			e.doUpload(ctx)
		}
	}
}

func (e *Engine) doUpload(ctx context.Context) {
	select {
	case e.uploadSem <- struct{}{}:
	default:
		return
	}
	defer func() { <-e.uploadSem }()

	backends := e.pool.Backends()
	if len(backends) == 0 {
		return
	}
	backend := backends[0]

	_, err := e.stream.Flush(e.myDir, e.id, backend.Backend, ctx)
	if err != nil {
		log.Printf("stream upload error backend=%s: %v", backend.Name, err)
		e.metrics.mu.Lock()
		e.metrics.uploadErrors++
		e.metrics.retries++
		e.metrics.mu.Unlock()
		return
	}
	log.Printf("stream uploaded backend=%s", backend.Name)
	e.metrics.mu.Lock()
	e.metrics.uploadAttempts++
	e.metrics.mu.Unlock()
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
	select {
	case e.downloadSem <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-e.downloadSem }()

	backends := e.pool.Backends()
	if len(backends) == 0 {
		return
	}
	backend := backends[0]

	data, err := e.stream.DownloadStream(e.peerDir, e.id, backend.Backend, ctx)
	if err != nil {
		if !isNotFoundErr(err) {
			log.Printf("stream download error backend=%s: %v", backend.Name, err)
			e.metrics.mu.Lock()
			e.metrics.downloadErrors++
			e.metrics.mu.Unlock()
		}
		return
	}

	e.serverOffsetMu.Lock()
	offset := e.serverOffset
	if int64(len(data)) <= offset {
		e.serverOffsetMu.Unlock()
		return
	}
	newData := data[offset:]
	e.serverOffset = int64(len(data))
	e.serverOffsetMu.Unlock()
	e.serverOffsetDirty = true

	processed := e.processStreamData(newData)

	e.metrics.mu.Lock()
	e.metrics.downloadAttempts++
	e.metrics.mu.Unlock()

	log.Printf("stream downloaded offset=%d processed=%d bytes=%d",
		e.serverOffset, processed, len(newData))
}

func (e *Engine) processStreamData(data []byte) int {
	processed := 0
	off := 0

	for off < len(data) {
		remaining := len(data) - off
		if remaining < 5 {
			break
		}
		env := &Envelope{}
		if err := env.Decode(bytes.NewReader(data[off:])); err != nil {
			break
		}
		envLen := envelopeBinaryLen(data[off:])
		if envLen == 0 {
			break
		}

		session := e.GetSession(env.SessionID)
		if session == nil {
			if env.Kind == KindOpen {
				session = NewSession(env.SessionID)
				session.ClientID = e.id
				session.TargetAddr = env.TargetAddr
				e.AddSession(session)
				if e.OnNewSession != nil {
					go e.OnNewSession(env.SessionID, env.TargetAddr, session)
				}
			} else {
				off += envLen
				continue
			}
		}

		_, advanced, closedNow, err := session.ProcessRx(env)
		if err != nil {
			log.Printf("process segment failed session=%s seq=%d: %v", env.SessionID, env.Seq, err)
			break
		}
		if advanced {
			processed++
			e.metrics.mu.Lock()
			if e.myDir == DirReq {
				e.metrics.bytesC2S += uint64(len(env.Payload))
			} else {
				e.metrics.bytesS2C += uint64(len(env.Payload))
			}
			e.metrics.mu.Unlock()
		}
		if closedNow {
			e.markClosedSession(env.SessionID)
		}
		off += envLen
	}

	return processed
}

func envelopeBinaryLen(data []byte) int {
	if len(data) < 5 {
		return 0
	}
	sessionLen := int(binary.BigEndian.Uint16(data[3:5]))
	off := 5 + sessionLen
	// After session ID: Seq(8) + Created(8) + TargetLen(2)
	if off+8+8+2 > len(data) {
		return 0
	}
	targetLen := int(binary.BigEndian.Uint16(data[off+16 : off+18]))
	off += 8 + 8 + 2 + targetLen
	// After target: Checksum(4) + PayloadLen(4) + Payload
	if off+4+4 > len(data) {
		return 0
	}
	off += 4 // skip checksum
	payloadLen := int(binary.BigEndian.Uint32(data[off : off+4]))
	off += 4 + payloadLen
	return off
}

func isNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return len(s) > 0 && (s == "file not found: " || len(s) > 100)
}

func (e *Engine) markClosedSession(id string) {
	e.closedSessionsMu.Lock()
	e.closedSessions[id] = time.Now()
	e.closedSessionsMu.Unlock()
}

func (e *Engine) currentPollInterval() time.Duration {
	e.sessionMu.RLock()
	active := len(e.sessions)
	e.sessionMu.RUnlock()
	if active > 0 {
		return e.opts.ActivePollRate
	}
	return e.opts.PollRate
}

// Server-side offset persistence
func (e *Engine) loadServerOffset(ctx context.Context) {
	backends := e.pool.Backends()
	if len(backends) == 0 {
		return
	}
	backend := backends[0]
	offset, err := loadOffsetFromDrive(backend.Backend, e.myDir, e.id, ctx)
	if err != nil {
		log.Printf("no saved offset found, starting from 0")
		return
	}
	e.serverOffset = offset.Offset
	log.Printf("loaded server offset=%d from drive", offset.Offset)
}

func (e *Engine) offsetFlushLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if e.serverOffsetDirty {
				e.flushServerOffset(ctx)
			}
		}
	}
}

func (e *Engine) flushServerOffset(ctx context.Context) {
	e.serverOffsetMu.Lock()
	if !e.serverOffsetDirty {
		e.serverOffsetMu.Unlock()
		return
	}
	offset := e.serverOffset
	e.serverOffsetDirty = false
	e.serverOffsetMu.Unlock()

	backends := e.pool.Backends()
	if len(backends) == 0 {
		return
	}
	backend := backends[0]
	off := streamOffset{
		Dir:            e.myDir,
		ClientID:       e.id,
		Offset:         offset,
		LastUploadedMs: time.Now().UnixMilli(),
	}
	if err := saveOffsetToDrive(off, backend.Backend, ctx); err != nil {
		log.Printf("failed to save offset: %v", err)
		e.serverOffsetDirty = true
		return
	}
	log.Printf("saved server offset=%d", offset)
}

func (e *Engine) maintenanceLoop(ctx context.Context) {
	ticker := time.NewTicker(e.opts.CleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.cleanupSessions()
			e.logMetrics()
		}
	}
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
		if s.ReadyForTeardown(now, e.opts.SessionIdleTimeout) {
			e.RemoveSession(s.ID)
		}
	}
}

func (e *Engine) logMetrics() {
	e.metrics.mu.Lock()
	defer e.metrics.mu.Unlock()
	if time.Since(e.metrics.lastMetricsLog) < e.opts.CleanupInterval {
		return
	}
	e.metrics.lastMetricsLog = time.Now()
	log.Printf("metrics active_sessions=%d bytes_c2s=%d bytes_s2c=%d retries=%d upload_errors=%d download_errors=%d",
		e.metrics.activeSessions,
		e.metrics.bytesC2S,
		e.metrics.bytesS2C,
		e.metrics.retries,
		e.metrics.uploadErrors,
		e.metrics.downloadErrors,
	)
}
