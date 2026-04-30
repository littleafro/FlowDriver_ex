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
	pool                *storage.BackendPool
	myDir               Direction
	peerDir             Direction
	idMu                sync.RWMutex
	id                  string
	opts                Options
	sessions            map[string]*Session
	sessionMu           sync.RWMutex
	closedSessions      map[string]time.Time
	closedSessionsMu    sync.Mutex
	stream              *streamManager
	uploadSem           chan struct{}
	downloadSem         chan struct{}
	metrics             metrics
	startedAtUnixMs     int64
	OnNewSession        func(sessionID, targetAddr string, s *Session)
	streamOffsets       map[string]int64
	streamOffsetDirty   map[string]bool
	streamOffsetMu      sync.Mutex
	streamOffsetsLoaded bool
}

func NewEngine(backend storage.Backend, isClient bool, clientID string) *Engine {
	return NewEngineWithPool(storage.NewSingleBackendPool("default", backend), isClient, clientID, DefaultOptions())
}

func NewEngineWithPool(pool *storage.BackendPool, isClient bool, clientID string, opts Options) *Engine {
	opts.ApplyDefaults()
	e := &Engine{
		pool:              pool,
		id:                clientID,
		opts:              opts,
		sessions:          make(map[string]*Session),
		closedSessions:    make(map[string]time.Time),
		stream:            newStreamManager(opts.DataDir),
		uploadSem:         make(chan struct{}, opts.MaxConcurrentUploads),
		downloadSem:       make(chan struct{}, opts.MaxConcurrentDownloads),
		streamOffsets:     make(map[string]int64),
		streamOffsetDirty: make(map[string]bool),
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
	e.ensureStreamContext(ctx)
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
	e.flushStreamOffsets(ctx)
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
	e.sessionMu.Lock()
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
	payloads := make(map[string]*bytes.Buffer)

	for _, s := range sessions {
		if now.Sub(s.LastActivity()) >= e.opts.SessionIdleTimeout && !s.HasCloseQueued() {
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
		allowHeartbeat := false
		if e.opts.HeartbeatInterval > 0 {
			allowHeartbeat = s.CanHeartbeat(now, e.opts.HeartbeatInterval)
		}
		envs, err := s.PrepareEnvelopeBatch(now, e.opts.SegmentBytes, e.opts.MaxSegmentBytes, e.opts.MaxMuxSegments, force, allowHeartbeat)
		if err != nil {
			log.Printf("prepare envelope batch failed session=%s: %v", s.ID, err)
			continue
		}
		buf := payloads[s.BackendName]
		if buf == nil {
			buf = &bytes.Buffer{}
			payloads[s.BackendName] = buf
		}
		for i := range envs {
			data, err := envs[i].Env.MarshalBinary()
			if err != nil {
				log.Printf("marshal envelope failed session=%s seq=%d: %v", s.ID, envs[i].Env.Seq, err)
				continue
			}
			buf.Write(data)
			_ = s.CommitEnvelope(envs[i].Env, envs[i].Consumed)
		}
	}

	if len(payloads) == 0 {
		return
	}

	clientID := e.clientID()
	if clientID == "" {
		return
	}
	for backendName, payload := range payloads {
		if payload.Len() == 0 {
			continue
		}
		_, err := e.stream.AppendForBackend(e.myDir, clientID, backendName, payload.Bytes())
		if err != nil {
			log.Printf("stream append failed backend=%s: %v", backendName, err)
			continue
		}
		log.Printf("flushed %d bytes to stream backend=%s", payload.Len(), backendName)
	}
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

	clientID := e.clientID()
	if clientID == "" {
		return
	}
	for _, backend := range e.pool.Backends() {
		size, err := e.stream.FlushForBackend(e.myDir, clientID, backend.Name, backend.Backend, ctx)
		if err != nil {
			log.Printf("stream upload error backend=%s: %v", backend.Name, err)
			e.metrics.mu.Lock()
			e.metrics.uploadErrors++
			e.metrics.retries++
			e.metrics.mu.Unlock()
			continue
		}
		if size == 0 {
			continue
		}
		log.Printf("stream uploaded backend=%s bytes=%d", backend.Name, size)
		e.metrics.mu.Lock()
		e.metrics.uploadAttempts++
		e.metrics.mu.Unlock()
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
	select {
	case e.downloadSem <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-e.downloadSem }()

	if !e.ensureStreamContext(ctx) {
		return
	}
	clientID := e.clientID()
	if clientID == "" {
		return
	}
	for _, backend := range e.pool.Backends() {
		data, err := e.stream.DownloadStream(e.peerDir, clientID, backend.Backend, ctx)
		if err != nil {
			if !storage.IsNotFoundError(err) {
				log.Printf("stream download error backend=%s: %v", backend.Name, err)
				e.metrics.mu.Lock()
				e.metrics.downloadErrors++
				e.metrics.mu.Unlock()
			}
			continue
		}

		offset := e.streamOffset(backend.Name)
		streamLen := int64(len(data))
		if streamLen < offset {
			log.Printf("stream regressed backend=%s previous_offset=%d current_size=%d; resetting offset to 0",
				backend.Name, offset, streamLen)
			offset = 0
			e.setStreamOffset(backend.Name, 0, true)
		}
		if streamLen <= offset {
			continue
		}
		newData := data[offset:]
		e.setStreamOffset(backend.Name, int64(len(data)), true)

		processed := e.processStreamData(newData, backend.Name)

		e.metrics.mu.Lock()
		e.metrics.downloadAttempts++
		e.metrics.mu.Unlock()

		log.Printf("stream downloaded backend=%s offset=%d processed=%d bytes=%d",
			backend.Name, len(data), processed, len(newData))
	}
}

func (e *Engine) processStreamData(data []byte, backendName string) int {
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
				session.ClientID = e.clientID()
				session.TargetAddr = env.TargetAddr
				session.BackendName = backendName
				e.AddSession(session)
				if e.OnNewSession != nil {
					go e.OnNewSession(env.SessionID, env.TargetAddr, session)
				}
			} else {
				off += envLen
				continue
			}
		} else if session.BackendName == "" {
			session.BackendName = backendName
		} else if session.BackendName != backendName {
			log.Printf("session backend mismatch session=%s existing=%s incoming=%s", env.SessionID, session.BackendName, backendName)
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

func (e *Engine) offsetFlushLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.flushStreamOffsets(ctx)
		}
	}
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
	return e.pool.Select(clientID + ":" + s.ID)
}

func (e *Engine) ensureStreamContext(ctx context.Context) bool {
	if e.clientID() == "" && !e.discoverClientID(ctx) {
		return false
	}
	e.ensureStreamOffsetsLoaded(ctx)
	return e.clientID() != ""
}

func (e *Engine) discoverClientID(ctx context.Context) bool {
	seen := make(map[string]struct{})
	for _, backend := range e.pool.Backends() {
		names, err := backend.Backend.ListQuery(ctx, "stream-"+string(e.peerDir)+"-")
		if err != nil {
			log.Printf("stream discovery error backend=%s: %v", backend.Name, err)
			continue
		}
		for _, name := range names {
			dir, clientID, ok := parseStreamFilename(name)
			if !ok || dir != e.peerDir || clientID == "" {
				continue
			}
			seen[clientID] = struct{}{}
		}
	}
	if len(seen) != 1 {
		if len(seen) > 1 {
			log.Printf("multiple client stream IDs discovered; set client_id explicitly on this side")
		}
		return false
	}
	for clientID := range seen {
		if e.setClientID(clientID) {
			log.Printf("discovered client_id=%s from %s streams", clientID, e.peerDir)
		}
		return true
	}
	return false
}

func (e *Engine) ensureStreamOffsetsLoaded(ctx context.Context) {
	clientID := e.clientID()
	if clientID == "" {
		return
	}

	e.streamOffsetMu.Lock()
	if e.streamOffsetsLoaded {
		e.streamOffsetMu.Unlock()
		return
	}
	e.streamOffsetMu.Unlock()

	for _, backend := range e.pool.Backends() {
		offset, err := loadOffsetFromDrive(backend.Backend, e.peerDir, clientID, ctx)
		if err != nil {
			continue
		}
		e.setStreamOffset(backend.Name, offset.Offset, false)
		log.Printf("loaded stream offset backend=%s dir=%s offset=%d", backend.Name, e.peerDir, offset.Offset)
	}

	e.streamOffsetMu.Lock()
	e.streamOffsetsLoaded = true
	e.streamOffsetMu.Unlock()
}

func (e *Engine) streamOffset(backendName string) int64 {
	e.streamOffsetMu.Lock()
	defer e.streamOffsetMu.Unlock()
	return e.streamOffsets[backendName]
}

func (e *Engine) setStreamOffset(backendName string, offset int64, dirty bool) {
	e.streamOffsetMu.Lock()
	e.streamOffsets[backendName] = offset
	if dirty {
		e.streamOffsetDirty[backendName] = true
	}
	e.streamOffsetMu.Unlock()
}

func (e *Engine) flushStreamOffsets(ctx context.Context) {
	clientID := e.clientID()
	if clientID == "" {
		return
	}

	for _, backend := range e.pool.Backends() {
		e.streamOffsetMu.Lock()
		dirty := e.streamOffsetDirty[backend.Name]
		offset := e.streamOffsets[backend.Name]
		if dirty {
			e.streamOffsetDirty[backend.Name] = false
		}
		e.streamOffsetMu.Unlock()
		if !dirty {
			continue
		}

		off := streamOffset{
			Dir:            e.peerDir,
			ClientID:       clientID,
			Offset:         offset,
			LastUploadedMs: time.Now().UnixMilli(),
		}
		if err := saveOffsetToDrive(off, backend.Backend, ctx); err != nil {
			log.Printf("failed to save offset backend=%s: %v", backend.Name, err)
			e.streamOffsetMu.Lock()
			e.streamOffsetDirty[backend.Name] = true
			e.streamOffsetMu.Unlock()
			continue
		}
		log.Printf("saved stream offset backend=%s dir=%s offset=%d", backend.Name, e.peerDir, offset)
	}
}
