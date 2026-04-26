package transport

import (
	"fmt"
	"sync"
	"time"
)

type Session struct {
	ID string

	mu    sync.Mutex
	txBuf []byte
	txSeq uint64
	rxSeq uint64

	rxQueue map[uint64]*Envelope

	lastActivity     time.Time
	lastHeartbeatTx  time.Time
	closeQueued      bool
	closeSent        bool
	rxClosed         bool
	closed           bool
	openSent         bool
	TargetAddr       string
	ClientID         string
	BackendName      string
	maxTxBufferBytes int
	maxOutOfOrder    int
	flushThreshold   int
	onFlush          func()
	onForceFlush     func()

	txCond *sync.Cond
	RxChan chan []byte
}

func NewSession(id string) *Session {
	s := &Session{
		ID:               id,
		rxQueue:          make(map[uint64]*Envelope),
		lastActivity:     time.Now(),
		lastHeartbeatTx:  time.Now(),
		maxTxBufferBytes: 4 * 1024 * 1024,
		maxOutOfOrder:    64,
		flushThreshold:   64 * 1024,
		RxChan:           make(chan []byte, 1024),
	}
	s.txCond = sync.NewCond(&s.mu)
	return s
}

func (s *Session) Configure(maxTxBufferBytes, maxOutOfOrder, flushThreshold int, onFlush, onForceFlush func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if maxTxBufferBytes > 0 {
		s.maxTxBufferBytes = maxTxBufferBytes
	}
	if maxOutOfOrder > 0 {
		s.maxOutOfOrder = maxOutOfOrder
	}
	if flushThreshold > 0 {
		s.flushThreshold = flushThreshold
	}
	s.onFlush = onFlush
	s.onForceFlush = onForceFlush
}

func (s *Session) EnqueueTx(data []byte) {
	s.mu.Lock()
	for len(s.txBuf) >= s.maxTxBufferBytes && !s.closeQueued {
		s.txCond.Wait()
	}
	wasEmpty := len(s.txBuf) == 0
	if len(data) > 0 {
		s.txBuf = append(s.txBuf, data...)
	}
	s.lastActivity = time.Now()
	forceNotify := wasEmpty && len(s.txBuf) > 0 && s.onForceFlush != nil
	notify := s.onFlush != nil && len(s.txBuf) >= s.flushThreshold
	cb := s.onFlush
	forceCB := s.onForceFlush
	s.mu.Unlock()

	if forceNotify {
		forceCB()
		return
	}
	if notify {
		cb()
	}
}

func (s *Session) QueueClose() {
	s.mu.Lock()
	if s.closeQueued {
		s.mu.Unlock()
		return
	}
	s.closeQueued = true
	s.lastActivity = time.Now()
	cb := s.onFlush
	s.mu.Unlock()

	if cb != nil {
		cb()
	}
}

func (s *Session) PrepareEnvelope(now time.Time, segmentBytes, maxSegmentBytes int, force, allowHeartbeat bool) (*Envelope, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closeSent || s.rxClosed {
		return nil, 0, nil
	}

	shouldSendOpen := !s.openSent
	shouldHeartbeat := allowHeartbeat && !s.closeQueued && len(s.txBuf) == 0 && now.Sub(s.lastHeartbeatTx) > 0

	if !shouldSendOpen && !force && len(s.txBuf) < segmentBytes && !s.closeQueued && !shouldHeartbeat {
		return nil, 0, nil
	}
	if !shouldSendOpen && len(s.txBuf) == 0 && !s.closeQueued && !shouldHeartbeat {
		return nil, 0, nil
	}

	kind := KindData
	if shouldSendOpen {
		kind = KindOpen
	} else if shouldHeartbeat {
		kind = KindHeartbeat
	}

	payloadLen := len(s.txBuf)
	if payloadLen > maxSegmentBytes {
		payloadLen = maxSegmentBytes
	}
	if payloadLen == 0 && kind == KindData && s.closeQueued {
		kind = KindClose
	}
	if kind == KindClose {
		payloadLen = len(s.txBuf)
		if payloadLen > maxSegmentBytes {
			payloadLen = maxSegmentBytes
			kind = KindData
		}
	}
	if kind == KindData && s.closeQueued && payloadLen == len(s.txBuf) {
		kind = KindClose
	}

	payload := make([]byte, payloadLen)
	copy(payload, s.txBuf[:payloadLen])

	env := &Envelope{
		SessionID:     s.ID,
		Seq:           s.txSeq,
		Kind:          kind,
		TargetAddr:    s.TargetAddr,
		Payload:       payload,
		CreatedUnixMs: now.UnixMilli(),
	}
	env.EnsureChecksum()
	return env, payloadLen, nil
}

func (s *Session) CommitEnvelope(env *Envelope, consumed int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if env == nil {
		return nil
	}
	if env.Seq != s.txSeq {
		return fmt.Errorf("session %s seq mismatch commit=%d expected=%d", s.ID, env.Seq, s.txSeq)
	}
	if consumed > len(s.txBuf) {
		return fmt.Errorf("session %s commit overrun consumed=%d buffered=%d", s.ID, consumed, len(s.txBuf))
	}

	if consumed > 0 {
		s.txBuf = append([]byte(nil), s.txBuf[consumed:]...)
	}
	s.txSeq++
	s.openSent = s.openSent || env.Kind == KindOpen
	if env.Kind == KindClose {
		s.closeSent = true
		s.closed = true
	}
	if env.Kind == KindHeartbeat {
		s.lastHeartbeatTx = time.Now()
	}
	s.txCond.Broadcast()
	return nil
}

func (s *Session) ProcessRx(env *Envelope) (uint64, bool, bool, error) {
	s.mu.Lock()
	if s.rxClosed {
		acked, ok := s.currentAckedSeqLocked()
		s.mu.Unlock()
		return acked, ok, true, nil
	}
	s.lastActivity = time.Now()

	if env.Seq < s.rxSeq {
		acked, _ := s.currentAckedSeqLocked()
		s.mu.Unlock()
		return acked, false, false, nil
	}
	if env.Seq > s.rxSeq {
		if _, exists := s.rxQueue[env.Seq]; !exists {
			if len(s.rxQueue) >= s.maxOutOfOrder {
				acked, _ := s.currentAckedSeqLocked()
				s.mu.Unlock()
				return acked, false, false, fmt.Errorf("session %s out-of-order buffer full", s.ID)
			}
			copyEnv := *env
			copyEnv.Payload = append([]byte(nil), env.Payload...)
			s.rxQueue[env.Seq] = &copyEnv
		}
		acked, _ := s.currentAckedSeqLocked()
		s.mu.Unlock()
		return acked, false, false, nil
	}

	payloads := make([][]byte, 0, 2)
	closeNow := false
	for current := env; current != nil; {
		if len(current.Payload) > 0 {
			payloads = append(payloads, append([]byte(nil), current.Payload...))
		}
		s.rxSeq++
		switch current.Kind {
		case KindClose:
			s.rxClosed = true
			s.closed = true
			closeNow = true
		case KindHeartbeat, KindOpen, KindData:
		default:
			s.mu.Unlock()
			return 0, false, false, fmt.Errorf("unknown envelope kind: %d", current.Kind)
		}
		if closeNow {
			break
		}
		next := s.rxQueue[s.rxSeq]
		delete(s.rxQueue, s.rxSeq)
		current = next
	}
	acked, _ := s.currentAckedSeqLocked()
	s.mu.Unlock()

	for _, payload := range payloads {
		s.RxChan <- payload
	}
	if closeNow {
		close(s.RxChan)
	}
	return acked, true, closeNow, nil
}

func (s *Session) currentAckedSeqLocked() (uint64, bool) {
	if s.rxSeq == 0 {
		return 0, false
	}
	return s.rxSeq - 1, true
}

func (s *Session) CurrentAckedSeq() (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentAckedSeqLocked()
}

func (s *Session) LastActivity() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastActivity
}

func (s *Session) HasCloseQueued() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeQueued
}

func (s *Session) PendingBytes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.txBuf)
}

func (s *Session) PendingSeq() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.txSeq
}

func (s *Session) CanHeartbeat(now time.Time, heartbeatInterval time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closeQueued || s.closeSent || s.rxClosed {
		return false
	}
	return now.Sub(s.lastHeartbeatTx) >= heartbeatInterval
}

func (s *Session) ReadyForTeardown(now time.Time, grace time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.txBuf) > 0 {
		return false
	}
	if s.rxClosed {
		return true
	}
	if s.closeQueued || s.closeSent || s.closed {
		return now.Sub(s.lastActivity) >= grace
	}
	return false
}
