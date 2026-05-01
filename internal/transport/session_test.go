package transport

import (
	"testing"
	"time"
)

func TestDuplicateSeqIgnored(t *testing.T) {
	t.Parallel()

	s := NewSession("s1")
	env := &Envelope{
		SessionID:     "s1",
		Seq:           0,
		Kind:          KindData,
		Payload:       []byte("hello"),
		CreatedUnixMs: time.Now().UnixMilli(),
	}
	env.EnsureChecksum()
	if advanced, _, err := s.ProcessRx(env); err != nil || !advanced {
		t.Fatalf("first ProcessRx failed advanced=%v err=%v", advanced, err)
	}
	if advanced, _, err := s.ProcessRx(env); err != nil {
		t.Fatalf("duplicate ProcessRx failed: %v", err)
	} else if advanced {
		t.Fatalf("duplicate seq should not advance")
	}

	select {
	case got := <-s.RxChan:
		if string(got) != "hello" {
			t.Fatalf("unexpected payload %q", got)
		}
	default:
		t.Fatalf("expected payload")
	}
	select {
	case <-s.RxChan:
		t.Fatalf("duplicate payload should not be delivered")
	default:
	}
}

func TestOutOfOrderBuffered(t *testing.T) {
	t.Parallel()

	s := NewSession("s1")
	second := &Envelope{
		SessionID:     "s1",
		Seq:           1,
		Kind:          KindData,
		Payload:       []byte("two"),
		CreatedUnixMs: time.Now().UnixMilli(),
	}
	second.EnsureChecksum()
	first := &Envelope{
		SessionID:     "s1",
		Seq:           0,
		Kind:          KindData,
		Payload:       []byte("one"),
		CreatedUnixMs: time.Now().UnixMilli(),
	}
	first.EnsureChecksum()

	if advanced, _, err := s.ProcessRx(second); err != nil {
		t.Fatalf("process second failed: %v", err)
	} else if advanced {
		t.Fatalf("out-of-order segment should not advance immediately")
	}
	if advanced, _, err := s.ProcessRx(first); err != nil {
		t.Fatalf("process first failed: %v", err)
	} else if !advanced {
		t.Fatalf("expected in-order advance after missing segment arrived")
	}

	got1 := <-s.RxChan
	got2 := <-s.RxChan
	if string(got1) != "one" || string(got2) != "two" {
		t.Fatalf("unexpected delivery order %q %q", got1, got2)
	}
}

func TestGapTimedOut(t *testing.T) {
	t.Parallel()

	s := NewSession("s1")
	env := &Envelope{
		SessionID:     "s1",
		Seq:           2,
		Kind:          KindData,
		Payload:       []byte("late"),
		CreatedUnixMs: time.Now().UnixMilli(),
	}
	env.EnsureChecksum()

	if advanced, _, err := s.ProcessRx(env); err != nil {
		t.Fatalf("process rx failed: %v", err)
	} else if advanced {
		t.Fatalf("gap should not advance")
	}
	time.Sleep(25 * time.Millisecond)
	if !s.GapTimedOut(time.Now(), 10*time.Millisecond) {
		t.Fatalf("expected gap timeout to trip")
	}
}

func TestEnqueueTxTriggersForceFlushOnFirstSmallWrite(t *testing.T) {
	t.Parallel()

	s := NewSession("s1")
	forceFlushes := 0
	flushes := 0
	s.Configure(4*1024*1024, 64, 64*1024, func() {
		flushes++
	}, func() {
		forceFlushes++
	})

	s.EnqueueTx([]byte("hello"))
	if forceFlushes != 1 {
		t.Fatalf("expected first small write to trigger force flush once, got %d", forceFlushes)
	}
	if flushes != 0 {
		t.Fatalf("expected no threshold flush on first small write, got %d", flushes)
	}
}
