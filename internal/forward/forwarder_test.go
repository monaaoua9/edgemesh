package forward

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"edgemesh/internal/sink"
	"edgemesh/internal/telemetry"
	"edgemesh/internal/wal"
)

type fakeSink struct {
	mu      sync.Mutex
	down    atomic.Bool
	perm    atomic.Bool
	got     []float64
	batches []int
}

func (s *fakeSink) Send(_ context.Context, b sink.Batch) error {
	if s.down.Load() {
		return errors.New("network down")
	}
	if s.perm.Load() {
		return fmt.Errorf("%w: nope", sink.ErrPermanent)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range b.Readings {
		s.got = append(s.got, r.Value)
	}
	s.batches = append(s.batches, len(b.Readings))
	return nil
}

func (s *fakeSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.got)
}

func put(t *testing.T, l *wal.Log, from, n int) {
	t.Helper()
	for i := from; i < from+n; i++ {
		b, _ := (&telemetry.Reading{DeviceID: "d1", Metric: "m", Value: float64(i)}).AppendBinary(nil)
		if _, err := l.Append(b); err != nil {
			t.Fatal(err)
		}
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func fastCfg() Config {
	return Config{GatewayID: "gw", MinBatch: 4, MaxBatch: 64, Linger: 5 * time.Millisecond, BackoffMin: 5 * time.Millisecond, BackoffMax: 20 * time.Millisecond}
}

func TestOutageThenRecoveryLosesNothing(t *testing.T) {
	dir := t.TempDir()
	l, err := wal.Open(dir, wal.Options{SegmentBytes: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	s := &fakeSink{}
	s.down.Store(true)
	f := New(l, s, OpenCheckpoint(dir), fastCfg())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { f.Run(ctx); close(done) }()

	put(t, l, 0, 300)
	waitFor(t, "offline state", func() bool { return f.State() == StateOffline })
	if s.count() != 0 {
		t.Fatal("delivered while down")
	}
	if f.Lag() != 300 {
		t.Fatalf("lag=%d", f.Lag())
	}

	s.down.Store(false)
	put(t, l, 300, 50) // live traffic keeps arriving during the drain
	waitFor(t, "drain", func() bool { return s.count() == 350 })
	waitFor(t, "lag zero", func() bool { return f.Lag() == 0 })

	cancel()
	<-done

	s.mu.Lock()
	defer s.mu.Unlock()
	for i, v := range s.got {
		if v != float64(i) {
			t.Fatalf("order broken at %d: %v", i, v)
		}
	}
	// slow start: first batch after recovery is the probe size
	if s.batches[0] != 4 {
		t.Fatalf("first batch %d, want probe of 4", s.batches[0])
	}
}

func TestRestartResumesFromCheckpoint(t *testing.T) {
	dir := t.TempDir()
	l, _ := wal.Open(dir, wal.Options{})
	s := &fakeSink{}

	f := New(l, s, OpenCheckpoint(dir), fastCfg())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { f.Run(ctx); close(done) }()
	put(t, l, 0, 40)
	waitFor(t, "first run", func() bool { return s.count() == 40 })
	cancel()
	<-done
	l.Close()

	// "reboot"
	l2, _ := wal.Open(dir, wal.Options{})
	defer l2.Close()
	put(t, l2, 40, 10)
	s2 := &fakeSink{}
	f2 := New(l2, s2, OpenCheckpoint(dir), fastCfg())
	if f2.Committed() != 40 {
		t.Fatalf("committed=%d", f2.Committed())
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go f2.Run(ctx2)
	waitFor(t, "second run", func() bool { return s2.count() == 10 })
	s2.mu.Lock()
	if s2.got[0] != 40 {
		t.Fatalf("resent old data, first=%v", s2.got[0])
	}
	s2.mu.Unlock()
}

func TestPermanentErrorIsSkipped(t *testing.T) {
	dir := t.TempDir()
	l, _ := wal.Open(dir, wal.Options{})
	defer l.Close()
	s := &fakeSink{}
	s.perm.Store(true)
	f := New(l, s, OpenCheckpoint(dir), fastCfg())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.Run(ctx)

	put(t, l, 0, 10)
	waitFor(t, "dead letter", func() bool { return f.DeadLettered.Load() == 10 })

	s.perm.Store(false)
	put(t, l, 10, 5)
	waitFor(t, "recover", func() bool { return s.count() == 5 })
}

func TestCheckpointRoundTrip(t *testing.T) {
	cp := OpenCheckpoint(t.TempDir())
	if cp.Load() != 0 {
		t.Fatal("fresh checkpoint should be 0")
	}
	if err := cp.Store(12345); err != nil {
		t.Fatal(err)
	}
	if cp.Load() != 12345 {
		t.Fatal("lost value")
	}
}
