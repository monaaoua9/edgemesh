// Package forward drains the WAL to the cloud sink. It owns the "store and
// forward" half of the gateway: ack to devices comes from the WAL write, this
// loop is what moves bytes upstream whenever the network allows it.
package forward

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"edgemesh/internal/sink"
	"edgemesh/internal/telemetry"
	"edgemesh/internal/wal"
)

type State int32

const (
	StateIdle State = iota
	StateOnline
	StateOffline
)

func (s State) String() string {
	switch s {
	case StateOnline:
		return "online"
	case StateOffline:
		return "offline"
	}
	return "idle"
}

type Config struct {
	GatewayID  string
	MinBatch   int // batch size right after a failure (probe)
	MaxBatch   int
	Linger     time.Duration // wait this long for a batch to fill
	BackoffMin time.Duration
	BackoffMax time.Duration
	MaxRate    int // records/s while catching up, 0 = as fast as possible
	Logger     *slog.Logger
}

func (c *Config) defaults() {
	if c.MinBatch <= 0 {
		c.MinBatch = 16
	}
	if c.MaxBatch < c.MinBatch {
		c.MaxBatch = 2048
	}
	if c.Linger <= 0 {
		c.Linger = 50 * time.Millisecond
	}
	if c.BackoffMin <= 0 {
		c.BackoffMin = 250 * time.Millisecond
	}
	if c.BackoffMax < c.BackoffMin {
		c.BackoffMax = 30 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
}

type Forwarder struct {
	log  *wal.Log
	sink sink.Sink
	cp   *Checkpoint
	cfg  Config
	lg   *slog.Logger

	state     atomic.Int32
	committed atomic.Uint64
	curBatch  atomic.Int64

	Sent         atomic.Uint64 // readings delivered
	Batches      atomic.Uint64
	Failures     atomic.Uint64 // failed send attempts
	DeadLettered atomic.Uint64
	DecodeErrors atomic.Uint64
	LastError    atomic.Value // string
}

func New(l *wal.Log, s sink.Sink, cp *Checkpoint, cfg Config) *Forwarder {
	cfg.defaults()
	f := &Forwarder{log: l, sink: s, cp: cp, cfg: cfg, lg: cfg.Logger.With("component", "forwarder")}
	f.committed.Store(cp.Load())
	f.curBatch.Store(int64(cfg.MinBatch))
	return f
}

func (f *Forwarder) State() State { return State(f.state.Load()) }

// Lag is how many records are on disk but not yet acknowledged upstream.
func (f *Forwarder) Lag() uint64 {
	next := f.log.Stats().NextIndex
	c := f.committed.Load()
	if c > next {
		return 0
	}
	return next - c
}

func (f *Forwarder) Committed() uint64 { return f.committed.Load() }
func (f *Forwarder) BatchSize() int    { return int(f.curBatch.Load()) }

type pending struct {
	first, last uint64
	readings    []telemetry.Reading
}

func (f *Forwarder) Run(ctx context.Context) error {
	r := f.log.NewReader(f.committed.Load())
	defer r.Close()

	size := f.cfg.MinBatch
	backoff := f.cfg.BackoffMin

	for ctx.Err() == nil {
		b, err := f.collect(ctx, r, size)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if b == nil {
			continue
		}

		for {
			start := time.Now()
			err := f.sink.Send(ctx, sink.Batch{GatewayID: f.cfg.GatewayID, First: b.first, Last: b.last, Readings: b.readings})
			if err == nil {
				f.state.Store(int32(StateOnline))
				f.Sent.Add(uint64(len(b.readings)))
				f.Batches.Add(1)
				f.commit(b.last + 1)
				// slow start: double until we hit the cap
				size = min(size*2, f.cfg.MaxBatch)
				backoff = f.cfg.BackoffMin
				f.curBatch.Store(int64(size))
				f.throttle(ctx, len(b.readings), time.Since(start))
				break
			}
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, sink.ErrPermanent) {
				f.lg.Error("dropping batch the sink refuses", "first", b.first, "last", b.last, "err", err)
				f.DeadLettered.Add(uint64(len(b.readings)))
				f.commit(b.last + 1)
				break
			}

			f.Failures.Add(1)
			f.LastError.Store(err.Error())
			if f.State() != StateOffline {
				f.lg.Warn("upstream unreachable, buffering to disk", "err", err)
			}
			f.state.Store(int32(StateOffline))
			size = f.cfg.MinBatch
			f.curBatch.Store(int64(size))

			if !sleep(ctx, jitter(backoff)) {
				return nil
			}
			backoff = min(backoff*2, f.cfg.BackoffMax)
		}
	}
	return nil
}

func (f *Forwarder) commit(next uint64) {
	if err := f.cp.Store(next); err != nil {
		// worst case we resend a few batches after a crash
		f.lg.Error("checkpoint write failed", "err", err)
	}
	f.committed.Store(next)
	f.log.TruncateBefore(next)
}

// pull up to n records; nil batch means nothing arrived (caller loops)
func (f *Forwarder) collect(ctx context.Context, r *wal.Reader, n int) (*pending, error) {
	var b *pending
	var deadline time.Time

	for {
		idx, p, err := r.Next()
		switch {
		case err == nil:
			if b == nil {
				b = &pending{first: idx, readings: make([]telemetry.Reading, 0, n)}
				deadline = time.Now().Add(f.cfg.Linger)
			}
			b.last = idx
			var rd telemetry.Reading
			if derr := rd.UnmarshalBinary(p); derr != nil {
				f.DecodeErrors.Add(1)
			} else {
				b.readings = append(b.readings, rd)
			}
			if len(b.readings) >= n {
				return b, nil
			}
			continue

		case errors.Is(err, wal.ErrNoData):
			wait := time.Second
			if b != nil {
				wait = time.Until(deadline)
				if wait <= 0 {
					return b.nonEmpty(f), nil
				}
			}
			t := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				t.Stop()
				return nil, ctx.Err()
			case <-f.log.Notify():
			case <-t.C:
			}
			t.Stop()

		case errors.Is(err, wal.ErrCorrupt):
			f.lg.Error("corrupt wal record, skipping rest of segment")
			r.SkipCorrupt()

		default:
			return nil, err
		}
	}
}

// a batch made only of undecodable records still has to advance the checkpoint
func (b *pending) nonEmpty(f *Forwarder) *pending {
	if len(b.readings) == 0 {
		f.commit(b.last + 1)
		return nil
	}
	return b
}

// during catch-up don't hammer the cloud the instant it comes back
func (f *Forwarder) throttle(ctx context.Context, n int, took time.Duration) {
	if f.cfg.MaxRate <= 0 || f.Lag() == 0 {
		return
	}
	want := time.Duration(float64(n) / float64(f.cfg.MaxRate) * float64(time.Second))
	if want > took {
		sleep(ctx, want-took)
	}
}

// somewhere in [d/2, 1.5d) so a fleet of gateways does not retry in lockstep
func jitter(d time.Duration) time.Duration {
	return time.Duration(rand.Int64N(int64(d)) + int64(d)/2)
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
