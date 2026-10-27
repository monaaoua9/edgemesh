// Package ingest connects the protocol layer to storage: readings are
// sharded by device onto workers, scored by the anomaly detector, and
// group-committed to the WAL. A QoS1 publish is only acked once its batch hit
// the log.
package ingest

import (
	"context"
	"errors"
	"hash/fnv"
	"log/slog"
	"sync"
	"time"

	"edgemesh/internal/anomaly"
	"edgemesh/internal/metrics"
	"edgemesh/internal/telemetry"
	"edgemesh/internal/wal"
)

var (
	ErrOverloaded = errors.New("ingest: queue full")
	ErrClosed     = errors.New("ingest: closed")
)

type Config struct {
	Workers        int
	QueueDepth     int
	MaxBatch       int           // max readings per WAL write
	EnqueueTimeout time.Duration // how long a QoS1 publish may wait for queue space
	Logger         *slog.Logger
}

func (c *Config) defaults() {
	if c.Workers <= 0 {
		c.Workers = 4
	}
	if c.QueueDepth <= 0 {
		c.QueueDepth = 4096
	}
	if c.MaxBatch <= 0 {
		c.MaxBatch = 256
	}
	if c.EnqueueTimeout <= 0 {
		c.EnqueueTimeout = 250 * time.Millisecond
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

type job struct {
	r    telemetry.Reading
	t0   time.Time // monotonic; wall-clock micros are too coarse on windows
	wait bool
	done chan error
}

var jobPool = sync.Pool{New: func() any { return &job{done: make(chan error, 1)} }}

type Metrics struct {
	Accepted   *metrics.Counter
	Dropped    *metrics.Counter // QoS0 shed under load
	Rejected   *metrics.Counter // QoS1 that never got queue space
	WALErrors  *metrics.Counter
	Anomalies  *metrics.Counter
	AlertsLost *metrics.Counter
	Latency    *metrics.Histogram // receive -> durable in WAL
	BatchSize  *metrics.Histogram
}

func NewMetrics(reg *metrics.Registry) *Metrics {
	return &Metrics{
		Accepted:   reg.Counter("edge_readings_accepted_total", "readings written to the WAL"),
		Dropped:    reg.Counter("edge_readings_dropped_total", "QoS0 readings shed because the queue was full"),
		Rejected:   reg.Counter("edge_readings_rejected_total", "QoS1 readings refused (no ack sent)"),
		WALErrors:  reg.Counter("edge_wal_errors_total", "failed WAL appends"),
		Anomalies:  reg.Counter("edge_anomalies_total", "readings flagged anomalous"),
		AlertsLost: reg.Counter("edge_alerts_dropped_total", "alerts the consumer could not take"),
		Latency:    reg.Histogram("edge_ingest_latency_seconds", "time from receipt to durable in the WAL"),
		BatchSize:  reg.Distribution("edge_wal_batch_records", "records per WAL group commit"),
	}
}

type Pipeline struct {
	cfg     Config
	log     *wal.Log
	det     *anomaly.Detector
	onAlert func(anomaly.Alert)
	m       *Metrics
	lg      *slog.Logger

	mu     sync.RWMutex
	closed bool
	qs     []chan *job
	wg     sync.WaitGroup
}

// onAlert runs on the worker goroutine, it must not block.
func NewPipeline(cfg Config, l *wal.Log, det *anomaly.Detector, onAlert func(anomaly.Alert), m *Metrics) *Pipeline {
	cfg.defaults()
	p := &Pipeline{cfg: cfg, log: l, det: det, onAlert: onAlert, m: m, lg: cfg.Logger.With("component", "ingest")}
	for range cfg.Workers {
		q := make(chan *job, cfg.QueueDepth/cfg.Workers+1)
		p.qs = append(p.qs, q)
		p.wg.Add(1)
		go p.worker(q)
	}
	return p
}

func (p *Pipeline) QueueDepth() int {
	n := 0
	for _, q := range p.qs {
		n += len(q)
	}
	return n
}

func (p *Pipeline) shard(dev string) chan *job {
	h := fnv.New32a()
	h.Write([]byte(dev))
	return p.qs[h.Sum32()%uint32(len(p.qs))]
}

// Submit queues r. With wait=true it blocks until the reading is durable and
// returns the WAL error, if any; with wait=false it is fire and forget and may
// be shed under load.
func (p *Pipeline) Submit(ctx context.Context, r telemetry.Reading, wait bool) error {
	j, err := p.enqueue(ctx, r, wait)
	if err != nil || j == nil {
		return err
	}
	return p.await(ctx, j)
}

// SubmitMany queues every reading first and then waits for all of them, so a
// batch costs roughly one group commit instead of len(rs) serial ones. It
// returns how many are durable. Everything is accepted or the first error comes back.
func (p *Pipeline) SubmitMany(ctx context.Context, rs []telemetry.Reading) (int, error) {
	jobs := make([]*job, 0, len(rs))
	var first error
	for _, r := range rs {
		j, err := p.enqueue(ctx, r, true)
		if err != nil {
			first = err
			break
		}
		jobs = append(jobs, j)
	}
	ok := 0
	for _, j := range jobs {
		if err := p.await(ctx, j); err != nil {
			if first == nil {
				first = err
			}
			continue
		}
		ok++
	}
	return ok, first
}

// returns a nil job for fire-and-forget submissions
func (p *Pipeline) enqueue(ctx context.Context, r telemetry.Reading, wait bool) (*job, error) {
	j := jobPool.Get().(*job)
	j.r, j.wait, j.t0 = r, wait, time.Now()

	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		jobPool.Put(j)
		return nil, ErrClosed
	}
	q := p.shard(r.DeviceID)
	if !wait {
		select {
		case q <- j:
			p.mu.RUnlock()
			return nil, nil
		default:
			p.mu.RUnlock()
			p.m.Dropped.Inc()
			jobPool.Put(j)
			return nil, ErrOverloaded
		}
	}

	t := time.NewTimer(p.cfg.EnqueueTimeout)
	defer t.Stop()
	select {
	case q <- j:
		p.mu.RUnlock()
		return j, nil
	case <-t.C:
		p.mu.RUnlock()
		p.m.Rejected.Inc()
		jobPool.Put(j)
		return nil, ErrOverloaded
	case <-ctx.Done():
		p.mu.RUnlock()
		jobPool.Put(j)
		return nil, ctx.Err()
	}
}

func (p *Pipeline) await(ctx context.Context, j *job) error {
	select {
	case err := <-j.done:
		jobPool.Put(j)
		return err
	case <-ctx.Done():
		// worker still owns j and will write to done (cap 1); let the GC have it
		return ctx.Err()
	}
}

// Close stops intake and waits for everything already queued to be flushed.
func (p *Pipeline) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	for _, q := range p.qs {
		close(q)
	}
	p.mu.Unlock()
	p.wg.Wait()
}

func (p *Pipeline) worker(q chan *job) {
	defer p.wg.Done()
	batch := make([]*job, 0, p.cfg.MaxBatch)
	arena := make([]byte, 0, p.cfg.MaxBatch*48)
	recs := make([][]byte, 0, p.cfg.MaxBatch)
	valid := make([]*job, 0, p.cfg.MaxBatch)

	for first := range q {
		batch = append(batch[:0], first)
	fill:
		for len(batch) < p.cfg.MaxBatch {
			select {
			case j, ok := <-q:
				if !ok {
					break fill
				}
				batch = append(batch, j)
			default:
				break fill
			}
		}

		// arena can't be reallocated mid-loop or earlier slices would point at stale memory,
		// so size it up front
		need := len(batch) * (26 + 2 + 2*255)
		if cap(arena) < need {
			arena = make([]byte, 0, need)
		}
		arena, recs, valid = arena[:0], recs[:0], valid[:0]
		for _, j := range batch {
			if fire := p.score(&j.r); fire != nil {
				p.alert(*fire)
			}
			start := len(arena)
			var err error
			if arena, err = j.r.AppendBinary(arena); err != nil {
				p.finish(j, err)
				continue
			}
			recs = append(recs, arena[start:])
			valid = append(valid, j)
		}
		if len(recs) == 0 {
			continue
		}

		_, err := p.log.AppendBatch(recs)
		now := time.Now()
		p.m.BatchSize.ObserveValue(uint64(len(recs)))
		if err != nil {
			p.m.WALErrors.Inc()
			p.lg.Error("wal append failed", "err", err, "records", len(recs))
		}
		for _, j := range valid {
			if err == nil {
				p.m.Accepted.Inc()
				p.m.Latency.Observe(now.Sub(j.t0))
			}
			p.finish(j, err)
		}
	}
}

func (p *Pipeline) score(r *telemetry.Reading) *anomaly.Alert {
	a, fire := p.det.Observe(r)
	if r.Anomalous() {
		p.m.Anomalies.Inc()
	}
	if fire {
		return &a
	}
	return nil
}

func (p *Pipeline) alert(a anomaly.Alert) {
	if p.onAlert != nil {
		p.onAlert(a)
	}
}

func (p *Pipeline) finish(j *job, err error) {
	if j.wait {
		j.done <- err
		return
	}
	jobPool.Put(j)
}
