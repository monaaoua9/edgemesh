package metrics

import (
	"fmt"
	"io"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Counter struct{ v atomic.Uint64 }

func (c *Counter) Inc()          { c.v.Add(1) }
func (c *Counter) Add(n uint64)  { c.v.Add(n) }
func (c *Counter) Value() uint64 { return c.v.Load() }

type Gauge struct{ v atomic.Int64 }

func (g *Gauge) Set(n int64)  { g.v.Store(n) }
func (g *Gauge) Add(n int64)  { g.v.Add(n) }
func (g *Gauge) Value() int64 { return g.v.Load() }

type entry struct {
	name, help, kind string
	write            func(w io.Writer, name string)
}

type Registry struct {
	mu      sync.Mutex
	entries []entry
}

func NewRegistry() *Registry { return &Registry{} }

func (r *Registry) add(e entry) {
	r.mu.Lock()
	r.entries = append(r.entries, e)
	r.mu.Unlock()
}

func (r *Registry) Counter(name, help string) *Counter {
	c := &Counter{}
	r.add(entry{name, help, "counter", func(w io.Writer, n string) { fmt.Fprintf(w, "%s %d\n", n, c.Value()) }})
	return c
}

func (r *Registry) Gauge(name, help string) *Gauge {
	g := &Gauge{}
	r.add(entry{name, help, "gauge", func(w io.Writer, n string) { fmt.Fprintf(w, "%s %d\n", n, g.Value()) }})
	return g
}

func (r *Registry) GaugeFunc(name, help string, fn func() float64) {
	r.add(entry{name, help, "gauge", func(w io.Writer, n string) { fmt.Fprintf(w, "%s %g\n", n, fn()) }})
}

func (r *Registry) CounterFunc(name, help string, fn func() float64) {
	r.add(entry{name, help, "counter", func(w io.Writer, n string) { fmt.Fprintf(w, "%s %g\n", n, fn()) }})
}

// exposed as a prometheus summary; values in seconds
func (r *Registry) Histogram(name, help string) *Histogram {
	h := &Histogram{}
	r.add(entry{name, help, "summary", func(w io.Writer, n string) {
		for _, q := range []float64{0.5, 0.9, 0.99, 0.999} {
			fmt.Fprintf(w, "%s{quantile=\"%g\"} %g\n", n, q, h.Quantile(q).Seconds())
		}
		fmt.Fprintf(w, "%s_sum %g\n%s_count %d\n", n, h.Sum().Seconds(), n, h.Count())
		fmt.Fprintf(w, "%s_max %g\n", n, h.Max().Seconds())
	}})
	return h
}

func (r *Registry) Expose(w io.Writer) {
	r.mu.Lock()
	es := append([]entry(nil), r.entries...)
	r.mu.Unlock()
	sort.Slice(es, func(i, j int) bool { return es[i].name < es[j].name })
	for _, e := range es {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", e.name, e.help, e.name, e.kind)
		e.write(w, e.name)
	}
}

// Distribution is a histogram of plain numbers (batch sizes etc), via ObserveValue.
func (r *Registry) Distribution(name, help string) *Histogram {
	h := &Histogram{}
	r.add(entry{name, help, "summary", func(w io.Writer, n string) {
		for _, q := range []float64{0.5, 0.9, 0.99} {
			fmt.Fprintf(w, "%s{quantile=\"%g\"} %d\n", n, q, h.Quantile(q)/time.Microsecond)
		}
		fmt.Fprintf(w, "%s_sum %d\n%s_count %d\n", n, h.Sum()/time.Microsecond, n, h.Count())
	}})
	return h
}

// ReadMemStats stops the world briefly, so scrapes share one cached sample.
type memSampler struct {
	mu   sync.Mutex
	at   time.Time
	last runtime.MemStats
}

func (m *memSampler) get() runtime.MemStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	if time.Since(m.at) > time.Second {
		runtime.ReadMemStats(&m.last)
		m.at = time.Now()
	}
	return m.last
}

func (r *Registry) RegisterRuntime() {
	m := &memSampler{}
	r.GaugeFunc("process_heap_alloc_bytes", "live heap bytes", func() float64 { s := m.get(); return float64(s.HeapAlloc) })
	r.GaugeFunc("process_heap_inuse_bytes", "heap spans in use", func() float64 { s := m.get(); return float64(s.HeapInuse) })
	r.GaugeFunc("process_stack_inuse_bytes", "goroutine stack bytes", func() float64 { s := m.get(); return float64(s.StackInuse) })
	r.GaugeFunc("process_sys_bytes", "bytes obtained from the OS", func() float64 { s := m.get(); return float64(s.Sys) })
	r.GaugeFunc("process_goroutines", "goroutine count", func() float64 { return float64(runtime.NumGoroutine()) })
	r.CounterFunc("process_gc_cycles_total", "completed GC cycles", func() float64 { s := m.get(); return float64(s.NumGC) })
	r.CounterFunc("process_gc_pause_seconds_total", "cumulative GC stop-the-world", func() float64 { s := m.get(); return float64(s.PauseTotalNs) / 1e9 })
	r.CounterFunc("process_alloc_bytes_total", "cumulative bytes allocated", func() float64 { s := m.get(); return float64(s.TotalAlloc) })
}
