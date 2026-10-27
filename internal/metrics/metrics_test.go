package metrics

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestBucketMonotonic(t *testing.T) {
	prev := -1
	for us := uint64(0); us < 5_000_000; us += 7 {
		b := bucketOf(us)
		if b < prev {
			t.Fatalf("bucket went backwards at %d", us)
		}
		prev = b
		if us >= bucketUpper(b) {
			t.Fatalf("%d not under upper bound %d of bucket %d", us, bucketUpper(b), b)
		}
	}
}

func TestQuantiles(t *testing.T) {
	var h Histogram
	for i := 1; i <= 1000; i++ {
		h.Observe(time.Duration(i) * time.Millisecond)
	}
	p50 := h.Quantile(0.5)
	p99 := h.Quantile(0.99)
	if p50 < 450*time.Millisecond || p50 > 620*time.Millisecond {
		t.Fatalf("p50=%v", p50)
	}
	if p99 < 950*time.Millisecond || p99 > 1100*time.Millisecond {
		t.Fatalf("p99=%v", p99)
	}
	if h.Max() != time.Second || h.Count() != 1000 {
		t.Fatalf("max=%v count=%d", h.Max(), h.Count())
	}
}

func TestHistogramConcurrent(t *testing.T) {
	var h Histogram
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 10000 {
				h.Observe(time.Duration(i) * time.Microsecond)
			}
		}()
	}
	wg.Wait()
	if h.Count() != 80000 {
		t.Fatalf("count=%d", h.Count())
	}
}

func TestExposition(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("x_total", "things")
	c.Add(3)
	r.Gauge("y", "g").Set(-2)
	r.Histogram("z_seconds", "lat").Observe(2 * time.Millisecond)
	r.RegisterRuntime()
	var b bytes.Buffer
	r.Expose(&b)
	out := b.String()
	for _, want := range []string{"x_total 3", "y -2", `z_seconds{quantile="0.99"}`, "z_seconds_count 1", "process_heap_alloc_bytes"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q", want)
		}
	}
}

func BenchmarkObserve(b *testing.B) {
	var h Histogram
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			h.Observe(123 * time.Microsecond)
		}
	})
}
