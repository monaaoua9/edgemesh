package metrics

import (
	"math/bits"
	"sync/atomic"
	"time"
)

const nBuckets = 256

// Histogram is lock-free: log-linear buckets (4 per power of two, ~12% error)
// over microseconds. Observe is a couple of atomic adds, fine for the hot path.
type Histogram struct {
	b     [nBuckets]atomic.Uint64
	count atomic.Uint64
	sumUs atomic.Uint64
	maxUs atomic.Uint64
}

func bucketOf(us uint64) int {
	if us < 4 {
		return int(us)
	}
	e := bits.Len64(us) - 1
	sub := (us >> (e - 2)) & 3
	return 4 + (e-2)*4 + int(sub)
}

func bucketUpper(i int) uint64 {
	if i < 4 {
		return uint64(i) + 1
	}
	e := (i-4)/4 + 2
	sub := uint64((i - 4) % 4)
	return (4 + sub + 1) << (e - 2)
}

func (h *Histogram) Observe(d time.Duration) {
	us := uint64(0)
	if d > 0 {
		us = uint64(d / time.Microsecond)
	}
	h.b[bucketOf(us)].Add(1)
	h.count.Add(1)
	h.sumUs.Add(us)
	for {
		m := h.maxUs.Load()
		if us <= m || h.maxUs.CompareAndSwap(m, us) {
			break
		}
	}
}

// ObserveValue records a plain number (not a duration), for size distributions.
func (h *Histogram) ObserveValue(v uint64) { h.Observe(time.Duration(v) * time.Microsecond) }

func (h *Histogram) Count() uint64 { return h.count.Load() }

func (h *Histogram) Sum() time.Duration {
	return time.Duration(h.sumUs.Load()) * time.Microsecond
}

func (h *Histogram) Max() time.Duration {
	return time.Duration(h.maxUs.Load()) * time.Microsecond
}

func (h *Histogram) Mean() time.Duration {
	n := h.count.Load()
	if n == 0 {
		return 0
	}
	return time.Duration(h.sumUs.Load()/n) * time.Microsecond
}

// Quantile returns the bucket upper bound, so it errs on the high side.
func (h *Histogram) Quantile(q float64) time.Duration {
	total := h.count.Load()
	if total == 0 {
		return 0
	}
	rank := uint64(q*float64(total) + 0.5)
	if rank < 1 {
		rank = 1
	}
	var seen uint64
	for i := range h.b {
		seen += h.b[i].Load()
		if seen >= rank {
			up := time.Duration(bucketUpper(i)) * time.Microsecond
			if m := h.Max(); up > m && m > 0 {
				return m
			}
			return up
		}
	}
	return h.Max()
}
