package anomaly

import (
	"encoding/json"
	"hash/fnv"
	"math"
	"os"
	"sync"
	"time"

	"edgemesh/internal/telemetry"
)

type Kind string

const (
	KindThreshold Kind = "threshold"
	KindRate      Kind = "rate_of_change"
	KindZScore    Kind = "zscore"
)

type Alert struct {
	DeviceID string  `json:"device_id"`
	Metric   string  `json:"metric"`
	Kind     Kind    `json:"kind"`
	Value    float64 `json:"value"`
	Expected float64 `json:"expected"` // running mean at the time
	Score    float64 `json:"score"`    // z, rate, or distance past the limit
	TS       int64   `json:"ts_us"`
}

// nil Min/Max = unbounded on that side
type Rule struct {
	Min     *float64 `json:"min,omitempty"`
	Max     *float64 `json:"max,omitempty"`
	MaxRate *float64 `json:"max_rate_per_sec,omitempty"`
	Z       float64  `json:"z,omitempty"`
	MinStd  float64  `json:"min_std,omitempty"`
}

func fp(v float64) *float64 { return &v }

func DefaultRules() map[string]Rule {
	return map[string]Rule{
		"engine_temp": {Min: fp(-40), Max: fp(130), MaxRate: fp(12), Z: 4, MinStd: 0.5},
		"vibration":   {Max: fp(15), Z: 4.5, MinStd: 0.1},
		"pressure":    {Min: fp(0), Max: fp(12), MaxRate: fp(3), Z: 4, MinStd: 0.05},
		"rpm":         {Min: fp(0), Max: fp(7500), Z: 5, MinStd: 25},
	}
}

func LoadRules(path string) (map[string]Rule, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	m := map[string]Rule{}
	return m, json.Unmarshal(b, &m)
}

type Config struct {
	Rules    map[string]Rule
	Default  Rule          // for metrics with no rule
	Warmup   int           // samples before z-score is trusted
	Alpha    float64       // EWMA weight after warmup
	Cooldown time.Duration // min gap between alerts of one kind per series
	Streak   int           // this many anomalies in a row => treat as the new normal
}

func (c *Config) defaults() {
	if c.Rules == nil {
		c.Rules = DefaultRules()
	}
	if c.Default.Z == 0 {
		c.Default = Rule{Z: 5, MinStd: 0.01}
	}
	if c.Warmup <= 0 {
		c.Warmup = 30
	}
	if c.Alpha <= 0 {
		c.Alpha = 0.05
	}
	if c.Cooldown <= 0 {
		c.Cooldown = 5 * time.Second
	}
	if c.Streak <= 0 {
		c.Streak = 5
	}
}

type key struct{ dev, metric string }

type state struct {
	n         int
	mean, m2  float64 // m2 is the variance once past warmup
	last      float64
	lastTS    int64
	streak    int
	alertedAt map[Kind]int64
	seen      int64
}

const nShards = 64

type shard struct {
	mu sync.Mutex
	m  map[key]*state
}

type Detector struct {
	cfg    Config
	shards [nShards]shard
}

func New(cfg Config) *Detector {
	cfg.defaults()
	d := &Detector{cfg: cfg}
	for i := range d.shards {
		d.shards[i].m = make(map[key]*state)
	}
	return d
}

func (d *Detector) shardFor(dev string) *shard {
	h := fnv.New32a()
	h.Write([]byte(dev))
	return &d.shards[h.Sum32()%nShards]
}

// Observe updates the baseline for the series and flags r if it looks wrong.
// The returned alert is only meaningful when fire is true; during cooldown
// the reading is still flagged but we don't spam.
func (d *Detector) Observe(r *telemetry.Reading) (a Alert, fire bool) {
	rule, ok := d.cfg.Rules[r.Metric]
	if !ok {
		rule = d.cfg.Default
	}

	sh := d.shardFor(r.DeviceID)
	k := key{r.DeviceID, r.Metric}
	sh.mu.Lock()
	defer sh.mu.Unlock()

	st := sh.m[k]
	if st == nil {
		st = &state{alertedAt: make(map[Kind]int64, 3)}
		sh.m[k] = st
	}
	st.seen = r.RecvTS
	x := r.Value

	kind, score := d.check(st, &rule, x, r.TS)
	anomalous := kind != ""

	if anomalous {
		r.Flags |= telemetry.FlagAnomaly
		st.streak++
	} else {
		st.streak = 0
	}

	expected := st.mean
	switch {
	case !anomalous:
		d.learn(st, x)
	case st.streak >= d.cfg.Streak:
		// not a blip, the process really moved. re-center instead of alerting forever
		st.n, st.mean, st.m2, st.streak = 1, x, 0, 0
	}
	st.last, st.lastTS = x, r.TS

	if !anomalous {
		return Alert{}, false
	}
	if prev, ok := st.alertedAt[kind]; ok && r.TS-prev < d.cfg.Cooldown.Microseconds() {
		return Alert{}, false
	}
	st.alertedAt[kind] = r.TS
	return Alert{DeviceID: r.DeviceID, Metric: r.Metric, Kind: kind, Value: x, Expected: expected, Score: score, TS: r.TS}, true
}

func (d *Detector) check(st *state, rule *Rule, x float64, ts int64) (Kind, float64) {
	if rule.Max != nil && x > *rule.Max {
		return KindThreshold, x - *rule.Max
	}
	if rule.Min != nil && x < *rule.Min {
		return KindThreshold, *rule.Min - x
	}

	if rule.MaxRate != nil && st.n > 0 && ts > st.lastTS {
		dt := float64(ts-st.lastTS) / 1e6
		// tiny dt makes the rate meaningless (batched / duplicated samples)
		if dt >= 0.1 {
			if rate := math.Abs(x-st.last) / dt; rate > *rule.MaxRate {
				return KindRate, rate
			}
		}
	}

	if rule.Z > 0 && st.n >= d.cfg.Warmup {
		std := math.Max(math.Sqrt(st.m2), rule.MinStd)
		if std > 0 {
			if z := math.Abs(x-st.mean) / std; z > rule.Z {
				return KindZScore, z
			}
		}
	}
	return "", 0
}

// Welford while warming up, EWMA mean/variance afterwards so the baseline can drift
func (d *Detector) learn(st *state, x float64) {
	st.n++
	if st.n <= d.cfg.Warmup {
		delta := x - st.mean
		st.mean += delta / float64(st.n)
		st.m2 += delta * (x - st.mean)
		if st.n == d.cfg.Warmup {
			st.m2 /= float64(st.n - 1) // sum of squares -> variance
		}
		return
	}
	a := d.cfg.Alpha
	diff := x - st.mean
	st.mean += a * diff
	st.m2 = (1 - a) * (st.m2 + a*diff*diff)
}

// Sweep forgets series that haven't reported since cutoff (unix micros).
func (d *Detector) Sweep(cutoffUs int64) (removed int) {
	for i := range d.shards {
		sh := &d.shards[i]
		sh.mu.Lock()
		for k, st := range sh.m {
			if st.seen < cutoffUs {
				delete(sh.m, k)
				removed++
			}
		}
		sh.mu.Unlock()
	}
	return removed
}

func (d *Detector) Series() int {
	n := 0
	for i := range d.shards {
		d.shards[i].mu.Lock()
		n += len(d.shards[i].m)
		d.shards[i].mu.Unlock()
	}
	return n
}
