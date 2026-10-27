package fleet

import (
	"context"
	"hash/fnv"
	"sort"
	"sync"
	"time"
)

type State uint8

const (
	Online State = iota
	Suspect
	Offline
)

func (s State) String() string {
	return [...]string{"online", "suspect", "offline"}[s]
}

func (s State) MarshalText() ([]byte, error) { return []byte(s.String()), nil }

type Device struct {
	ID        string    `json:"id"`
	State     State     `json:"state"`
	FirstSeen time.Time `json:"first_seen"`
	LastSeen  time.Time `json:"last_seen"`
	Messages  uint64    `json:"messages"`
}

type Event struct {
	DeviceID string
	From, To State
	At       time.Time
}

type Config struct {
	SuspectAfter time.Duration
	OfflineAfter time.Duration
	SweepEvery   time.Duration
	Now          func() time.Time
}

func (c *Config) defaults() {
	if c.SuspectAfter <= 0 {
		c.SuspectAfter = 15 * time.Second
	}
	if c.OfflineAfter <= c.SuspectAfter {
		c.OfflineAfter = 2 * c.SuspectAfter
	}
	if c.SweepEvery <= 0 {
		c.SweepEvery = time.Second
	}
	if c.Now == nil {
		c.Now = time.Now
	}
}

const nShards = 64

type shard struct {
	mu sync.Mutex
	m  map[string]*Device
}

type Registry struct {
	cfg    Config
	shards [nShards]shard

	lmu       sync.RWMutex
	listeners []func(Event)
}

func NewRegistry(cfg Config) *Registry {
	cfg.defaults()
	r := &Registry{cfg: cfg}
	for i := range r.shards {
		r.shards[i].m = make(map[string]*Device)
	}
	return r
}

// listeners run on the sweeper / caller goroutine, keep them quick
func (r *Registry) OnChange(fn func(Event)) {
	r.lmu.Lock()
	r.listeners = append(r.listeners, fn)
	r.lmu.Unlock()
}

func (r *Registry) emit(evs []Event) {
	if len(evs) == 0 {
		return
	}
	r.lmu.RLock()
	ls := r.listeners
	r.lmu.RUnlock()
	for _, e := range evs {
		for _, fn := range ls {
			fn(e)
		}
	}
}

func (r *Registry) shardFor(id string) *shard {
	h := fnv.New32a()
	h.Write([]byte(id))
	return &r.shards[h.Sum32()%nShards]
}

// Touch is called for every message (telemetry counts as a heartbeat too).
func (r *Registry) Touch(id string) {
	now := r.cfg.Now()
	sh := r.shardFor(id)

	sh.mu.Lock()
	d := sh.m[id]
	var ev *Event
	if d == nil {
		d = &Device{ID: id, FirstSeen: now}
		sh.m[id] = d
		ev = &Event{DeviceID: id, From: Offline, To: Online, At: now}
	} else if d.State != Online {
		ev = &Event{DeviceID: id, From: d.State, To: Online, At: now}
		d.State = Online
	}
	d.LastSeen = now
	d.Messages++
	sh.mu.Unlock()

	if ev != nil {
		r.emit([]Event{*ev})
	}
}

// Sweep demotes silent devices and returns what changed.
func (r *Registry) Sweep() []Event {
	now := r.cfg.Now()
	var evs []Event
	for i := range r.shards {
		sh := &r.shards[i]
		sh.mu.Lock()
		for _, d := range sh.m {
			silent := now.Sub(d.LastSeen)
			want := Online
			switch {
			case silent >= r.cfg.OfflineAfter:
				want = Offline
			case silent >= r.cfg.SuspectAfter:
				want = Suspect
			}
			if want != d.State && want > d.State {
				evs = append(evs, Event{DeviceID: d.ID, From: d.State, To: want, At: now})
				d.State = want
			}
		}
		sh.mu.Unlock()
	}
	r.emit(evs)
	return evs
}

func (r *Registry) Run(ctx context.Context) {
	t := time.NewTicker(r.cfg.SweepEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.Sweep()
		}
	}
}

func (r *Registry) Get(id string) (Device, bool) {
	sh := r.shardFor(id)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	d, ok := sh.m[id]
	if !ok {
		return Device{}, false
	}
	return *d, true
}

func (r *Registry) Counts() (on, suspect, off int) {
	for i := range r.shards {
		sh := &r.shards[i]
		sh.mu.Lock()
		for _, d := range sh.m {
			switch d.State {
			case Online:
				on++
			case Suspect:
				suspect++
			default:
				off++
			}
		}
		sh.mu.Unlock()
	}
	return
}

// OnlineIDs is sorted so scheduling decisions are deterministic.
func (r *Registry) OnlineIDs() []string {
	var ids []string
	for i := range r.shards {
		sh := &r.shards[i]
		sh.mu.Lock()
		for id, d := range sh.m {
			if d.State == Online {
				ids = append(ids, id)
			}
		}
		sh.mu.Unlock()
	}
	sort.Strings(ids)
	return ids
}

// Snapshot returns up to limit devices, optionally only in the given state.
func (r *Registry) Snapshot(limit int, only *State) []Device {
	var out []Device
	for i := range r.shards {
		sh := &r.shards[i]
		sh.mu.Lock()
		for _, d := range sh.m {
			if only == nil || d.State == *only {
				out = append(out, *d)
			}
		}
		sh.mu.Unlock()
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}
