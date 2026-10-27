package fleet

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newReg() (*Registry, *clock) {
	c := &clock{t: time.Unix(1_700_000_000, 0)}
	return NewRegistry(Config{SuspectAfter: 10 * time.Second, OfflineAfter: 30 * time.Second, Now: c.now}), c
}

func TestStateMachine(t *testing.T) {
	r, c := newReg()
	var evs []Event
	r.OnChange(func(e Event) { evs = append(evs, e) })

	r.Touch("a")
	c.advance(5 * time.Second)
	if len(r.Sweep()) != 0 {
		t.Fatal("5s of silence is fine")
	}
	c.advance(6 * time.Second)
	if got := r.Sweep(); len(got) != 1 || got[0].To != Suspect {
		t.Fatalf("want suspect, got %+v", got)
	}
	c.advance(20 * time.Second)
	if got := r.Sweep(); len(got) != 1 || got[0].To != Offline {
		t.Fatalf("want offline, got %+v", got)
	}
	if got := r.Sweep(); len(got) != 0 {
		t.Fatal("no repeat events")
	}

	r.Touch("a")
	d, _ := r.Get("a")
	if d.State != Online || d.Messages != 2 {
		t.Fatalf("%+v", d)
	}
	// new -> online, online -> suspect, suspect -> offline, offline -> online
	if len(evs) != 4 {
		t.Fatalf("events: %+v", evs)
	}
}

func TestCountsAndSnapshot(t *testing.T) {
	r, c := newReg()
	r.Touch("a")
	r.Touch("b")
	c.advance(40 * time.Second)
	r.Sweep()
	r.Touch("c")
	on, sus, off := r.Counts()
	if on != 1 || sus != 0 || off != 2 {
		t.Fatalf("%d %d %d", on, sus, off)
	}
	st := Offline
	if got := r.Snapshot(0, &st); len(got) != 2 || got[0].ID != "a" {
		t.Fatalf("%+v", got)
	}
}

func TestConcurrentTouch(t *testing.T) {
	r, _ := newReg()
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 500 {
				r.Touch(fmt.Sprintf("d%d", (g*7+i)%100))
				if i%50 == 0 {
					r.Sweep()
				}
			}
		}()
	}
	wg.Wait()
	if on, _, _ := r.Counts(); on != 100 {
		t.Fatalf("online=%d", on)
	}
}

type recorder struct {
	mu   sync.Mutex
	sent map[string][]string
	bad  map[string]bool
}

func (rc *recorder) dispatch(dev string, t Task) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if rc.bad[dev] {
		return errors.New("not subscribed")
	}
	if rc.sent == nil {
		rc.sent = map[string][]string{}
	}
	rc.sent[dev] = append(rc.sent[dev], t.ID)
	return nil
}

func TestSchedulerBalancesAndReassigns(t *testing.T) {
	r, c := newReg()
	rc := &recorder{}
	s := NewScheduler(r, rc.dispatch)
	for _, id := range []string{"a", "b", "c"} {
		r.Touch(id)
	}

	for i := range 6 {
		if _, err := s.Submit(Task{ID: fmt.Sprintf("t%d", i), Kind: "reboot"}); err != nil {
			t.Fatal(err)
		}
	}
	load := map[string]int{}
	for _, tk := range s.List() {
		load[tk.Assignee]++
	}
	if load["a"] != 2 || load["b"] != 2 || load["c"] != 2 {
		t.Fatalf("uneven: %v", load)
	}

	// a, b stay alive; c goes quiet
	c.advance(35 * time.Second)
	r.Touch("a")
	r.Touch("b")
	r.Sweep()

	load = map[string]int{}
	for _, tk := range s.List() {
		if tk.Assignee == "c" || tk.Assignee == "" {
			t.Fatalf("task left on dead device: %+v", tk)
		}
		load[tk.Assignee]++
	}
	if load["a"] != 3 || load["b"] != 3 {
		t.Fatalf("not rebalanced: %v", load)
	}
}

func TestPendingWhenNobodyOnline(t *testing.T) {
	r, _ := newReg()
	rc := &recorder{}
	s := NewScheduler(r, rc.dispatch)

	if _, err := s.Submit(Task{ID: "x"}); !errors.Is(err, ErrNoDevice) {
		t.Fatalf("got %v", err)
	}
	if s.Pending() != 1 {
		t.Fatal("task should wait")
	}
	r.Touch("a") // capacity appears
	if s.Pending() != 0 || s.List()[0].Assignee != "a" {
		t.Fatalf("not picked up: %+v", s.List())
	}
	if _, err := s.Submit(Task{ID: "x"}); !errors.Is(err, ErrDuplicate) {
		t.Fatal("dup should fail")
	}
	if err := s.Done("x"); err != nil {
		t.Fatal(err)
	}
	if err := s.Done("x"); !errors.Is(err, ErrUnknown) {
		t.Fatal("second done should fail")
	}
}

func TestDispatchFailureFallsThrough(t *testing.T) {
	r, _ := newReg()
	rc := &recorder{bad: map[string]bool{"a": true}}
	s := NewScheduler(r, rc.dispatch)
	r.Touch("a")
	r.Touch("b")
	tk, err := s.Submit(Task{ID: "t"})
	if err != nil || tk.Assignee != "b" {
		t.Fatalf("%+v %v", tk, err)
	}
}
