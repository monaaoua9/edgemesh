package fleet

import (
	"encoding/json"
	"errors"
	"sort"
	"sync"
)

var (
	ErrNoDevice  = errors.New("fleet: no online device accepted the task")
	ErrDuplicate = errors.New("fleet: task id already exists")
	ErrUnknown   = errors.New("fleet: unknown task")
)

type Task struct {
	ID       string          `json:"id"`
	Kind     string          `json:"kind"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	Assignee string          `json:"assignee,omitempty"` // empty while pending
	Attempts int             `json:"attempts"`
}

// Dispatch pushes a task to a device (MQTT downlink in the gateway). An error
// means the device could not take it right now.
type Dispatch func(deviceID string, t Task) error

// Scheduler hands tasks to the least loaded online device and moves them when
// their device goes dark.
type Scheduler struct {
	reg      *Registry
	dispatch Dispatch

	mu    sync.Mutex
	tasks map[string]*Task
}

func NewScheduler(reg *Registry, d Dispatch) *Scheduler {
	s := &Scheduler{reg: reg, dispatch: d, tasks: make(map[string]*Task)}
	reg.OnChange(s.onEvent)
	return s
}

func (s *Scheduler) Submit(t Task) (Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.tasks[t.ID]; dup {
		return Task{}, ErrDuplicate
	}
	tk := t
	tk.Assignee, tk.Attempts = "", 0
	s.tasks[t.ID] = &tk
	err := s.place(&tk, "")
	return tk, err
}

func (s *Scheduler) Done(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tasks[id]; !ok {
		return ErrUnknown
	}
	delete(s.tasks, id)
	return nil
}

func (s *Scheduler) List() []Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		out = append(out, *t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (s *Scheduler) Pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, t := range s.tasks {
		if t.Assignee == "" {
			n++
		}
	}
	return n
}

// caller holds mu. avoid is the device we are moving away from.
func (s *Scheduler) place(t *Task, avoid string) error {
	load := map[string]int{}
	for _, o := range s.tasks {
		if o.Assignee != "" {
			load[o.Assignee]++
		}
	}
	cands := s.reg.OnlineIDs()
	sort.SliceStable(cands, func(i, j int) bool { return load[cands[i]] < load[cands[j]] })

	for _, id := range cands {
		if id == avoid {
			continue
		}
		t.Attempts++
		if err := s.dispatch(id, *t); err != nil {
			continue
		}
		t.Assignee = id
		return nil
	}
	t.Assignee = ""
	return ErrNoDevice
}

func (s *Scheduler) onEvent(e Event) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch e.To {
	case Offline:
		ids := make([]string, 0)
		for id, t := range s.tasks {
			if t.Assignee == e.DeviceID {
				ids = append(ids, id)
			}
		}
		sort.Strings(ids)
		for _, id := range ids {
			t := s.tasks[id]
			t.Assignee = ""
			_ = s.place(t, e.DeviceID) // stays pending if nobody can take it
		}
	case Online:
		// capacity showed up, retry whatever is waiting
		var waiting []string
		for id, t := range s.tasks {
			if t.Assignee == "" {
				waiting = append(waiting, id)
			}
		}
		sort.Strings(waiting)
		for _, id := range waiting {
			_ = s.place(s.tasks[id], "")
		}
	}
}
