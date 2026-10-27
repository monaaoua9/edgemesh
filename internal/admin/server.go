package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/pprof"
	"strconv"
	"sync"
	"time"

	"edgemesh/internal/anomaly"
	"edgemesh/internal/fleet"
	"edgemesh/internal/forward"
	"edgemesh/internal/metrics"
	"edgemesh/internal/mqtt"
	"edgemesh/internal/wal"
)

// ring of the latest alerts, so operators can see them without a cloud round trip
type AlertLog struct {
	mu   sync.Mutex
	buf  []anomaly.Alert
	next int
	full bool
}

func NewAlertLog(n int) *AlertLog { return &AlertLog{buf: make([]anomaly.Alert, n)} }

func (a *AlertLog) Add(x anomaly.Alert) {
	a.mu.Lock()
	a.buf[a.next] = x
	a.next = (a.next + 1) % len(a.buf)
	if a.next == 0 {
		a.full = true
	}
	a.mu.Unlock()
}

// newest first
func (a *AlertLog) Recent() []anomaly.Alert {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := a.next
	if a.full {
		n = len(a.buf)
	}
	out := make([]anomaly.Alert, 0, n)
	for i := 1; i <= n; i++ {
		out = append(out, a.buf[(a.next-i+len(a.buf))%len(a.buf)])
	}
	return out
}

type Deps struct {
	Metrics   *metrics.Registry
	Fleet     *fleet.Registry
	Scheduler *fleet.Scheduler
	Alerts    *AlertLog
	WAL       *wal.Log
	Forwarder *forward.Forwarder
	MQTT      *mqtt.Server
	Token     string // if set, mutating endpoints need it as a bearer token
	Ready     func() bool
}

func New(addr string, d Deps) *http.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if d.Ready != nil && !d.Ready() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ready\n"))
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		d.Metrics.Expose(w)
	})

	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		on, sus, off := d.Fleet.Counts()
		ws := d.WAL.Stats()
		ms := d.MQTT.Stats()
		writeJSON(w, http.StatusOK, map[string]any{
			"fleet": map[string]int{"online": on, "suspect": sus, "offline": off},
			"mqtt":  ms,
			"wal":   ws,
			"forward": map[string]any{
				"state":      d.Forwarder.State().String(),
				"lag":        d.Forwarder.Lag(),
				"committed":  d.Forwarder.Committed(),
				"batch_size": d.Forwarder.BatchSize(),
				"sent":       d.Forwarder.Sent.Load(),
				"failures":   d.Forwarder.Failures.Load(),
				"last_error": d.Forwarder.LastError.Load(),
			},
			"pending_tasks": d.Scheduler.Pending(),
		})
	})

	mux.HandleFunc("GET /fleet", func(w http.ResponseWriter, r *http.Request) {
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if limit <= 0 || limit > 5000 {
			limit = 500
		}
		var only *fleet.State
		for _, s := range []fleet.State{fleet.Online, fleet.Suspect, fleet.Offline} {
			if s.String() == r.URL.Query().Get("state") {
				s := s
				only = &s
			}
		}
		writeJSON(w, http.StatusOK, d.Fleet.Snapshot(limit, only))
	})

	mux.HandleFunc("GET /alerts", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, d.Alerts.Recent())
	})

	mux.HandleFunc("GET /tasks", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, d.Scheduler.List())
	})
	mux.HandleFunc("POST /tasks", d.auth(func(w http.ResponseWriter, r *http.Request) {
		var t fleet.Task
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&t); err != nil || t.ID == "" || t.Kind == "" {
			writeJSON(w, http.StatusBadRequest, errBody("INVALID_TASK", "id and kind are required"))
			return
		}
		got, err := d.Scheduler.Submit(t)
		switch {
		case errors.Is(err, fleet.ErrDuplicate):
			writeJSON(w, http.StatusConflict, errBody("TASK_EXISTS", err.Error()))
		case errors.Is(err, fleet.ErrNoDevice):
			// stored and will be placed when a device shows up
			writeJSON(w, http.StatusAccepted, map[string]any{"data": got})
		case err != nil:
			writeJSON(w, http.StatusInternalServerError, errBody("INTERNAL", err.Error()))
		default:
			writeJSON(w, http.StatusCreated, map[string]any{"data": got})
		}
	}))
	mux.HandleFunc("DELETE /tasks/{id}", d.auth(func(w http.ResponseWriter, r *http.Request) {
		if err := d.Scheduler.Done(r.PathValue("id")); err != nil {
			writeJSON(w, http.StatusNotFound, errBody("TASK_NOT_FOUND", err.Error()))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	// profiling: heap/goroutine/cpu under load is half the point of this project
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

func (d Deps) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if d.Token != "" && r.Header.Get("Authorization") != "Bearer "+d.Token {
			writeJSON(w, http.StatusUnauthorized, errBody("UNAUTHORIZED", "bearer token required"))
			return
		}
		next(w, r)
	}
}

func errBody(code, msg string) map[string]any {
	return map[string]any{"error": map[string]string{"code": code, "message": msg}}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func Shutdown(srv *http.Server, d time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return srv.Shutdown(ctx)
}
