// cloudmock stands in for the cloud ingest API. It can be told to fail so the
// gateway's store-and-forward path can be watched doing its job.
package main

import (
	"compress/gzip"
	"encoding/json"
	"flag"
	"log"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"edgemesh/internal/sink"
)

type store struct {
	mu        sync.Mutex
	seen      map[string]bool
	readings  uint64
	anomalies uint64
	dupBatch  uint64
	batches   uint64
	perDevice map[string]uint64
}

func main() {
	addr := flag.String("addr", ":9090", "listen address")
	failRate := flag.Float64("fail-rate", 0, "fraction of requests answered with 503")
	flag.Parse()

	st := &store{seen: map[string]bool{}, perDevice: map[string]uint64{}}
	var downUntil atomic.Int64 // unix nanos

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/ingest", func(w http.ResponseWriter, r *http.Request) {
		if time.Now().UnixNano() < downUntil.Load() || rand.Float64() < *failRate {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		var body = r.Body
		if r.Header.Get("Content-Encoding") == "gzip" {
			zr, err := gzip.NewReader(r.Body)
			if err != nil {
				http.Error(w, "bad gzip", http.StatusBadRequest)
				return
			}
			defer zr.Close()
			body = zr
		}
		var b sink.Batch
		if err := json.NewDecoder(http.MaxBytesReader(w, body, 32<<20)).Decode(&b); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}

		id := r.Header.Get("X-Batch-ID")
		st.mu.Lock()
		st.batches++
		if st.seen[id] {
			st.dupBatch++ // retry of something we already took; ack again
		} else {
			st.seen[id] = true
			st.readings += uint64(len(b.Readings))
			for _, x := range b.Readings {
				st.perDevice[x.DeviceID]++
				if x.Anomalous() {
					st.anomalies++
				}
			}
		}
		st.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		st.mu.Lock()
		defer st.mu.Unlock()
		json.NewEncoder(w).Encode(map[string]any{
			"batches": st.batches, "duplicate_batches": st.dupBatch, "readings": st.readings,
			"anomalies": st.anomalies, "devices": len(st.perDevice),
			"down": time.Now().UnixNano() < downUntil.Load(),
		})
	})

	// POST /admin/outage?seconds=30
	mux.HandleFunc("POST /admin/outage", func(w http.ResponseWriter, r *http.Request) {
		s, err := strconv.Atoi(r.URL.Query().Get("seconds"))
		if err != nil || s < 0 {
			http.Error(w, "seconds=N", http.StatusBadRequest)
			return
		}
		downUntil.Store(time.Now().Add(time.Duration(s) * time.Second).UnixNano())
		log.Printf("simulating outage for %ds", s)
	})

	log.Printf("cloudmock on %s", *addr)
	log.Fatal((&http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}).ListenAndServe())
}
