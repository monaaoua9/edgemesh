package e2e

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"edgemesh/internal/anomaly"
	"edgemesh/internal/fleet"
	"edgemesh/internal/forward"
	"edgemesh/internal/ingest"
	"edgemesh/internal/metrics"
	"edgemesh/internal/mqtt"
	"edgemesh/internal/sink"
	"edgemesh/internal/telemetry"
	"edgemesh/internal/wal"
)

// cloud that dedupes on batch id like the real contract says it should
type cloud struct {
	down atomic.Bool
	mu   sync.Mutex
	seen map[string]bool
	vals map[string][]float64 // device -> values in arrival order
}

func (c *cloud) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if c.down.Load() {
		http.Error(w, "down", http.StatusServiceUnavailable)
		return
	}
	zr, err := gzip.NewReader(r.Body)
	if err != nil {
		http.Error(w, "gzip", 400)
		return
	}
	var b sink.Batch
	if json.NewDecoder(zr).Decode(&b) != nil {
		http.Error(w, "json", 400)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	id := r.Header.Get("X-Batch-ID")
	if !c.seen[id] {
		c.seen[id] = true
		for _, x := range b.Readings {
			c.vals[x.DeviceID] = append(c.vals[x.DeviceID], x.Value)
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

func (c *cloud) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, v := range c.vals {
		n += len(v)
	}
	return n
}

func TestDeviceToCloudThroughOutageAndRestart(t *testing.T) {
	dir := t.TempDir()
	cl := &cloud{seen: map[string]bool{}, vals: map[string][]float64{}}
	srv := httptest.NewServer(cl)
	defer srv.Close()

	const devs, perDev = 20, 150
	boot := func() (addr string, stop func()) {
		l, err := wal.Open(dir, wal.Options{SegmentBytes: 64 << 10})
		if err != nil {
			t.Fatal(err)
		}
		reg := metrics.NewRegistry()
		pipe := ingest.NewPipeline(ingest.Config{Workers: 2}, l, anomaly.New(anomaly.Config{}), nil, ingest.NewMetrics(reg))
		h := ingest.NewHandler(pipe, fleet.NewRegistry(fleet.Config{}), ingest.NewHandlerMetrics(reg))
		ms := mqtt.NewServer(mqtt.Config{}, h)
		ln, _ := net.Listen("tcp", "127.0.0.1:0")
		go ms.Serve(ln)

		fwd := forward.New(l, sink.NewHTTP(srv.URL, "", 2*time.Second, nil), forward.OpenCheckpoint(dir), forward.Config{
			GatewayID: "gw", MinBatch: 8, MaxBatch: 256, Linger: 5 * time.Millisecond,
			BackoffMin: 10 * time.Millisecond, BackoffMax: 50 * time.Millisecond,
		})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { fwd.Run(ctx); close(done) }()

		return ln.Addr().String(), func() {
			sctx, c := context.WithTimeout(context.Background(), 3*time.Second)
			defer c()
			ms.Shutdown(sctx)
			pipe.Close()
			cancel()
			<-done
			l.Close()
		}
	}

	publish := func(addr string, from, to int) {
		var wg sync.WaitGroup
		for d := range devs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				id := fmt.Sprintf("dev-%02d", d)
				c, err := mqtt.Dial(addr, mqtt.ClientOptions{ClientID: id})
				if err != nil {
					t.Error(err)
					return
				}
				defer c.Close()
				for i := from; i < to; i++ {
					pl := telemetry.EncodePayload("load", float64(i), time.Now().UnixMicro())
					if err := c.Publish("devices/"+id+"/telemetry", pl, 1); err != nil {
						t.Error(err)
						return
					}
				}
			}()
		}
		wg.Wait()
	}

	addr, stop := boot()
	publish(addr, 0, 50)
	waitTotal(t, cl, devs*50)

	cl.down.Store(true) // uplink dies; devices keep publishing and get acks
	publish(addr, 50, 100)
	time.Sleep(100 * time.Millisecond)
	stop() // gateway restarts while there is a backlog on disk

	addr, stop = boot()
	defer stop()
	publish(addr, 100, perDev) // still down
	cl.down.Store(false)
	waitTotal(t, cl, devs*perDev)

	cl.mu.Lock()
	defer cl.mu.Unlock()
	for dev, vals := range cl.vals {
		if len(vals) != perDev {
			t.Fatalf("%s got %d readings, want %d", dev, len(vals), perDev)
		}
		for i, v := range vals {
			if v != float64(i) {
				t.Fatalf("%s out of order/duplicated at %d: %v", dev, i, v)
			}
		}
	}
}

func waitTotal(t *testing.T, c *cloud, want int) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for c.total() < want {
		if time.Now().After(deadline) {
			t.Fatalf("cloud has %d of %d readings", c.total(), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
