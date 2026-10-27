package ingest

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"edgemesh/internal/anomaly"
	"edgemesh/internal/fleet"
	"edgemesh/internal/metrics"
	"edgemesh/internal/mqtt"
	"edgemesh/internal/telemetry"
	"edgemesh/internal/wal"
)

type rig struct {
	log    *wal.Log
	pipe   *Pipeline
	fleet  *fleet.Registry
	srv    *mqtt.Server
	addr   string
	alerts chan anomaly.Alert
	reg    *metrics.Registry
	m      *Metrics
	hm     *HandlerMetrics
}

func newRig(t *testing.T, cfg Config) *rig {
	t.Helper()
	l, err := wal.Open(t.TempDir(), wal.Options{})
	if err != nil {
		t.Fatal(err)
	}
	reg := metrics.NewRegistry()
	r := &rig{log: l, reg: reg, alerts: make(chan anomaly.Alert, 64)}
	r.m = NewMetrics(reg)
	r.hm = NewHandlerMetrics(reg)
	r.fleet = fleet.NewRegistry(fleet.Config{})
	r.pipe = NewPipeline(cfg, l, anomaly.New(anomaly.Config{}), func(a anomaly.Alert) {
		select {
		case r.alerts <- a:
		default:
		}
	}, r.m)
	r.srv = mqtt.NewServer(mqtt.Config{}, NewHandler(r.pipe, r.fleet, r.hm))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r.addr = ln.Addr().String()
	go r.srv.Serve(ln)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		r.srv.Shutdown(ctx)
		r.pipe.Close()
		l.Close()
	})
	return r
}

func readAll(t *testing.T, l *wal.Log) []telemetry.Reading {
	t.Helper()
	rd := l.NewReader(0)
	defer rd.Close()
	var out []telemetry.Reading
	for {
		_, p, err := rd.Next()
		if err != nil {
			return out
		}
		var r telemetry.Reading
		if err := r.UnmarshalBinary(p); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
}

func TestEndToEndAckImpliesDurable(t *testing.T) {
	r := newRig(t, Config{Workers: 2})
	c, err := mqtt.Dial(r.addr, mqtt.ClientOptions{ClientID: "dev-1"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	for i := range 20 {
		pl := telemetry.EncodePayload("engine_temp", 80+float64(i)*0.01, time.Now().UnixMicro())
		if err := c.Publish("devices/dev-1/telemetry", pl, 1); err != nil {
			t.Fatal(err)
		}
		// the ack came back, so the record must already be readable
		if got := len(readAll(t, r.log)); got != i+1 {
			t.Fatalf("after ack %d only %d records in WAL", i, got)
		}
	}
	rs := readAll(t, r.log)
	if rs[0].DeviceID != "dev-1" || rs[0].Metric != "engine_temp" || rs[0].RecvTS == 0 {
		t.Fatalf("bad record %+v", rs[0])
	}
	if d, ok := r.fleet.Get("dev-1"); !ok || d.State != fleet.Online {
		t.Fatal("device not tracked")
	}
}

func TestAnomalyAlertIsImmediateAndFlagged(t *testing.T) {
	r := newRig(t, Config{Workers: 1})
	c, _ := mqtt.Dial(r.addr, mqtt.ClientOptions{ClientID: "dev-2"})
	defer c.Close()

	c.Publish("devices/dev-2/telemetry", telemetry.EncodePayload("engine_temp", 250, time.Now().UnixMicro()), 1)
	select {
	case a := <-r.alerts:
		if a.DeviceID != "dev-2" || a.Kind != anomaly.KindThreshold {
			t.Fatalf("%+v", a)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no alert")
	}
	if rs := readAll(t, r.log); len(rs) != 1 || !rs[0].Anomalous() {
		t.Fatalf("reading not flagged in WAL: %+v", rs)
	}
}

func TestSpoofingAndGarbage(t *testing.T) {
	r := newRig(t, Config{})
	c, _ := mqtt.Dial(r.addr, mqtt.ClientOptions{ClientID: "evil", Timeout: 200 * time.Millisecond})
	defer c.Close()

	if err := c.Publish("devices/victim/telemetry", telemetry.EncodePayload("m", 1, 0), 1); err == nil {
		t.Fatal("spoofed publish was acked")
	}
	if err := c.Publish("devices/evil/telemetry", []byte("not json"), 1); err != nil {
		t.Fatalf("garbage should be acked and dropped: %v", err)
	}
	if err := c.Publish("random/topic", []byte("x"), 1); err != nil {
		t.Fatal(err)
	}
	if r.hm.Spoofed.Value() != 1 || r.hm.Malformed.Value() != 2 {
		t.Fatalf("spoofed=%d malformed=%d", r.hm.Spoofed.Value(), r.hm.Malformed.Value())
	}
	if len(readAll(t, r.log)) != 0 {
		t.Fatal("junk reached the WAL")
	}
}

func TestHeartbeatKeepsDeviceAlive(t *testing.T) {
	r := newRig(t, Config{})
	c, _ := mqtt.Dial(r.addr, mqtt.ClientOptions{ClientID: "hb"})
	defer c.Close()
	c.Publish("devices/hb/heartbeat", nil, 0)
	deadline := time.Now().Add(2 * time.Second)
	for r.hm.Heartbeats.Value() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("heartbeat not counted")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if len(readAll(t, r.log)) != 0 {
		t.Fatal("heartbeats should not be stored")
	}
}

func TestManyConcurrentDevices(t *testing.T) {
	r := newRig(t, Config{Workers: 4})
	const devs, each = 100, 20
	var wg sync.WaitGroup
	for d := range devs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := fmt.Sprintf("dev-%03d", d)
			c, err := mqtt.Dial(r.addr, mqtt.ClientOptions{ClientID: id})
			if err != nil {
				t.Error(err)
				return
			}
			defer c.Close()
			for i := range each {
				if err := c.Publish("devices/"+id+"/telemetry", telemetry.EncodePayload("load", float64(i), time.Now().UnixMicro()), 1); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	rs := readAll(t, r.log)
	if len(rs) != devs*each {
		t.Fatalf("got %d records, want %d", len(rs), devs*each)
	}
	// per-device order must survive the sharded workers
	last := map[string]float64{}
	for _, x := range rs {
		if prev, ok := last[x.DeviceID]; ok && x.Value != prev+1 {
			t.Fatalf("%s out of order: %v after %v", x.DeviceID, x.Value, prev)
		}
		last[x.DeviceID] = x.Value
	}
	if r.m.Latency.Count() != devs*each {
		t.Fatalf("latency samples %d", r.m.Latency.Count())
	}
}

func TestQoS0ShedsUnderPressure(t *testing.T) {
	l, _ := wal.Open(t.TempDir(), wal.Options{})
	defer l.Close()
	reg := metrics.NewRegistry()
	m := NewMetrics(reg)
	p := NewPipeline(Config{Workers: 1, QueueDepth: 2, MaxBatch: 1}, l, anomaly.New(anomaly.Config{}), nil, m)

	// don't start draining fast: hammer far more than the queue holds
	shed := 0
	for i := range 50000 {
		if err := p.Submit(context.Background(), telemetry.Reading{DeviceID: "d", Metric: "m", Value: float64(i)}, false); err == ErrOverloaded {
			shed++
		}
	}
	p.Close()
	if shed == 0 || m.Dropped.Value() != uint64(shed) {
		t.Fatalf("shed=%d counter=%d", shed, m.Dropped.Value())
	}
	if err := p.Submit(context.Background(), telemetry.Reading{DeviceID: "d"}, true); err != ErrClosed {
		t.Fatalf("got %v", err)
	}
}

func TestParseTopic(t *testing.T) {
	cases := []struct {
		in, dev, kind string
		ok            bool
	}{
		{"devices/a/telemetry", "a", "telemetry", true},
		{"devices/a-b_1/heartbeat", "a-b_1", "heartbeat", true},
		{"devices//telemetry", "", "", false},
		{"devices/a/other", "", "", false},
		{"devices/a/telemetry/x", "", "", false},
		{"other/a/telemetry", "", "", false},
		{"devices/a", "", "", false},
	}
	for _, c := range cases {
		d, k, ok := parseTopic(c.in)
		if ok != c.ok || (ok && (d != c.dev || k != c.kind)) {
			t.Errorf("%q => %q %q %v", c.in, d, k, ok)
		}
	}
}
