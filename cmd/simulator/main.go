// simulator spins up thousands of fake devices that speak MQTT to the gateway.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"edgemesh/internal/metrics"
	"edgemesh/internal/mqtt"
	"edgemesh/internal/telemetry"
)

type opts struct {
	addr      string
	devices   int
	interval  time.Duration
	hbEvery   time.Duration
	ramp      int
	duration  time.Duration
	token     string
	spike     float64
	silent    float64
	prefix    string
	proto     string
	tlsCA     string
	tlsCert   string
	tlsKey    string
	grpcConns int
	tls       *tls.Config
}

type stats struct {
	connected atomic.Int64
	sent      atomic.Uint64
	acked     atomic.Uint64
	errs      atomic.Uint64
	reconns   atomic.Uint64
	rtt       metrics.Histogram
}

func main() {
	var o opts
	flag.StringVar(&o.addr, "addr", "127.0.0.1:1883", "gateway mqtt address")
	flag.IntVar(&o.devices, "devices", 1000, "number of simulated devices")
	flag.DurationVar(&o.interval, "interval", time.Second, "telemetry period per device")
	flag.DurationVar(&o.hbEvery, "heartbeat", 5*time.Second, "heartbeat period")
	flag.IntVar(&o.ramp, "ramp", 1000, "new connections per second")
	flag.DurationVar(&o.duration, "duration", 0, "stop after this long (0 = until ctrl-c)")
	flag.StringVar(&o.token, "token", os.Getenv("EDGE_AUTH_TOKEN"), "mqtt password")
	flag.Float64Var(&o.spike, "spike", 0.002, "per-reading probability of an engine temp spike")
	flag.Float64Var(&o.silent, "silent", 0.01, "fraction of devices that go dark for a while mid-run")
	flag.StringVar(&o.prefix, "prefix", "sim", "device id prefix")
	flag.StringVar(&o.proto, "proto", "mqtt", "mqtt|grpc")
	flag.StringVar(&o.tlsCA, "tls-ca", "", "CA bundle; enables TLS")
	flag.StringVar(&o.tlsCert, "tls-cert", "", "client cert for mTLS")
	flag.StringVar(&o.tlsKey, "tls-key", "", "client key for mTLS")
	flag.IntVar(&o.grpcConns, "grpc-conns", 16, "shared http2 connections for -proto=grpc (streams are multiplexed)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if o.duration > 0 {
		var c context.CancelFunc
		ctx, c = context.WithTimeout(ctx, o.duration)
		defer c()
	}

	tcfg, err := clientTLS(o)
	if err != nil {
		fmt.Fprintln(os.Stderr, "tls:", err)
		os.Exit(2)
	}
	o.tls = tcfg
	st := &stats{}
	var wg sync.WaitGroup
	var pool *grpcPool
	if o.proto == "grpc" {
		if pool, err = newGRPCPool(o); err != nil {
			fmt.Fprintln(os.Stderr, "grpc:", err)
			os.Exit(2)
		}
		defer pool.close()
	}
	go report(ctx, st)

	gap := time.Second / time.Duration(max(o.ramp, 1))
	for i := range o.devices {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if pool != nil {
				grpcDevice(ctx, o, i, st, pool)
				return
			}
			device(ctx, o, i, st)
		}()
		time.Sleep(gap)
	}
	wg.Wait()

	fmt.Printf("\nfinal: sent=%d acked=%d errors=%d reconnects=%d  puback rtt p50=%v p99=%v p99.9=%v max=%v\n",
		st.sent.Load(), st.acked.Load(), st.errs.Load(), st.reconns.Load(),
		st.rtt.Quantile(0.5), st.rtt.Quantile(0.99), st.rtt.Quantile(0.999), st.rtt.Max())
}

func report(ctx context.Context, st *stats) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	var last uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n := st.acked.Load()
			fmt.Printf("conns=%d acked=%d (%d/s) errs=%d rtt p50=%v p99=%v\n",
				st.connected.Load(), n, (n-last)/5, st.errs.Load(), st.rtt.Quantile(0.5), st.rtt.Quantile(0.99))
			last = n
		}
	}
}

func device(ctx context.Context, o opts, n int, st *stats) {
	id := fmt.Sprintf("%s-%06d", o.prefix, n)
	rng := rand.New(rand.NewPCG(uint64(n), 99))
	temp := 70 + rng.Float64()*15
	phase := rng.Float64() * 2 * math.Pi

	var darkFrom, darkTo time.Time
	if rng.Float64() < o.silent {
		darkFrom = time.Now().Add(20*time.Second + time.Duration(rng.IntN(20))*time.Second)
		darkTo = darkFrom.Add(45 * time.Second)
	}

	backoff := 500 * time.Millisecond
	first := true
	for ctx.Err() == nil {
		c, err := mqtt.Dial(o.addr, mqtt.ClientOptions{ClientID: id, Password: o.token, Username: "sim", KeepAlive: 30 * time.Second, TLS: o.tls})
		if err != nil {
			st.errs.Add(1)
			if !sleep(ctx, backoff+time.Duration(rng.IntN(500))*time.Millisecond) {
				return
			}
			backoff = min(backoff*2, 10*time.Second)
			continue
		}
		backoff = 500 * time.Millisecond
		if !first {
			st.reconns.Add(1)
		}
		first = false
		st.connected.Add(1)
		_ = c.Subscribe("devices/" + id + "/tasks")

		// stagger so 5k devices don't all fire on the same tick
		sleep(ctx, time.Duration(rng.Int64N(int64(o.interval))))
		tick := time.NewTicker(o.interval)
		hb := time.NewTicker(o.hbEvery)
		err = loop(ctx, c, id, o, st, rng, &temp, phase, tick, hb, darkFrom, darkTo)
		tick.Stop()
		hb.Stop()
		c.Close()
		st.connected.Add(-1)
		if err == nil {
			return
		}
		st.errs.Add(1)
	}
}

func loop(ctx context.Context, c *mqtt.Client, id string, o opts, st *stats, rng *rand.Rand, temp *float64, phase float64, tick, hb *time.Ticker, darkFrom, darkTo time.Time) error {
	topic := "devices/" + id + "/telemetry"
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-hb.C:
			if dark(darkFrom, darkTo) {
				continue
			}
			if err := c.Publish("devices/"+id+"/heartbeat", nil, 0); err != nil {
				return err
			}
			_ = c.Poll(time.Millisecond) // pick up downlink tasks
		case now := <-tick.C:
			if dark(darkFrom, darkTo) {
				continue
			}
			*temp += (rng.Float64()-0.5)*0.6 + 0.02*math.Sin(float64(now.Unix())/30+phase)
			*temp = min(max(*temp, 40), 105)
			v := *temp
			if rng.Float64() < o.spike {
				v += 35 + rng.Float64()*20
			}
			pl := telemetry.EncodePayload("engine_temp", v, time.Now().UnixMicro())
			st.sent.Add(1)
			t0 := time.Now()
			if err := c.Publish(topic, pl, 1); err != nil {
				return err
			}
			st.rtt.Observe(time.Since(t0))
			st.acked.Add(1)

			if rng.IntN(5) == 0 {
				vib := telemetry.EncodePayload("vibration", 2+rng.Float64()*0.8, time.Now().UnixMicro())
				if err := c.Publish(topic, vib, 0); err != nil {
					return err
				}
			}
		}
	}
}

func dark(from, to time.Time) bool {
	if from.IsZero() {
		return false
	}
	n := time.Now()
	return n.After(from) && n.Before(to)
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
