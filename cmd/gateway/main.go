package main

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"edgemesh/internal/admin"
	"edgemesh/internal/anomaly"
	"edgemesh/internal/config"
	"edgemesh/internal/fleet"
	"edgemesh/internal/forward"
	"edgemesh/internal/grpcapi"
	"edgemesh/internal/ingest"
	"edgemesh/internal/metrics"
	"edgemesh/internal/mqtt"
	"edgemesh/internal/sink"
	"edgemesh/internal/tlsutil"
	"edgemesh/internal/wal"
	"google.golang.org/grpc/credentials"
)

// set at build time: -ldflags "-X main.version=v1.2.3"
var version = "dev"

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "version", "-version", "--version":
			fmt.Println(version)
			return
		case "healthcheck": // for container HEALTHCHECK, the image has no curl
			os.Exit(healthcheck(os.Args[2:]))
		}
	}
	cfg, err := config.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(2)
	}
	if err := run(cfg); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func newLogger(c config.Config) *slog.Logger {
	var lvl slog.Level
	_ = lvl.UnmarshalText([]byte(c.LogLevel))
	opts := &slog.HandlerOptions{Level: lvl}
	if c.LogJSON {
		return slog.New(slog.NewJSONHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewTextHandler(os.Stderr, opts))
}

func run(cfg config.Config) error {
	lg := newLogger(cfg)
	slog.SetDefault(lg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reg := metrics.NewRegistry()
	reg.RegisterRuntime()

	var err error
	var srvTLS *tls.Config
	if cfg.TLSCert != "" {
		if srvTLS, err = tlsutil.Server(cfg.TLSCert, cfg.TLSKey, cfg.TLSClientCA); err != nil {
			return err
		}
		lg.Info("tls enabled", "mtls", cfg.TLSClientCA != "")
	}
	cloudTLS, err := tlsutil.Client(cfg.CloudCA)
	if err != nil {
		return fmt.Errorf("cloud-ca: %w", err)
	}

	syncMode, _ := cfg.WALSync()
	log, err := wal.Open(filepath.Join(cfg.DataDir, "wal"), wal.Options{
		SegmentBytes: int64(cfg.SegmentMB) << 20,
		MaxBytes:     int64(cfg.WALMaxMB) << 20,
		Sync:         syncMode,
		SyncEvery:    cfg.SyncEvery,
		Logger:       lg,
	})
	if err != nil {
		return fmt.Errorf("open wal: %w", err)
	}
	defer log.Close()
	ws := log.Stats()
	lg.Info("wal ready", "first", ws.FirstIndex, "next", ws.NextIndex, "segments", ws.Segments)

	rules := anomaly.DefaultRules()
	if cfg.RulesFile != "" {
		if rules, err = anomaly.LoadRules(cfg.RulesFile); err != nil {
			return fmt.Errorf("rules: %w", err)
		}
	}
	det := anomaly.New(anomaly.Config{Rules: rules})

	fl := fleet.NewRegistry(fleet.Config{SuspectAfter: cfg.SuspectAfter, OfflineAfter: cfg.OfflineAfter})
	fl.OnChange(func(e fleet.Event) {
		lg.Info("device state", "device", e.DeviceID, "from", e.From.String(), "to", e.To.String())
	})

	alerts := admin.NewAlertLog(500)
	im := ingest.NewMetrics(reg)
	onAlert := func(a anomaly.Alert) {
		alerts.Add(a)
		lg.Warn("anomaly", "device", a.DeviceID, "metric", a.Metric, "kind", a.Kind, "value", a.Value, "expected", a.Expected, "score", a.Score)
	}
	pipe := ingest.NewPipeline(ingest.Config{Workers: cfg.Workers, QueueDepth: cfg.QueueDepth, Logger: lg}, log, det, onAlert, im)
	handler := ingest.NewHandler(pipe, fl, ingest.NewHandlerMetrics(reg))

	var auth func(string, string, []byte) bool
	if cfg.AuthToken != "" {
		want := []byte(cfg.AuthToken)
		auth = func(_, _ string, pw []byte) bool { return subtle.ConstantTimeCompare(pw, want) == 1 }
	}
	srv := mqtt.NewServer(mqtt.Config{Addr: cfg.MQTTAddr, MaxConns: cfg.MaxConns, Auth: auth, Logger: lg}, handler)

	var gsrv *grpcapi.Server
	sched := fleet.NewScheduler(fl, func(dev string, t fleet.Task) error {
		b, _ := json.Marshal(t)
		err := srv.Publish(dev, "devices/"+dev+"/tasks", b)
		if err != nil && gsrv != nil {
			return gsrv.Dispatch(dev, t)
		}
		return err
	})

	if cfg.GRPCAddr != "" {
		var creds credentials.TransportCredentials
		if srvTLS != nil {
			creds = credentials.NewTLS(srvTLS)
		}
		gsrv = grpcapi.New(pipe, fl, cfg.AuthToken, creds, reg)
	}

	var up sink.Sink = sink.Discard{}
	if cfg.CloudURL != "" {
		up = sink.NewHTTP(cfg.CloudURL, cfg.CloudToken, 10*time.Second, cloudTLS)
	}
	fwd := forward.New(log, up, forward.OpenCheckpoint(filepath.Join(cfg.DataDir, "wal")), forward.Config{
		GatewayID: cfg.GatewayID, MinBatch: cfg.MinBatch, MaxBatch: cfg.MaxBatch, MaxRate: cfg.CatchUpRate, Logger: lg,
	})

	registerGauges(reg, srv, pipe, log, fwd, fl, det, sched)

	isReady := make(chan struct{})
	adm := admin.New(cfg.AdminAddr, admin.Deps{
		Metrics: reg, Fleet: fl, Scheduler: sched, Alerts: alerts, WAL: log, Forwarder: fwd, MQTT: srv,
		Token: cfg.AuthToken,
		Ready: func() bool {
			select {
			case <-isReady:
				return true
			default:
				return false
			}
		},
	})

	bg, cancelBG := context.WithCancel(context.Background())
	defer cancelBG()
	var wg sync.WaitGroup
	spawn := func(name string, fn func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(bg); err != nil && !errors.Is(err, context.Canceled) {
				lg.Error(name+" stopped", "err", err)
			}
		}()
	}
	spawn("forwarder", fwd.Run)
	spawn("fleet", func(c context.Context) error { fl.Run(c); return nil })
	spawn("detector-gc", func(c context.Context) error { gcLoop(c, det); return nil })

	errc := make(chan error, 3)
	mqttLn, err := net.Listen("tcp", cfg.MQTTAddr)
	if err != nil {
		return err
	}
	if srvTLS != nil {
		mqttLn = tls.NewListener(mqttLn, srvTLS)
	}
	go func() { errc <- srv.Serve(mqttLn) }()

	if gsrv != nil {
		gl, err := net.Listen("tcp", cfg.GRPCAddr)
		if err != nil {
			return err
		}
		go func() { errc <- gsrv.Serve(gl) }()
	}

	adm.TLSConfig = srvTLS
	go func() {
		var err error
		if srvTLS != nil {
			err = adm.ListenAndServeTLS("", "") // certs come from TLSConfig.GetCertificate
		} else {
			err = adm.ListenAndServe()
		}
		if !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()
	close(isReady)
	lg.Info("gateway up", "version", version, "id", cfg.GatewayID, "mqtt", cfg.MQTTAddr, "grpc", cfg.GRPCAddr, "tls", srvTLS != nil, "admin", cfg.AdminAddr, "cloud", cfg.CloudURL, "workers", cfg.Workers)

	select {
	case <-ctx.Done():
		lg.Info("shutting down")
	case err = <-errc:
		lg.Error("listener failed", "err", err)
	}

	// order matters: stop intake, flush what is queued, then stop the uploader
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(sctx)
	if gsrv != nil {
		gsrv.Shutdown(sctx)
	}
	pipe.Close()
	cancelBG()
	wg.Wait()
	_ = admin.Shutdown(adm, 2*time.Second)
	lg.Info("bye", "lag_records", fwd.Lag())
	return err
}

func gcLoop(ctx context.Context, det *anomaly.Detector) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			det.Sweep(time.Now().Add(-15 * time.Minute).UnixMicro())
		}
	}
}

func registerGauges(reg *metrics.Registry, srv *mqtt.Server, pipe *ingest.Pipeline, log *wal.Log, fwd *forward.Forwarder, fl *fleet.Registry, det *anomaly.Detector, sched *fleet.Scheduler) {
	f := func(v any) float64 {
		switch x := v.(type) {
		case int64:
			return float64(x)
		case uint64:
			return float64(x)
		case int:
			return float64(x)
		}
		return 0
	}
	reg.GaugeFunc("edge_mqtt_connections", "open device connections", func() float64 { return f(srv.Stats().Active) })
	reg.CounterFunc("edge_mqtt_accepted_total", "accepted connections", func() float64 { return f(srv.Stats().Accepted) })
	reg.CounterFunc("edge_mqtt_rejected_total", "rejected connections", func() float64 { return f(srv.Stats().Rejected) })
	reg.CounterFunc("edge_mqtt_packets_total", "packets read", func() float64 { return f(srv.Stats().PacketsIn) })
	reg.CounterFunc("edge_mqtt_bytes_total", "bytes read", func() float64 { return f(srv.Stats().BytesIn) })
	reg.CounterFunc("edge_mqtt_protocol_errors_total", "protocol violations", func() float64 { return f(srv.Stats().ProtoError) })
	reg.GaugeFunc("edge_ingest_queue_depth", "readings waiting for a worker", func() float64 { return f(pipe.QueueDepth()) })
	reg.GaugeFunc("edge_wal_bytes", "bytes buffered on disk", func() float64 { return f(log.Stats().Bytes) })
	reg.GaugeFunc("edge_wal_segments", "wal segment files", func() float64 { return f(log.Stats().Segments) })
	reg.CounterFunc("edge_wal_evicted_total", "records dropped by the disk cap before upload", func() float64 { return f(log.Stats().Dropped) })
	reg.GaugeFunc("edge_forward_lag_records", "records not yet acked upstream", func() float64 { return f(fwd.Lag()) })
	reg.GaugeFunc("edge_forward_online", "1 when the last upload succeeded", func() float64 {
		if fwd.State() == forward.StateOffline {
			return 0
		}
		return 1
	})
	reg.GaugeFunc("edge_forward_batch_size", "current upload batch size", func() float64 { return f(fwd.BatchSize()) })
	reg.CounterFunc("edge_forward_sent_total", "readings delivered upstream", func() float64 { return f(fwd.Sent.Load()) })
	reg.CounterFunc("edge_forward_failures_total", "failed upload attempts", func() float64 { return f(fwd.Failures.Load()) })
	reg.CounterFunc("edge_forward_dead_lettered_total", "readings the sink refused permanently", func() float64 { return f(fwd.DeadLettered.Load()) })
	reg.GaugeFunc("edge_fleet_online", "devices online", func() float64 { on, _, _ := fl.Counts(); return f(on) })
	reg.GaugeFunc("edge_fleet_suspect", "devices suspect", func() float64 { _, s, _ := fl.Counts(); return f(s) })
	reg.GaugeFunc("edge_fleet_offline", "devices offline", func() float64 { _, _, o := fl.Counts(); return f(o) })
	reg.GaugeFunc("edge_anomaly_series", "tracked (device, metric) series", func() float64 { return f(det.Series()) })
	reg.GaugeFunc("edge_tasks_pending", "tasks with no device", func() float64 { return f(sched.Pending()) })
}

func healthcheck(args []string) int {
	url := "http://127.0.0.1:8080/readyz"
	if len(args) > 0 {
		url = args[0]
	}
	c := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}} // loopback probe only
	resp, err := c.Get(url)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
