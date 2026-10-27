package config

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"time"

	"edgemesh/internal/wal"
)

type Config struct {
	GatewayID   string
	MQTTAddr    string
	GRPCAddr    string
	AdminAddr   string
	TLSCert     string
	TLSKey      string
	TLSClientCA string // set = devices must present a cert signed by it (mTLS)
	CloudCA     string
	DataDir     string
	CloudURL    string
	CloudToken  string
	AuthToken   string // devices must present this as MQTT password
	RulesFile   string
	LogLevel    string
	LogJSON     bool

	MaxConns     int
	Workers      int
	QueueDepth   int
	SegmentMB    int
	WALMaxMB     int
	SyncMode     string
	SyncEvery    time.Duration
	MinBatch     int
	MaxBatch     int
	CatchUpRate  int
	SuspectAfter time.Duration
	OfflineAfter time.Duration
}

func Parse(args []string) (Config, error) {
	host, _ := os.Hostname()
	c := Config{}
	fs := flag.NewFlagSet("gateway", flag.ContinueOnError)
	fs.StringVar(&c.GatewayID, "id", host, "gateway id sent upstream")
	fs.StringVar(&c.MQTTAddr, "mqtt", ":1883", "mqtt listen address")
	fs.StringVar(&c.GRPCAddr, "grpc", ":9000", "grpc ingest listen address (empty = disabled)")
	fs.StringVar(&c.TLSCert, "tls-cert", "", "server certificate; enables TLS on mqtt, grpc and admin")
	fs.StringVar(&c.TLSKey, "tls-key", "", "server private key")
	fs.StringVar(&c.TLSClientCA, "tls-client-ca", "", "CA bundle for verifying device certs (enables mTLS)")
	fs.StringVar(&c.CloudCA, "cloud-ca", "", "CA bundle to trust for the upstream (default: system roots)")
	fs.StringVar(&c.AdminAddr, "admin", ":8080", "admin/metrics http address")
	fs.StringVar(&c.DataDir, "data", "./data", "wal + checkpoint directory")
	fs.StringVar(&c.CloudURL, "cloud", "", "upstream ingest url (empty = discard)")
	fs.StringVar(&c.CloudToken, "cloud-token", os.Getenv("EDGE_CLOUD_TOKEN"), "bearer token for upstream")
	fs.StringVar(&c.AuthToken, "auth-token", os.Getenv("EDGE_AUTH_TOKEN"), "required mqtt password (empty = open)")
	fs.StringVar(&c.RulesFile, "rules", "", "anomaly rules json (default: built-in)")
	fs.StringVar(&c.LogLevel, "log-level", "info", "debug|info|warn|error")
	fs.BoolVar(&c.LogJSON, "log-json", false, "json logs")

	fs.IntVar(&c.MaxConns, "max-conns", 50000, "max concurrent device connections")
	fs.IntVar(&c.Workers, "workers", runtime.NumCPU(), "ingest workers")
	fs.IntVar(&c.QueueDepth, "queue", 8192, "total ingest queue depth")
	fs.IntVar(&c.SegmentMB, "segment-mb", 16, "wal segment size")
	fs.IntVar(&c.WALMaxMB, "wal-max-mb", 1024, "disk cap for buffered data, oldest dropped past it (0 = unlimited)")
	fs.StringVar(&c.SyncMode, "sync", "interval", "wal fsync: always|interval|never")
	fs.DurationVar(&c.SyncEvery, "sync-every", 100*time.Millisecond, "fsync period for -sync=interval")
	fs.IntVar(&c.MinBatch, "min-batch", 16, "upstream batch size after a failure")
	fs.IntVar(&c.MaxBatch, "max-batch", 2048, "max upstream batch size")
	fs.IntVar(&c.CatchUpRate, "catchup-rate", 0, "max records/s while draining a backlog (0 = unlimited)")
	fs.DurationVar(&c.SuspectAfter, "suspect-after", 15*time.Second, "silence before a device is suspect")
	fs.DurationVar(&c.OfflineAfter, "offline-after", 30*time.Second, "silence before a device is offline")

	if err := fs.Parse(args); err != nil {
		return c, err
	}
	return c, c.validate()
}

func (c *Config) validate() error {
	if (c.TLSCert == "") != (c.TLSKey == "") {
		return fmt.Errorf("tls-cert and tls-key go together")
	}
	if c.TLSClientCA != "" && c.TLSCert == "" {
		return fmt.Errorf("tls-client-ca needs tls-cert")
	}
	if c.GatewayID == "" {
		c.GatewayID = "gateway"
	}
	if c.Workers < 1 || c.QueueDepth < c.Workers {
		return fmt.Errorf("need workers >= 1 and queue >= workers")
	}
	if c.MinBatch < 1 || c.MaxBatch < c.MinBatch {
		return fmt.Errorf("bad batch bounds %d..%d", c.MinBatch, c.MaxBatch)
	}
	if c.OfflineAfter <= c.SuspectAfter {
		return fmt.Errorf("offline-after must be greater than suspect-after")
	}
	if c.SegmentMB < 1 {
		return fmt.Errorf("segment-mb must be >= 1")
	}
	if c.WALMaxMB != 0 && c.WALMaxMB < 2*c.SegmentMB {
		return fmt.Errorf("wal-max-mb must be 0 or at least twice segment-mb")
	}
	if _, err := c.WALSync(); err != nil {
		return err
	}
	return nil
}

func (c *Config) WALSync() (wal.SyncMode, error) {
	switch c.SyncMode {
	case "always":
		return wal.SyncAlways, nil
	case "interval":
		return wal.SyncInterval, nil
	case "never":
		return wal.SyncNever, nil
	}
	return 0, fmt.Errorf("unknown sync mode %q", c.SyncMode)
}
