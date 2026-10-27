package grpcapi

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"edgemesh/internal/anomaly"
	"edgemesh/internal/fleet"
	"edgemesh/internal/grpcapi/edgev1"
	"edgemesh/internal/ingest"
	"edgemesh/internal/metrics"
	"edgemesh/internal/telemetry"
	"edgemesh/internal/tlsutil"
	"edgemesh/internal/wal"
)

type rig struct {
	log  *wal.Log
	fl   *fleet.Registry
	addr string
}

func newRig(t *testing.T, token string, creds credentials.TransportCredentials) *rig {
	t.Helper()
	l, err := wal.Open(t.TempDir(), wal.Options{})
	if err != nil {
		t.Fatal(err)
	}
	reg := metrics.NewRegistry()
	pipe := ingest.NewPipeline(ingest.Config{Workers: 2}, l, anomaly.New(anomaly.Config{}), nil, ingest.NewMetrics(reg))
	fl := fleet.NewRegistry(fleet.Config{})
	s := New(pipe, fl, token, creds, reg)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go s.Serve(ln)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		s.Shutdown(ctx)
		pipe.Close()
		l.Close()
	})
	return &rig{log: l, fl: fl, addr: ln.Addr().String()}
}

func dial(t *testing.T, addr string, creds credentials.TransportCredentials) edgev1.IngestClient {
	t.Helper()
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	return edgev1.NewIngestClient(cc)
}

func walCount(l *wal.Log) []telemetry.Reading {
	rd := l.NewReader(0)
	defer rd.Close()
	var out []telemetry.Reading
	for {
		_, p, err := rd.Next()
		if err != nil {
			return out
		}
		var r telemetry.Reading
		r.UnmarshalBinary(p)
		out = append(out, r)
	}
}

func TestStreamAckMeansDurable(t *testing.T) {
	r := newRig(t, "", nil)
	c := dial(t, r.addr, insecure.NewCredentials())
	st, err := c.Stream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for seq := uint64(1); seq <= 5; seq++ {
		err := st.Send(&edgev1.ReadingBatch{DeviceId: "g-1", Seq: seq, Readings: []*edgev1.Reading{
			{Metric: "engine_temp", Value: 80},
			{Metric: "vibration", Value: 2.5},
			{Metric: "engine_temp", Value: 81},
		}})
		if err != nil {
			t.Fatal(err)
		}
		ack, err := st.Recv()
		if err != nil || ack.Seq != seq || ack.Accepted != 3 || ack.Status != edgev1.Status_STATUS_OK {
			t.Fatalf("ack=%v err=%v", ack, err)
		}
		if got := len(walCount(r.log)); got != int(seq)*3 {
			t.Fatalf("after ack %d: %d records in WAL", seq, got)
		}
	}
	if d, ok := r.fl.Get("g-1"); !ok || d.State != fleet.Online {
		t.Fatal("device not tracked")
	}
}

func TestInvalidBatches(t *testing.T) {
	r := newRig(t, "", nil)
	c := dial(t, r.addr, insecure.NewCredentials())
	st, _ := c.Stream(context.Background())
	bad := []*edgev1.ReadingBatch{
		{DeviceId: "", Readings: []*edgev1.Reading{{Metric: "m"}}},
		{DeviceId: "a/b", Readings: []*edgev1.Reading{{Metric: "m"}}},
		{DeviceId: "a"},
		{DeviceId: "a", Readings: []*edgev1.Reading{{Metric: ""}}},
	}
	for i, b := range bad {
		b.Seq = uint64(i)
		st.Send(b)
		ack, err := st.Recv()
		if err != nil || ack.Status != edgev1.Status_STATUS_INVALID {
			t.Fatalf("case %d: %v %v", i, ack, err)
		}
	}
	if len(walCount(r.log)) != 0 {
		t.Fatal("invalid data reached the WAL")
	}
}

func TestAuth(t *testing.T) {
	r := newRig(t, "s3cret", nil)
	c := dial(t, r.addr, insecure.NewCredentials())

	_, err := c.Heartbeat(context.Background(), &edgev1.HeartbeatRequest{DeviceId: "x"})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unary without token: %v", err)
	}
	st, _ := c.Stream(context.Background())
	st.Send(&edgev1.ReadingBatch{DeviceId: "x", Readings: []*edgev1.Reading{{Metric: "m"}}})
	if _, err := st.Recv(); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("stream without token: %v", err)
	}

	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer s3cret")
	if _, err := c.Heartbeat(ctx, &edgev1.HeartbeatRequest{DeviceId: "x"}); err != nil {
		t.Fatal(err)
	}
}

func TestMutualTLS(t *testing.T) {
	dir := t.TempDir()
	dev, err := tlsutil.GenerateDev(dir, "localhost", "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	srvCfg, err := tlsutil.Server(dev.Cert, dev.Key, dev.CA)
	if err != nil {
		t.Fatal(err)
	}
	r := newRig(t, "", credentials.NewTLS(srvCfg))

	// no client cert: handshake must fail
	noCert, _ := tlsutil.Client(dev.CA)
	noCert.ServerName = "localhost"
	c := dial(t, r.addr, credentials.NewTLS(noCert))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := c.Heartbeat(ctx, &edgev1.HeartbeatRequest{DeviceId: "x"}); err == nil {
		t.Fatal("connection without client cert succeeded")
	}

	withCert, _ := tlsutil.Client(dev.CA)
	withCert.ServerName = "localhost"
	kp, err := loadPair(dev.ClientCert, dev.ClientKey)
	if err != nil {
		t.Fatal(err)
	}
	withCert.Certificates = kp
	c2 := dial(t, r.addr, credentials.NewTLS(withCert))
	if _, err := c2.Heartbeat(context.Background(), &edgev1.HeartbeatRequest{DeviceId: "x"}); err != nil {
		t.Fatalf("mTLS call failed: %v", err)
	}
}
