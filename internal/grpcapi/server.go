// Package grpcapi is the gRPC front door. It feeds the same pipeline as MQTT:
// an Ack means every reading in that batch is in the WAL.
package grpcapi

import (
	"context"
	"crypto/subtle"
	"errors"
	"io"
	"math"
	"net"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"edgemesh/internal/fleet"
	"edgemesh/internal/grpcapi/edgev1"
	"edgemesh/internal/ingest"
	"edgemesh/internal/metrics"
	"edgemesh/internal/telemetry"
)

const MaxBatchReadings = 1000

var (
	ErrNoTaskStream = errors.New("grpcapi: device has no open task stream")
	ErrTaskBusy     = errors.New("grpcapi: task stream backed up")
)

type taskSub struct {
	ch   chan *edgev1.Task
	done chan struct{} // closed when a newer stream for the same device takes over
}

type Server struct {
	edgev1.UnimplementedIngestServer
	pipe  *ingest.Pipeline
	fleet *fleet.Registry
	token []byte
	gs    *grpc.Server

	tmu   sync.Mutex
	tasks map[string]*taskSub

	batches  *metrics.Counter
	readings *metrics.Counter
	invalid  *metrics.Counter
	rejected *metrics.Counter
	streams  *metrics.Gauge
}

// token empty = no auth. creds nil = plaintext.
func New(p *ingest.Pipeline, f *fleet.Registry, token string, creds credentials.TransportCredentials, reg *metrics.Registry) *Server {
	s := &Server{
		pipe: p, fleet: f, token: []byte(token), tasks: map[string]*taskSub{},
		batches:  reg.Counter("edge_grpc_batches_total", "batches received over gRPC"),
		readings: reg.Counter("edge_grpc_readings_total", "readings acked over gRPC"),
		invalid:  reg.Counter("edge_grpc_invalid_total", "batches rejected as invalid"),
		rejected: reg.Counter("edge_grpc_overloaded_total", "batches answered OVERLOADED"),
		streams:  reg.Gauge("edge_grpc_streams", "open ingest streams"),
	}
	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(s.authUnary),
		grpc.ChainStreamInterceptor(s.authStream),
		grpc.MaxConcurrentStreams(100000),
		grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionIdle: 10 * time.Minute, Time: time.Minute, Timeout: 20 * time.Second}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}),
	}
	if creds != nil {
		opts = append(opts, grpc.Creds(creds))
	}
	s.gs = grpc.NewServer(opts...)
	edgev1.RegisterIngestServer(s.gs, s)
	return s
}

func (s *Server) Serve(l net.Listener) error { return s.gs.Serve(l) }

// GracefulStop waits for in-flight RPCs, falling back to a hard stop at ctx expiry.
func (s *Server) Shutdown(ctx context.Context) {
	done := make(chan struct{})
	go func() { s.gs.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		s.gs.Stop()
	}
}

func (s *Server) check(ctx context.Context) error {
	if len(s.token) == 0 {
		return nil
	}
	md, _ := metadata.FromIncomingContext(ctx)
	for _, v := range md.Get("authorization") {
		if got, ok := strings.CutPrefix(v, "Bearer "); ok && subtle.ConstantTimeCompare([]byte(got), s.token) == 1 {
			return nil
		}
	}
	return status.Error(codes.Unauthenticated, "bearer token required")
}

func (s *Server) authUnary(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	if err := s.check(ctx); err != nil {
		return nil, err
	}
	return h(ctx, req)
}

func (s *Server) authStream(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
	if err := s.check(ss.Context()); err != nil {
		return err
	}
	return h(srv, ss)
}

func validDevice(id string) bool {
	return id != "" && len(id) <= 255 && !strings.ContainsAny(id, "/+#")
}

func (s *Server) Heartbeat(_ context.Context, r *edgev1.HeartbeatRequest) (*edgev1.HeartbeatResponse, error) {
	if !validDevice(r.DeviceId) {
		return nil, status.Error(codes.InvalidArgument, "bad device_id")
	}
	s.fleet.Touch(r.DeviceId)
	return &edgev1.HeartbeatResponse{}, nil
}

func (s *Server) Stream(st edgev1.Ingest_StreamServer) error {
	s.streams.Add(1)
	defer s.streams.Add(-1)
	for {
		b, err := st.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		ack, err := s.batch(st.Context(), b)
		if err != nil {
			return err
		}
		if err := st.Send(ack); err != nil {
			return err
		}
	}
}

// non-nil error only when the stream itself should die (ctx cancelled / shutdown)
func (s *Server) batch(ctx context.Context, b *edgev1.ReadingBatch) (*edgev1.Ack, error) {
	s.batches.Inc()
	ack := &edgev1.Ack{Seq: b.Seq}
	if !validDevice(b.DeviceId) || len(b.Readings) == 0 || len(b.Readings) > MaxBatchReadings {
		s.invalid.Inc()
		ack.Status = edgev1.Status_STATUS_INVALID
		return ack, nil
	}

	now := time.Now().UnixMicro()
	rs := make([]telemetry.Reading, len(b.Readings))
	for i, x := range b.Readings {
		if x.Metric == "" || len(x.Metric) > 255 || math.IsNaN(x.Value) || math.IsInf(x.Value, 0) {
			s.invalid.Inc()
			ack.Status = edgev1.Status_STATUS_INVALID
			return ack, nil
		}
		ts := x.TsUs
		if ts == 0 {
			ts = now
		}
		rs[i] = telemetry.Reading{DeviceID: b.DeviceId, Metric: x.Metric, Value: x.Value, TS: ts, RecvTS: now}
	}
	s.fleet.Touch(b.DeviceId)

	n, err := s.pipe.SubmitMany(ctx, rs)
	ack.Accepted = uint32(n)
	switch {
	case err == nil:
		s.readings.Add(uint64(n))
	case errors.Is(err, ingest.ErrOverloaded):
		s.rejected.Inc()
		ack.Status = edgev1.Status_STATUS_OVERLOADED
	case ctx.Err() != nil || errors.Is(err, ingest.ErrClosed):
		return nil, status.Error(codes.Unavailable, "shutting down")
	default:
		ack.Status = edgev1.Status_STATUS_INTERNAL
	}
	return ack, nil
}

func (s *Server) Tasks(r *edgev1.TaskRequest, st edgev1.Ingest_TasksServer) error {
	if !validDevice(r.DeviceId) {
		return status.Error(codes.InvalidArgument, "bad device_id")
	}
	sub := &taskSub{ch: make(chan *edgev1.Task, 16), done: make(chan struct{})}
	s.tmu.Lock()
	if old := s.tasks[r.DeviceId]; old != nil {
		close(old.done)
	}
	s.tasks[r.DeviceId] = sub
	s.tmu.Unlock()
	defer func() {
		s.tmu.Lock()
		if s.tasks[r.DeviceId] == sub {
			delete(s.tasks, r.DeviceId)
		}
		s.tmu.Unlock()
	}()

	s.fleet.Touch(r.DeviceId)
	for {
		select {
		case t := <-sub.ch:
			if err := st.Send(t); err != nil {
				return err
			}
		case <-sub.done:
			return status.Error(codes.AlreadyExists, "replaced by a newer task stream")
		case <-st.Context().Done():
			return st.Context().Err()
		}
	}
}

// Dispatch matches fleet.Dispatch. Errors mean the scheduler should try another device.
func (s *Server) Dispatch(dev string, t fleet.Task) error {
	s.tmu.Lock()
	sub := s.tasks[dev]
	s.tmu.Unlock()
	if sub == nil {
		return ErrNoTaskStream
	}
	select {
	case sub.ch <- &edgev1.Task{Id: t.ID, Kind: t.Kind, Payload: t.Payload, Attempts: uint32(t.Attempts)}:
		return nil
	default:
		return ErrTaskBusy
	}
}
