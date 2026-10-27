package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand/v2"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"edgemesh/internal/grpcapi/edgev1"
	"edgemesh/internal/tlsutil"
)

func clientTLS(o opts) (*tls.Config, error) {
	if o.tlsCA == "" {
		return nil, nil
	}
	c, err := tlsutil.Client(o.tlsCA)
	if err != nil {
		return nil, err
	}
	if o.tlsCert != "" {
		kp, err := tls.LoadX509KeyPair(o.tlsCert, o.tlsKey)
		if err != nil {
			return nil, err
		}
		c.Certificates = []tls.Certificate{kp}
	}
	return c, nil
}

type grpcPool struct {
	conns []*grpc.ClientConn
}

func newGRPCPool(o opts) (*grpcPool, error) {
	creds := credentials.TransportCredentials(insecure.NewCredentials())
	if o.tls != nil {
		creds = credentials.NewTLS(o.tls)
	}
	p := &grpcPool{}
	for range max(o.grpcConns, 1) {
		cc, err := grpc.NewClient(o.addr, grpc.WithTransportCredentials(creds))
		if err != nil {
			return nil, err
		}
		p.conns = append(p.conns, cc)
	}
	return p, nil
}

func (p *grpcPool) pick(n int) edgev1.IngestClient {
	return edgev1.NewIngestClient(p.conns[n%len(p.conns)])
}

func (p *grpcPool) close() {
	for _, c := range p.conns {
		c.Close()
	}
}

func grpcDevice(ctx context.Context, o opts, n int, st *stats, pool *grpcPool) {
	id := fmt.Sprintf("%s-%06d", o.prefix, n)
	rng := rand.New(rand.NewPCG(uint64(n), 7))
	temp := 70 + rng.Float64()*15
	cli := pool.pick(n)
	backoff := 500 * time.Millisecond

	for ctx.Err() == nil {
		sctx := ctx
		if o.token != "" {
			sctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+o.token)
		}
		stream, err := cli.Stream(sctx)
		if err != nil {
			st.errs.Add(1)
			if !sleep(ctx, backoff) {
				return
			}
			backoff = min(backoff*2, 10*time.Second)
			continue
		}
		backoff = 500 * time.Millisecond
		st.connected.Add(1)

		sleep(ctx, time.Duration(rng.Int64N(int64(o.interval))))
		tick := time.NewTicker(o.interval)
		var seq uint64
		failed := false
		for !failed {
			select {
			case <-ctx.Done():
				failed = true
			case <-tick.C:
				temp += (rng.Float64() - 0.5) * 0.6
				temp = min(max(temp, 40), 105)
				v := temp
				if rng.Float64() < o.spike {
					v += 35 + rng.Float64()*20
				}
				seq++
				now := time.Now().UnixMicro()
				b := &edgev1.ReadingBatch{DeviceId: id, Seq: seq, Readings: []*edgev1.Reading{
					{Metric: "engine_temp", Value: v, TsUs: now},
					{Metric: "vibration", Value: 2 + rng.Float64()*0.8, TsUs: now},
				}}
				st.sent.Add(1)
				t0 := time.Now()
				if err := stream.Send(b); err != nil {
					failed = true
					break
				}
				ack, err := stream.Recv()
				if err != nil {
					failed = true
					break
				}
				if ack.Status != edgev1.Status_STATUS_OK {
					st.errs.Add(1)
					continue
				}
				st.rtt.Observe(time.Since(t0))
				st.acked.Add(1)
			}
		}
		tick.Stop()
		stream.CloseSend()
		st.connected.Add(-1)
		if ctx.Err() != nil {
			return
		}
		st.errs.Add(1)
	}
}
