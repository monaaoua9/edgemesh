# Distributed Real-Time Edge IoT Ingestion & Gateway Mesh

[![ci](https://github.com/monaaoua9/edgemesh/actions/workflows/ci.yml/badge.svg)](https://github.com/monaaoua9/edgemesh/actions/workflows/ci.yml)

An edge gateway in Go that ingests telemetry from thousands of devices over
**MQTT** and **gRPC** (TLS / mTLS), survives uplink outages with a disk-backed
**store-and-forward** queue, flags anomalies locally in microseconds, tracks
fleet health and reassigns work, and exposes latency / memory metrics and pprof.
Only third-party dependencies: gRPC and protobuf.

```
 devices ──MQTT/TLS──┐
                     ├─▶ [ handler ▶ sharded pipeline ▶ WAL ] ─▶ forwarder ──HTTPS──▶ cloud
 devices ──gRPC/TLS──┘          │             │          ▲            ▲
                                │       anomaly detector  └─ack after  └ checkpoint
                         fleet registry ─▶ task scheduler    durable write
```

Diagram, guarantees, WAL format, concurrency model: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).
Running it in production: [docs/OPERATIONS.md](docs/OPERATIONS.md) · [SECURITY.md](SECURITY.md).

## Guarantees

* **Ack means durable.** A QoS1 `PUBACK` / gRPC `Ack` is sent only after the reading is in the WAL.
* **No loss across uplink outages or restarts**; upstream is at-least-once with batch ids for dedupe.
* **Per-device ordering** preserved end to end.
* **Bounded resources**: connection cap, bounded queues (QoS0 is shed, QoS1 gets backpressure), disk cap with oldest-first eviction and an alert before it matters.

## Layout

```
api/edge/v1      gRPC contract (edge.proto)
cmd/gateway      the edge gateway          cmd/gencert    dev CA + certs
cmd/simulator    N fake devices, mqtt or grpc, optional TLS/mTLS
cmd/cloudmock    fake cloud: dedupes batches, /stats, POST /admin/outage?seconds=N
internal/mqtt    MQTT 3.1.1 codec, server, client (fuzzed parsers)
internal/grpcapi gRPC ingest service
internal/wal     segmented CRC'd write-ahead log, torn-tail recovery, retention
internal/forward store-and-forward uploader: backoff+jitter, slow start, checkpoint
internal/ingest  sharded worker pipeline with group commit + MQTT handler
internal/anomaly threshold / rate-of-change / EWMA z-score
internal/fleet   heartbeat state machine + task scheduler
internal/metrics lock-free histogram, Prometheus exposition, runtime gauges
internal/tlsutil TLS/mTLS configs with hot cert reload
internal/admin   /metrics /status /fleet /alerts /tasks /debug/pprof
deploy/          docker-compose (+Prometheus+Grafana), k8s, systemd, alert rules
```

## Quick start

```sh
make build
./bin/cloudmock &
./bin/gateway -cloud http://127.0.0.1:9090/v1/ingest &
./bin/simulator -devices 3000 -ramp 1500            # MQTT
./bin/simulator -proto grpc -addr 127.0.0.1:9000 -devices 3000   # gRPC

curl localhost:8080/status
curl localhost:8080/metrics | grep edge_
curl -XPOST 'localhost:9090/admin/outage?seconds=30'   # watch lag grow, then drain
```

With TLS / mTLS:

```sh
make certs
./bin/gateway -tls-cert certs/server.pem -tls-key certs/server-key.pem -tls-client-ca certs/ca.pem ...
./bin/simulator -tls-ca certs/ca.pem -tls-cert certs/client.pem -tls-key certs/client-key.pem
```

Full stack with dashboards: `make compose-up` (Grafana on :3000).
Kubernetes: `deploy/k8s/gateway.yaml` (StatefulSet + PVC, PDB, NetworkPolicy).
`./bin/gateway -h` lists every flag; `EDGE_AUTH_TOKEN` / `EDGE_CLOUD_TOKEN` set tokens without leaking them in `ps`.

## Measured (laptop, loopback, Windows 11, generator and gateway on the same CPU)

| Scenario | Result |
|---|---|
| 12,000 concurrent **TLS** MQTT devices, 1 msg/s each | ~11.9k acked msg/s, 0 shed / refused, heap ~150 MB, GC pause total 36 ms over 75 s |
| 8,000 MQTT + 5,000 gRPC streams (TLS), cloud down for 8 s mid-run | 928,752 accepted = 928,752 delivered, 0 duplicates, 0 lost |
| WAL | ~1 µs/record in 64-record group commits, ~4.9 µs single append |
| Anomaly detector | ~400 ns / reading; histogram `Observe` ~90 ns |

Windows has a coarse monotonic clock in Go, so sub-millisecond percentiles read
low there; run the simulator on Linux for real tail latencies.

## Quality gates

`make race` (all packages under the race detector), `make fuzz` (MQTT parsers,
wire decoder; 1.2M+ execs clean), `govulncheck` and `golangci-lint` in CI,
distroless non-root image. Tests cover WAL crash recovery, outage-then-recovery
without loss, restart from checkpoint, ack-implies-durable end to end (MQTT and
gRPC), spoofing, mTLS handshake rejection, cert rotation, detector behaviour,
fleet state machine and task reassignment.

## Scope notes

Not implemented: MQTT QoS 2, retained messages and persistent sessions
(QoS 0/1 cover the telemetry use case), task downlink over gRPC (MQTT only).
Anomaly baselines live in memory and re-warm after a restart. Use `-tls-cert`
on anything that leaves a private network.
