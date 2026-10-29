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

Diagram, guarantees, WAL format, concurrency model: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md). The decisions and trade-offs in short: [docs/DESIGN.md](docs/DESIGN.md).
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
Docker: every release tag publishes a multi-arch (amd64, arm64) gateway image to GHCR, `docker pull ghcr.io/monaaoua9/edgemesh-gateway:latest`. Release binaries for Linux, macOS and Windows are attached to the GitHub release.

Kubernetes: `deploy/k8s/gateway.yaml` (StatefulSet + PVC, PDB, NetworkPolicy).
`./bin/gateway -h` lists every flag; `EDGE_AUTH_TOKEN` / `EDGE_CLOUD_TOKEN` set tokens without leaking them in `ps`.

## Measured (loopback, generator and gateway on the same machine; first four rows on a Windows 11 laptop, last three on GitHub Actions Linux runners)

| Scenario | Result |
|---|---|
| 12,000 concurrent **TLS** MQTT devices, 1 msg/s each | ~11.9k acked msg/s, 0 shed / refused, heap ~150 MB, GC pause total 36 ms over 75 s |
| 8,000 MQTT + 5,000 gRPC streams (TLS), cloud down for 8 s mid-run | 928,752 accepted = 928,752 delivered, 0 duplicates, 0 lost |
| WAL | ~1 µs/record in 64-record group commits, ~4.9 µs single append |
| Anomaly detector | ~400 ns / reading; histogram `Observe` ~90 ns |
| Linux CI runner (`make smoke`): 2,000 MQTT devices, 4 s cloud outage | 42,241 accepted = 42,241 delivered, 0 dup / lost; ack-to-durable latency p50 12 µs, p99 80 µs, p99.9 160 µs |
| Linux CI runner, 1 h soak (`scripts/soak.sh`): 5,000 MQTT devices (~5k msg/s), 14 `kill -9` gateway restarts, a 5 s cloud outage every 90 s | 17.86 M readings acked to devices, 21.43 M delivered upstream, 0 lost, 0 evicted; heap max 38 MB, goroutines flat at 5,018, p99 ack-to-durable max 0.13 ms |
| Linux CI runner, 20 min soak, 10,000 MQTT devices (~9.9k msg/s), 4 `kill -9` restarts, outages every 90 s | 11.84 M readings acked, 14.21 M delivered upstream, 0 lost, 0 evicted; heap max 72 MB, goroutines flat at 10,018, p99 ack-to-durable max 0.19 ms |

Windows has a coarse monotonic clock in Go, so sub-millisecond percentiles read
low there. The Linux rows come from shared 4-vCPU GitHub runners with generator and
gateway on the same box, so absolute numbers are a floor; the soaks (`soak` workflow,
manual) are about no loss and no leak under kill -9 and outages, not peak throughput.

## Deployment checks in CI

* Docker image: built, and the full compose stack (gateway, mock cloud, simulator, Prometheus, Grafana) is brought up and checked.
* Kubernetes: `deploy/k8s/gateway.yaml` is applied to a kind cluster, both replicas must become ready over TLS, and a deleted pod must come back on its volume.
* systemd: the unit goes through `systemd-analyze verify` (syntax, paths, user). The sandbox options have not been exercised on a running host.

## Quality gates

`make race` (all packages under the race detector), `make fuzz` (MQTT parsers,
wire decoder; 1.2M+ execs clean), `govulncheck` and `golangci-lint` in CI,
distroless non-root image. Tests cover WAL crash recovery, outage-then-recovery
without loss, restart from checkpoint, ack-implies-durable end to end (MQTT and
gRPC), spoofing, mTLS handshake rejection, cert rotation, detector behaviour,
fleet state machine and task reassignment.

## Scope notes

Not implemented: MQTT QoS 2, retained messages and persistent sessions
(QoS 0/1 cover the telemetry use case).
Anomaly baselines live in memory and re-warm after a restart. Use `-tls-cert`
on anything that leaves a private network.

## Requirements

Go 1.26+ to build; Docker optional. Linux, macOS and Windows all build and pass
the tests; latency numbers above are from Windows, so treat them as a floor.

## License

MIT, see [LICENSE](LICENSE). Contributions: [CONTRIBUTING.md](CONTRIBUTING.md).
Security reports: [SECURITY.md](SECURITY.md).
