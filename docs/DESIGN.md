# edgemesh: design decisions

The short list of choices behind the gateway. Data flow, WAL format, concurrency model and topics: [ARCHITECTURE.md](ARCHITECTURE.md). Running it: [OPERATIONS.md](OPERATIONS.md).

## Scope

An edge IoT gateway. Devices talk MQTT 3.1.1 or gRPC (TLS / mTLS); the gateway flags anomalies locally, tracks fleet health, reassigns work, and forwards telemetry to a cloud API that may be down for minutes or hours.

## Decisions

**Ingest never touches the network.** It only appends to the WAL; a separate forwarder reads from the WAL at its own checkpoint. An uplink outage therefore can't slow or lose ingestion; it just makes the queue longer.

**A QoS 1 PUBACK is sent only after the reading's WAL batch is written.** Under `-sync=always` that means fsynced. Under the default `interval` mode a power cut can lose at most `-sync-every` worth of acknowledged data, while a process crash loses nothing because the OS already has the bytes. This is the main durability/throughput dial, and it's set to the cheaper side by default.

**Group commit in the pipeline.** A worker drains up to `MaxBatch` queued jobs and does one write (and one fsync if configured) for all of them. Many devices share the cost of the disk, which is how a single gateway keeps up with thousands of 1 msg/s devices.

**Upstream is at-least-once, deduplicated by batch id.** Each batch carries `X-Batch-ID: <gw>:<first>-<last>`; the checkpoint advances only after a 2xx. A crash between send and checkpoint replays the batch and the cloud dedupes on the id. Exactly-once would need the cloud's cooperation, which the gateway doesn't assume.

**Slow start after an outage.** On failure the forwarder shrinks to a probe-sized batch and backs off with jitter (doubling, capped at 30 s); on success the batch doubles each round up to `-max-batch`. A recovering uplink is probed before it's flooded. A permanently rejected batch (400/413/422) is dead-lettered and skipped, so one poison batch never blocks the queue.

**Bounded disk, oldest data goes first.** When a long outage fills `-wal-max-mb`, the oldest segments are dropped and counted in `edge_wal_evicted_total`. Dropping is loud and measured rather than a surprise.

**Sharding by device id keeps per-device order.** Same shard worker, same WAL order. QoS 1 waits (with a timeout) for queue space; QoS 0 is shed immediately.

**Anomaly baselines that don't chase the anomaly.** Per `(device, metric)`: hard min/max, then rate of change, then a z-score against a Welford baseline during warmup and an EWMA afterwards. Anomalous points don't update the baseline, but five in a row re-center it, since a real process shift shouldn't alert forever. Baselines live in memory and re-warm after a restart.

**A device may only publish under its own id.** The client id must match the topic, otherwise the publish is refused and counted (`edge_messages_spoofed_total`).

**Per-connection goroutine with a 512-byte read buffer.** Simple, and enough for the 12,000 TLS devices in the README measurements. TLS handshakes happen inside that goroutine under the connect timeout, so a handshake storm can't block the accept loop.

## How it's checked

WAL crash recovery, outage-then-recovery without loss, restart from checkpoint, and "ack implies durable" end to end over both MQTT and gRPC. Soaks on CI runners with repeated `kill -9` restarts and periodic cloud outages. Fuzzing of the MQTT parsers and wire decoder, `make race`, `govulncheck` and `golangci-lint`.

## Not done on purpose

MQTT QoS 2, retained messages and persistent sessions. Use TLS on anything that leaves a private network.
