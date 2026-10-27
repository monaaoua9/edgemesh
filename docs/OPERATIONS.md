# Operations runbook

## Sizing (measured, single process, loopback, TLS on)

| Load | Result |
|---|---|
| 12,000 concurrent TLS MQTT devices @ 1 msg/s | ~11.9k acked msg/s, 0 shed, heap ~150 MB, `process_sys_bytes` ~600 MB, p99 receipt-to-durable ~160 ms (load generator and gateway shared one laptop CPU) |
| 8,000 MQTT + 5,000 gRPC streams, 8s cloud outage mid-run | 928,752 accepted = 928,752 delivered, 0 duplicates, 0 lost |

Rule of thumb: ~12 KB heap + ~16 KB goroutine stack per TLS MQTT connection;
plain TCP is roughly half. Budget 1 GB per ~20k TLS devices plus headroom.

## OS tuning (Linux)

```
ulimit -n 200000                      # or LimitNOFILE in the unit / ulimits in compose
sysctl -w net.core.somaxconn=4096
sysctl -w net.ipv4.tcp_max_syn_backlog=8192
sysctl -w net.ipv4.ip_local_port_range="1024 65535"   # load generators only
```

Put the data directory on SSD/NVMe. `-sync=interval` (default) is the right
trade-off for most fleets; use `-sync=always` only if losing 100 ms of acked
data on power loss is unacceptable (group commit softens the throughput hit).

## Flags that matter

| Flag | Default | Notes |
|---|---|---|
| `-wal-max-mb` | 1024 | Disk cap. Size for (ingest rate x longest outage to ride out). Past it the oldest data is dropped and `DataLossRisk` fires |
| `-workers` / `-queue` | NumCPU / 8192 | Raise `-queue` for burstier fleets; shedding shows in `edge_readings_dropped_total` / `_rejected_total` |
| `-min-batch` / `-max-batch` | 16 / 2048 | probe size after a failure / steady state |
| `-catchup-rate` | 0 | cap records/s while draining a backlog so recovery does not stampede the cloud |
| `-suspect-after` / `-offline-after` | 15s / 30s | keep at least 3x the device heartbeat period |
| `-tls-client-ca` | off | turns on mTLS for MQTT and gRPC |

## Disk sizing example

50k devices x 1 reading/s x ~45 B/record is about 2.2 MB/s, or 8 GB/hour. A 4 h
outage needs ~32 GB: set `-wal-max-mb` accordingly and size the PVC above it.

## Certificates

Server cert/key are re-read when the cert file's mtime changes (checked at most
every 10 s), so cert-manager / certbot renewals need no restart. Local CA for
testing: `make certs`.

## Alerts and what to do

| Alert | Meaning | Action |
|---|---|---|
| UplinkDown | uploads failing for 2+ min | check cloud / DNS / credentials (`/status` shows `last_error`); data is safe on disk |
| BacklogGrowing | uploading slower than ingesting | cloud capacity, raise `-max-batch`, check `-catchup-rate` |
| DataLossRisk | disk cap reached, data evicted | raise `-wal-max-mb` / disk; restore the uplink |
| IngestShedding | queue full | more `-workers` / `-queue` / CPU, or another replica |
| WALWriteErrors | disk failing or full; acks withheld | free space or replace disk; QoS1 devices retry |
| FleetOfflineSpike | many devices silent | site or network issue, not the gateway |

## Backup / recovery

* Data dir = `wal/*.wal` + `wal/checkpoint`. Copy it while stopped for a consistent state; while running, copy the checkpoint first.
* Deleting `checkpoint` replays everything still on disk (the cloud dedupes batches).
* A torn tail after a crash is repaired on start (`truncating torn tail` log line).

## Upgrades

Rolling restart is safe: on SIGTERM the gateway stops accepting, flushes the
queue to the WAL, then exits; devices reconnect and re-send un-acked QoS1
publishes. With a StatefulSet each pod keeps its own PVC.

## Debugging

```
curl -k https://gw:8080/status
curl -k 'https://gw:8080/fleet?state=offline'
curl -k https://gw:8080/alerts
go tool pprof http://gw:8080/debug/pprof/heap
go tool pprof 'http://gw:8080/debug/pprof/profile?seconds=30'
```
