# edgemesh: benchmarks

Measured 2026-10-29.

| | |
|---|---|
| CPU | Intel i5-8365U, 4 cores / 8 threads, 1.6 GHz base |
| RAM / disk | 8 GB, SSD |
| OS | Windows 11 |
| Toolchain | Go 1.27.0 |

## Component benchmarks

```
go test -run xxx -bench . -benchmem -count 3 ./internal/wal ./internal/anomaly ./internal/metrics
```

Three runs; median with min–max in brackets. 0 allocations per op in every row.

| benchmark | ns/op |
|---|---:|
| WAL `Append`, sync `never` | 3,994 (3,418 – 4,454) |
| WAL `Append`, sync `interval` | 6,509 (4,930 – 6,965) |
| WAL `AppendBatch` of 64 records | 32,018 (25,421 – 33,790), about 500 per record |
| anomaly detector `Observe` | 118 (104 – 120) |
| metrics histogram `Observe` | 61 (58 – 67) |

Batching matters: appending 64 records in one call costs about 500 ns per record against about 4,000 for a single append, an 8× difference. That's the group commit the pipeline relies on to keep up with thousands of devices.

These are the component costs on a laptop; they are not end-to-end gateway throughput.

## Differences from the README

The README quotes ~1 µs/record for 64-record batches, ~4.9 µs for a single append and ~400 ns for the detector. Today's run is lower for the batch and the detector and in range for the single append. The README figures may have been measured differently (for example, the detector through the pipeline rather than the `Observe` call alone); I haven't tried to reconcile them.

## Not rerun today

The scenario figures in the README were not repeated:

- 12,000 TLS MQTT devices, and 8,000 MQTT + 5,000 gRPC streams with an 8 s cloud outage (Windows laptop);
- the CI smoke test and the 1-hour and 20-minute soaks with repeated `kill -9` restarts (GitHub Actions runners).

They take minutes to hours and need the load generator and cloud mock set up; the soaks run in CI (`soak` workflow). Treat them as earlier results, with the caveat the README already gives: generator and gateway share one machine, so absolute numbers are a floor, and Windows' coarse monotonic clock in Go makes sub-millisecond percentiles read low.
