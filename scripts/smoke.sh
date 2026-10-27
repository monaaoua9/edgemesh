#!/usr/bin/env bash
# end to end on real sockets: simulator -> gateway -> cloudmock, with a short
# uplink outage in the middle. fails if anything accepted didn't arrive.
set -euo pipefail

DEVICES=${DEVICES:-2000}
RUN=${RUN:-20s}
work=$(mktemp -d)
pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; wait 2>/dev/null || true; rm -rf "$work"; }
trap cleanup EXIT

bin=${BIN:-bin}
[ -x "$bin/gateway" ] || { echo "run make build first" >&2; exit 1; }

"$bin/cloudmock" -addr 127.0.0.1:9090 >"$work/cloud.log" 2>&1 & pids+=($!)
"$bin/gateway" -data "$work/data" -cloud http://127.0.0.1:9090/v1/ingest \
  -mqtt 127.0.0.1:1883 -grpc 127.0.0.1:9000 -admin 127.0.0.1:8080 >"$work/gw.log" 2>&1 & pids+=($!)

for _ in $(seq 50); do curl -sf localhost:8080/healthz >/dev/null && break; sleep 0.2; done
curl -sf localhost:8080/healthz >/dev/null || { cat "$work/gw.log"; exit 1; }

"$bin/simulator" -addr 127.0.0.1:1883 -devices "$DEVICES" -ramp 1000 -duration "$RUN" >"$work/sim.log" 2>&1 &
sim=$!
sleep 8
curl -sf -XPOST 'localhost:9090/admin/outage?seconds=4' >/dev/null
wait $sim || true

# let the forwarder drain
for _ in $(seq 60); do
  lag=$(curl -s localhost:8080/metrics | awk '$1=="edge_forward_lag_records"{print $2}')
  [ "${lag%.*}" = "0" ] && break
  sleep 1
done

m=$(curl -s localhost:8080/metrics)
acc=$(echo "$m" | awk '$1=="edge_readings_accepted_total"{print $2}')
shed=$(echo "$m" | awk '$1=="edge_readings_dropped_total"{print $2}')
stats=$(curl -s localhost:9090/stats)
got=$(echo "$stats" | grep -o "\"readings\":[0-9]*" | cut -d: -f2)
dup=$(echo "$stats" | grep -o "\"duplicate_batches\":[0-9]*" | cut -d: -f2)

echo "accepted=$acc delivered=$got shed=$shed duplicate_batches=$dup"
echo "$m" | grep '^edge_ingest_latency_seconds' || true
echo "$m" | grep -E '^(go_memstats_heap_alloc_bytes|process_resident_memory_bytes) ' || true

[ "${acc%.*}" -gt 0 ] || { echo "nothing accepted"; exit 1; }
[ "${acc%.*}" = "${got%.*}" ] || { echo "accepted != delivered"; tail -20 "$work/gw.log"; exit 1; }
