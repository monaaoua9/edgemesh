#!/usr/bin/env bash
# long run with abuse: steady fleet, uplink outages, gateway kill -9 + restart on
# the same data dir. samples runtime gauges into $OUT (csv). passes only if every
# reading the devices saw acked made it upstream.
set -euo pipefail

DURATION=${DURATION:-600}          # seconds
DEVICES=${DEVICES:-3000}
OUTAGE_EVERY=${OUTAGE_EVERY:-90}   # seconds between 5s cloud outages
KILL_EVERY=${KILL_EVERY:-240}      # seconds between kill -9 of the gateway
OUT=${OUT:-soak.csv}
bin=${BIN:-bin}

work=$(mktemp -d)
pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; wait 2>/dev/null || true; rm -rf "$work"; }
trap cleanup EXIT

start_gw() {
  "$bin/gateway" -data "$work/data" -cloud http://127.0.0.1:9090/v1/ingest \
    -mqtt 127.0.0.1:1883 -grpc 127.0.0.1:9000 -admin 127.0.0.1:8080 >>"$work/gw.log" 2>&1 &
  gw=$!; pids+=($gw)
  for _ in $(seq 100); do curl -sf localhost:8080/healthz >/dev/null && return 0; sleep 0.2; done
  echo "gateway did not come up"; tail -20 "$work/gw.log"; return 1
}

metric() { echo "$m" | awk -v k="$1" '$1==k{print $2; exit}'; }

"$bin/cloudmock" -addr 127.0.0.1:9090 >"$work/cloud.log" 2>&1 & pids+=($!)
start_gw
"$bin/simulator" -addr 127.0.0.1:1883 -devices "$DEVICES" -ramp 1000 -duration "${DURATION}s" >"$work/sim.log" 2>&1 &
sim=$!; pids+=($sim)

echo "t,heap_mb,goroutines,wal_mb,lag,queue,p99_ms,accepted,restarts" >"$OUT"
restarts=0
max_heap=0
t0=$(date +%s)
last_outage=0
last_kill=0
while kill -0 $sim 2>/dev/null; do
  sleep 10
  now=$(( $(date +%s) - t0 ))
  m=$(curl -s localhost:8080/metrics || true)
  heap=$(metric process_heap_alloc_bytes); gor=$(metric process_goroutines)
  walb=$(metric edge_wal_bytes); lag=$(metric edge_forward_lag_records)
  q=$(metric edge_ingest_queue_depth); acc=$(metric edge_readings_accepted_total)
  p99=$(echo "$m" | awk '/^edge_ingest_latency_seconds\{quantile="0.99"\}/{print $2*1000}')
  hmb=$(awk -v h="${heap:-0}" 'BEGIN{printf "%.0f", h/1048576}')
  wmb=$(awk -v h="${walb:-0}" 'BEGIN{printf "%.1f", h/1048576}')
  echo "$now,$hmb,${gor:-0},$wmb,${lag:-0},${q:-0},${p99:-0},${acc:-0},$restarts" >>"$OUT"
  [ "$hmb" -gt "$max_heap" ] && max_heap=$hmb

  if [ $((now - last_outage)) -ge "$OUTAGE_EVERY" ]; then
    curl -sf -XPOST 'localhost:9090/admin/outage?seconds=5' >/dev/null || true
    last_outage=$now
  fi
  if [ $((now - last_kill)) -ge "$KILL_EVERY" ] && [ "$now" -lt $((DURATION - 60)) ]; then
    kill -9 $gw; wait $gw 2>/dev/null || true
    restarts=$((restarts + 1)); last_kill=$now
    start_gw
  fi
done
wait $sim || true

for _ in $(seq 120); do
  m=$(curl -s localhost:8080/metrics || true)
  [ "$(metric edge_forward_lag_records | cut -d. -f1)" = "0" ] && break
  sleep 1
done

acked=$(grep -o 'acked=[0-9]*' "$work/sim.log" | tail -1 | cut -d= -f2)
stats=$(curl -s localhost:9090/stats)
got=$(echo "$stats" | grep -o '"readings":[0-9]*' | cut -d: -f2)
evicted=$(metric edge_wal_evicted_total)

echo "duration=${DURATION}s devices=$DEVICES restarts=$restarts max_heap_mb=$max_heap"
echo "device-side acked=$acked  cloud delivered=$got  wal evicted=${evicted:-0}"
grep -E 'final:' "$work/sim.log" | tail -1 || true

# acked implies durable, upstream is at-least-once and deduped by batch id,
# so delivered can only be >= what the devices saw acked
[ "${acked:-0}" -gt 0 ] || { echo "devices never got an ack"; exit 1; }
[ "$got" -ge "$acked" ] || { echo "LOST: acked=$acked delivered=$got"; exit 1; }
[ "${evicted%.*}" = "0" ] || { echo "wal evicted records"; exit 1; }
