#!/usr/bin/env bash
# brings the whole compose stack up and checks each piece answers.
set -euo pipefail

f=deploy/docker-compose.yml
trap 'docker compose -f $f logs --tail 40 gateway cloud simulator || true; docker compose -f $f down -v' EXIT

docker compose -f $f up -d --build

wait_for() {
  for _ in $(seq 60); do curl -sf "$1" >/dev/null && return 0; sleep 2; done
  echo "timeout waiting for $1"; return 1
}
wait_for localhost:8080/readyz
wait_for localhost:9091/-/ready
wait_for localhost:3000/api/health

sleep 25
readings=$(curl -s localhost:9090/stats | grep -o '"readings":[0-9]*' | cut -d: -f2)
echo "cloud readings=$readings"
[ "${readings:-0}" -gt 0 ] || { echo "nothing reached the cloud"; exit 1; }

up=$(curl -s localhost:9091/api/v1/targets | grep -o '"health":"up"' | wc -l)
echo "prometheus targets up=$up"
[ "$up" -ge 1 ] || { echo "prometheus scrapes nothing"; exit 1; }
