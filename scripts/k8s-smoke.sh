#!/usr/bin/env bash
# applies deploy/k8s/gateway.yaml to a throwaway kind cluster and waits for both
# replicas; then deletes a pod and checks it comes back on the same volume.
set -euo pipefail

img=edgemesh-gateway:ci
dump() {
  kubectl -n edge get pods,pvc -o wide || true
  kubectl -n edge describe pods || true
  kubectl -n edge logs -l app=gateway --tail=40 --prefix || true
}
trap 'rc=$?; [ $rc -ne 0 ] && dump; kind delete cluster >/dev/null 2>&1 || true; exit $rc' EXIT

docker build --target gateway -t $img .
go build -o bin/ ./cmd/gencert
bin/gencert -out certs

kind create cluster --wait 120s
kind load docker-image $img

kubectl create namespace edge
kubectl -n edge create secret generic gateway-secrets \
  --from-literal=EDGE_AUTH_TOKEN=ci-token \
  --from-literal=EDGE_CLOUD_TOKEN=ci-cloud \
  --from-literal=EDGE_CLOUD_URL=http://127.0.0.1:9/v1/ingest
kubectl -n edge create secret tls gateway-tls --cert=certs/server.pem --key=certs/server-key.pem

sed "s#ghcr.io/monaaoua9/edgemesh-gateway:latest#$img#" deploy/k8s/gateway.yaml | kubectl apply -f -
kubectl -n edge rollout status statefulset/gateway --timeout=240s

for p in gateway-0 gateway-1; do
  kubectl -n edge exec $p -- /gateway healthcheck https://127.0.0.1:8080/readyz
done

kubectl -n edge delete pod gateway-0 --wait=true
kubectl -n edge rollout status statefulset/gateway --timeout=240s
kubectl -n edge exec gateway-0 -- /gateway healthcheck https://127.0.0.1:8080/readyz
kubectl -n edge get pvc
echo "k8s ok"
