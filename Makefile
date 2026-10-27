VERSION ?= dev

.PHONY: build test race bench fuzz vet fmt-check proto certs docker compose-up loadtest clean

build:
	go build -trimpath -ldflags="-X main.version=$(VERSION)" -o bin/ ./cmd/...

test:
	go test -count=1 ./...

race:
	go test -race -count=1 ./...

bench:
	go test -run xxx -bench . -benchmem ./internal/wal ./internal/anomaly ./internal/metrics

fuzz:
	go test -fuzz FuzzParsers -fuzztime 30s ./internal/mqtt
	go test -fuzz FuzzUnmarshalBinary -fuzztime 15s ./internal/telemetry

vet:
	go vet ./...

fmt-check:
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)

# needs protoc, protoc-gen-go, protoc-gen-go-grpc on PATH
proto:
	protoc -I api --go_out=. --go_opt=module=edgemesh --go-grpc_out=. --go-grpc_opt=module=edgemesh api/edge/v1/edge.proto

certs: build
	./bin/gencert -out certs

docker:
	docker build --target gateway --build-arg VERSION=$(VERSION) -t edgemesh-gateway:$(VERSION) .
	docker build --target tools -t edgemesh-tools:$(VERSION) .

compose-up:
	docker compose -f deploy/docker-compose.yml up --build

# 10k plaintext devices against a local gateway (start cloudmock + gateway first)
loadtest: build
	./bin/simulator -devices 10000 -ramp 500 -duration 60s

clean:
	rm -rf bin data certs
