APP := mini-redis-server
PKG := ./...

.PHONY: all build test fmt vet run clean smoke benchmark benchmark-dict benchmark-observability rehash-demo encodings-demo persistence-demo eviction-demo session-demo replication-demo sentinel-demo cluster-demo resp3-demo observability-demo

all: fmt vet test build

build:
	mkdir -p bin
	go build -o bin/$(APP) ./cmd/mini-redis-server

test:
	go test $(PKG)

fmt:
	gofmt -w $$(find . -name '*.go' -type f)

vet:
	go vet $(PKG)

run:
	go run ./cmd/mini-redis-server -addr 127.0.0.1:6379

smoke: build
	bash ./scripts/smoke.sh

benchmark: build
	bash ./scripts/compare-engines.sh

benchmark-dict:
	go test -run '^$$' -bench 'Benchmark(Dict|GoMap)' ./internal/dict

benchmark-observability:
	go test -run '^$$' -bench 'Benchmark(Counter|Histogram)' -benchmem ./internal/observability

rehash-demo:
	go run ./examples/rehash

encodings-demo:
	go run ./examples/encodings

persistence-demo:
	go run ./examples/persistence

eviction-demo:
	go run ./examples/eviction

session-demo:
	go run ./examples/sessions

replication-demo:
	go run ./examples/replication

sentinel-demo:
	go run ./examples/sentinel

cluster-demo:
	go run ./examples/cluster

resp3-demo:
	go run ./examples/resp3

observability-demo:
	go run ./examples/observability

clean:
	rm -rf bin coverage.out *.aof *.mrdb cpu.pprof mem.pprof
