APP := mini-redis-server
PKG := ./...

.PHONY: all build test fmt vet run clean smoke benchmark benchmark-dict rehash-demo

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

rehash-demo:
	go run ./examples/rehash

clean:
	rm -rf bin coverage.out *.aof
