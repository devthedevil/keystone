# Keystone build and test targets.
#
# Keystone has no third-party dependencies, so there is no vendoring, no
# dependency download step, and nothing to go stale between `make build` on a
# laptop and `make build` in CI.

BIN       := bin
VERSION   ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS   := -X main.version=$(VERSION)
GOFLAGS   ?=
RACE      ?= -race

.PHONY: all build test test-short test-chaos bench vet fmt fmt-check lint clean run-local docker help

all: fmt-check vet test build

## build: compile the server and the CLI into ./bin
build:
	@mkdir -p $(BIN)
	go build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BIN)/keystoned ./cmd/keystoned
	go build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BIN)/keystonectl ./cmd/keystonectl
	@echo "built $(VERSION) -> $(BIN)/"

## test: everything, with the race detector
test:
	go test ./... $(RACE) -count=1 -timeout 10m

## test-short: skip the chaos suite
test-short:
	go test ./... $(RACE) -count=1 -short -timeout 5m

## test-chaos: fault injection and linearizability only
test-chaos:
	go test ./test/ $(RACE) -count=1 -v -timeout 10m

## bench: Go microbenchmarks
bench:
	go test ./... -run '^$$' -bench . -benchmem

vet:
	go vet ./...

fmt:
	gofmt -w .

## fmt-check: fail if anything is unformatted
fmt-check:
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "these files need gofmt:"; echo "$$unformatted"; exit 1; \
	fi

## lint: vet plus formatting
lint: fmt-check vet

clean:
	rm -rf $(BIN) /tmp/keystone-local

## run-local: three replicas on loopback (ports 9001-9003, admin token "secret")
run-local: build
	@./deploy/run-local.sh

## docker: build the container image
docker:
	docker build -t keystone:$(VERSION) -f deploy/Dockerfile .

help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/^## /  /'
