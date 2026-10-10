.PHONY: all build test lint fuzz faultbench run-demo clean

BIN_DIR := bin
BINS := $(BIN_DIR)/steerlb $(BIN_DIR)/chaosbackend $(BIN_DIR)/loadgen $(BIN_DIR)/faultbench

all: build test

build:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/steerlb ./cmd/steerlb
	go build -o $(BIN_DIR)/chaosbackend ./cmd/chaosbackend
	go build -o $(BIN_DIR)/loadgen ./cmd/loadgen
	go build -o $(BIN_DIR)/faultbench ./cmd/faultbench

test:
	go test -race -v ./...

lint:
	@test -z "$$(gofmt -l .)" || (echo "Unformatted files:" && gofmt -l . && exit 1)
	go vet ./...

fuzz:
	go test -run=XXX -fuzz=FuzzHeaders -fuzztime=30s ./internal/proxy

faultbench:
	go run ./cmd/faultbench -scenarios all -seed 42 -out bench/faultbench.json

run-demo: build
	@echo "Starting demo with 5 chaos backends and steerlb..."
	@bash scripts/run_demo.sh

clean:
	rm -rf $(BIN_DIR) bench/faultbench.json
