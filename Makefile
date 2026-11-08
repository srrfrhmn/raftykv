GO ?= go

.PHONY: all build test race vet importlint lint proto proto-check ci

all: build test

build:
	$(GO) build ./...

test:
	$(GO) test ./...

# The race detector needs cgo (and a C compiler on Windows).
race:
	CGO_ENABLED=1 $(GO) test -race ./...

vet:
	$(GO) vet ./...

# Determinism boundary check (docs/DESIGN.md §9.1).
importlint:
	$(GO) run ./tools/importlint .

lint:
	golangci-lint run ./...

# Code generation. buf and the protoc plugins are pinned in go.mod (tool directive).
proto:
	$(GO) tool buf lint
	$(GO) tool buf generate

# Fails if generated code is out of date.
proto-check: proto
	git diff --exit-code -- api/

ci: vet importlint race proto-check
