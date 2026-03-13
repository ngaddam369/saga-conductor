BINARY   := saga-conductor
MODULE   := github.com/ngaddam369/saga-conductor
PROTO_DIR := proto/saga/v1

.PHONY: build fmt lint test test-integration proto verify clean tidy

## build: compile the server binary
build:
	go build -o bin/$(BINARY) ./cmd/server

## fmt: format all Go source files in place
fmt:
	gofmt -w .

## lint: run golangci-lint (includes govet and gofmt checks)
lint:
	golangci-lint run ./...

## test: run all tests with race detector and show coverage summary
test:
	go test -v -race -count=1 -coverprofile=coverage.out $$(go list ./... | grep -v /proto/)
	@go tool cover -func=coverage.out | grep -E "^total|^github"

## test-integration: run integration tests (requires -tags integration)
test-integration:
	go test -v -race -count=1 -tags integration ./...

## proto: regenerate Go code from .proto files
## Requires: protoc + protoc-gen-go + protoc-gen-go-grpc
proto:
	protoc \
		--go_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_out=. \
		--go-grpc_opt=paths=source_relative \
		-I . \
		-I $(HOME)/.local/include \
		$(PROTO_DIR)/saga.proto

## verify: run the full checklist (build → lint → mod verify → vulncheck → test)
verify: build lint test
	go mod verify
	govulncheck ./...

## clean: remove build artifacts
clean:
	rm -rf bin/ coverage.out

## tidy: tidy and verify go modules
tidy:
	go mod tidy
	go mod verify
