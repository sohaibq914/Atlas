MODULE  := github.com/sohaibq914/atlas
VERSION ?= dev
LDFLAGS := -X $(MODULE)/internal/version.value=$(VERSION)
BINS    := atlas-node atlasctl

.PHONY: all build test lint fmt proto tools clean

all: lint test build

build:
	mkdir -p bin
	for b in $(BINS); do go build -ldflags "$(LDFLAGS)" -o bin/$$b ./cmd/$$b; done

test:
	go test ./... -race -count=1

lint:
	gofmt -l . | tee /dev/stderr | (! read)
	go vet ./...

fmt:
	gofmt -w .

tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

proto:
	protoc -I proto \
		--go_out=. --go_opt=module=$(MODULE) \
		--go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
		proto/atlas/v1/*.proto

clean:
	rm -rf bin gen
