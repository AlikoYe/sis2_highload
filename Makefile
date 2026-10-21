GOBIN := $(shell go env GOPATH)/bin
BUF := $(GOBIN)/buf

.DEFAULT_GOAL := build
.PHONY: tools gen build test vet tidy

tools:
	go install github.com/bufbuild/buf/cmd/buf@latest
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# buf resolves local plugins via PATH, so prepend GOBIN for this call.
gen:
	$(BUF) lint
	PATH="$(GOBIN):$$PATH" $(BUF) generate

build:
	go build ./...

test:
	go test ./...

vet:
	go vet ./...

tidy:
	go mod tidy
