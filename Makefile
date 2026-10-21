GOBIN := $(shell go env GOPATH)/bin
BUF := $(GOBIN)/buf
DATABASE_URL ?= postgres://notif:notif@localhost:5433/notif?sslmode=disable

.DEFAULT_GOAL := build
.PHONY: tools gen build test test-pg vet tidy db-up db-down db-migrate

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
	go test -race ./...

# -p 1: several packages truncate the same deliveries table.
test-pg:
	DATABASE_URL='$(DATABASE_URL)' go test -race -p 1 ./...

vet:
	go vet ./...

tidy:
	go mod tidy

db-up:
	docker compose up -d --wait postgres

db-down:
	docker compose down

db-migrate:
	@for f in migrations/*.sql; do \
		echo "applying $$f"; \
		docker compose exec -T postgres psql -q -v ON_ERROR_STOP=1 -U notif -d notif < $$f || exit 1; \
	done
