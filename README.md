# sis2_highload

Notification ingest service in Go (Backend for High-Load Environments).
Producers publish events over gRPC; a REST/JSON endpoint of the same interface exists as a measurement baseline.

```
make tools   # install buf, protoc-gen-go, protoc-gen-go-grpc
make gen     # buf lint + generate into gen/
make build   # go build ./...
make test    # go test ./...
```
