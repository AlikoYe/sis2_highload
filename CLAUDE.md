# sis2_highload — Notification ingest service

## What this is
University project (Backend for High-Load Environments). Go service implementing the producer-facing interface of a notification service: producers publish events; the service creates one delivery per channel (push/email) idempotently.

## Requirements (from SIS #1 design)
- ~3,000 events/s ingest, at-least-once delivery, delivery p99 < 5 s; ingest API p99 < 100 ms.
- Primary protocol: gRPC (internal, high-volume, typed contract, deadlines, HTTP/2).
- REST/JSON exists only as a measurement baseline of the same interface.

## Architecture rules
- One service layer (`internal/service`) holds ALL logic: validation, building deliveries, idempotency, response building.
- Transports (`internal/grpcapi`, `internal/httpapi`) are thin adapters: decode -> call service -> encode, plus error mapping. No business logic in transports.
- Storage behind an interface in `internal/store` (memory impl for protocol benchmarks, Postgres impl for real idempotency). SIS #3 will tune Postgres behind this interface; SIS #4 will add a cache decorator and Kafka publishing in the service. Keep these extension points clean.
- Idempotency: `event_id` from the producer; Postgres uses `INSERT ... ON CONFLICT (event_id, channel) DO NOTHING RETURNING`; losers read existing rows and return the same response. Never check-then-insert.
- Error mapping: invalid input -> gRPC INVALID_ARGUMENT / HTTP 400; overload -> RESOURCE_EXHAUSTED / 429; temporary failure -> UNAVAILABLE / 503.
- Timeouts everywhere: honour context deadlines; server checks ctx before work; pass ctx to DB calls.
- Retries only on the client side, one shared policy for both protocols: exponential backoff with full jitter, max attempts, retry budget, same event_id on every attempt.

## Code style
- Idiomatic, small, readable Go. Standard library first (net/http with Go 1.22 routing, log/slog).
- Very few comments; no long comment blocks. Comment only non-obvious "why".
- No unnecessary abstractions or frameworks.
- Tests next to code (`_test.go`); table-driven where it helps.

## Workflow
- Work in phases; only do what the current phase asks. Stop and report at the end of each phase (what changed, commands run, results).
- `make gen` regenerates code; `make test` (`go test -race ./...`) must pass before finishing a phase.
- Postgres: `make db-up db-migrate` starts it on localhost:5433 and applies `migrations/*.sql`; `make test-pg` runs tests with `DATABASE_URL` set (Postgres tests skip without it); `make db-down` stops it.
- Run the server: `go run ./cmd/server` (memory store; gRPC :50051, REST :8080) or `go run ./cmd/server -store=postgres -db-url=$DATABASE_URL -h2c-addr=:8081` (adds REST over h2c). Flags fall back to env (`STORE`, `DATABASE_URL`, `GRPC_ADDR`, ...); see `-h`.
- Error classification lives only in `internal/apierr`; transports map its result to gRPC codes / HTTP statuses. Cross-protocol tests are in `internal/e2e`.
- Clients: `internal/client` (`NewGRPC`, `NewREST`, `NewRESTH2C`) implement one `Client` interface; every call gets `CallTimeout` (default 300ms) and runs through `internal/retry` (exp backoff + full jitter, MaxAttempts, optional token-bucket `Budget`, honours retry-after). Only UNAVAILABLE/RESOURCE_EXHAUSTED and connection errors are retried, always with the same event_id.
- Fault injection (`internal/faults`, both transports): `-fault-fail-before`, `-fault-fail-after` (lost response after commit), `-fault-latency`, `-fault-latency-jitter` (env `FAULT_*`). Off by default. `make test-pg` uses `-p 1` because several packages truncate the same table.
- Measurement (all tools use `internal/workload` messages: small/medium/large/batch): `make payloadsize` (body + wire bytes -> `results/payload_sizes.{md,csv}`), `make bench-codec` (proto vs JSON codec benchmarks + benchstat), `bin/loadgen -protocol grpc|rest|h2c -op publish|batch|status -mode closed|open ...` (one CSV row per run, HDR latency, open mode measured from scheduled send time).
- Bench presets: `make bench-quick` (~1 min, validates tooling), `make bench-sample` (3 x 30s), `make bench-full` (full matrix x3, ~2h, needs Postgres). Each run starts a fresh server (memory store capped by `-memory-max-events`, default 1M in the script); results in `results/bench_<preset>_<ts>.csv` + `_server_cpu.csv`, machine info in `results/env.txt`. Commit CSV/md results, not `results/logs/`.
- Loadgen `-conns K` opens K gRPC ClientConns / K h2c transports (round-robin; ignored for rest). Matrix variants are named protocol+conns, e.g. `h2c8`. Server `-pprof-addr` (env `PPROF_ADDR`) serves pprof on its own listener; `make bench-investigate` profiles runs into `results/profiles/`. `scripts/bench.sh full --estimate` prints the run count and duration; the full matrix adds `h2c4` (env `FULL_H2C_EXTRA`, see `results/h2c_investigation.md`); protocols run interleaved per configuration, order rotated per repetition.
- Report run: `scripts/bench.sh final` (~1h, 138 runs of 20s+3s; env in `results/env_final.txt`).
- `make summary` (`cmd/benchsummary`) turns bench CSVs into `results/summary.md` + `results/summary.csv` (one row per group, for charts) (medians across reps, pivot tables, ratios vs REST); `SUMMARY_INPUT=<csv>` restricts input. Labels: `rel-*` = fault-injection runs, `prof-*` = profiled runs (excluded).
- Store behaviour is covered by the shared suite `internal/store/storetest`; every Store implementation must pass it.
- Commit at the end of each phase with a clear message.

## Not in SIS #2 scope
Index tuning, partitioning, replicas (SIS #3); Redis, Kafka, workers, Kubernetes (SIS #4).
