# h2c investigation (Phase 5a)

Why is REST over cleartext HTTP/2 (h2c) slower than REST over HTTP/1.1 and gRPC?

Setup: memory store, publish small, closed loop, fresh server per run, GOMAXPROCS 6 server / 6 loadgen on an
Apple M4 Pro (see `env.txt`). Raw data: `bench_investigate_20261021-233628.csv` (30 s runs) and
`h2c_conns_sweep.csv` (15 s runs, 2 reps each). Profiles: `profiles/` (20 s / 15 s server CPU profiles
taken after warm-up, in separate runs from the measured ones).

## Results

| protocol | conns | c | ok/s | p50 | p99 | server CPU/req | client CPU/req | server cores used |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| grpc | 1 | 64 | 155,602 | 382µs | 934µs | 22.9µs | 20.4µs | 3.57 |
| grpc | 4 | 64 | 182,268 | 310µs | 1.07ms | 25.2µs | 23.2µs | 4.59 |
| rest (HTTP/1.1) | per request | 64 | 89,813 | 684µs | 1.52ms | 48.6µs | 49.4µs | 4.36 |
| h2c | 1 | 64 | 38,321 | 1.66ms | 2.26ms | 79.5µs | 52.0µs | 3.05 |
| h2c | 2 | 64 | 68,256 ¹ | 919µs | 1.89ms | 61.4µs | 43.5µs | 4.20 |
| h2c | 4 | 64 | 89,283 | 685µs | 1.79ms | 54.6µs | 48.4µs | 4.88 |
| h2c | 8 | 64 | 61,388 | 1.05ms | 2.15ms | 75.8µs | 65.3µs | 4.65 |
| rest (HTTP/1.1) | per request | 256 | 89,627 | 2.81ms | 4.83ms | 49.6µs | 50.1µs | 4.45 |
| h2c | 1 → 2 ² | 256 | 62,534 | 6.60ms | 9.62ms | 65.9µs | 45.1µs | 4.12 |
| h2c | 4 | 256 | 117,203 ¹ | 1.97ms | 6.22ms | 42.2µs | 38.2µs | 4.94 |
| h2c | 8 | 256 | 72,428 | 4.10ms | 7.12ms | 65.1µs | 57.1µs | 4.71 |

¹ mean of the two 15 s sweep runs (they agree within 2%); the other rows are single 30 s runs, and the
sweep reproduced them (h2c1 39–40k, h2c4 88–89k, h2c8 61–62k at c=64; h2c8 69–70k at c=256).
² Go's HTTP/2 server allows 250 concurrent streams per connection, so at c=256 the client transparently
opened a second TCP connection (verified with `lsof`: 1 connection at c=64, 2 at c=256).

## Where the server CPU goes (per successful request, from the profiles)

| | ok/s | total | write path: `Flush` | read path: `readFrameHeader` | scheduler: `runtime.findRunnable` |
|---|---:|---:|---:|---:|---:|
| h2c, 1 conn | 38k | 74µs | 7.4µs (`http2bufferedWriter.Flush`) | 10.0µs (`http2readFrameHeader`) | **33.6µs** |
| h2c, 4 conns | 88k | 51µs | 14.6µs | 15.0µs | 6.6µs |
| h2c, 8 conns | 62k | 70µs | **31.2µs** | 11.8µs | 7.8µs |
| gRPC, 1 conn | 154k | 22µs | 2.2µs (`transport.(*bufWriter).Flush`) | 1.9µs (`transport.(*framer).readFrame`) | 7.3µs |

Top flat entries of the h2c 1-connection server profile: `syscall.rawsyscalln` 24%, `runtime.usleep` 23%,
`runtime.kevent` 23%, `runtime.pthread_cond_wait` 13%, `runtime.pthread_cond_signal` 6%; cumulatively
`runtime.schedule` 49%, `runtime.selectgo` 11%, `runtime.lock2` 12%. In the gRPC profile the syscall share
per request is 5x smaller and `selectgo`/`lock2` are negligible.

## Conclusion

**The single-connection h2c gap is per-connection serialization in net/http's HTTP/2 server, not HTTP/2 itself.**

- In `net/http`'s bundled HTTP/2 server every frame of a connection is read by one
  `(*http2serverConn).readFrames` goroutine and handed over a channel to the single
  `(*http2serverConn).serve` goroutine, which dispatches streams to `runHandler` goroutines and collects
  their writes over channels again; frames go out one batch at a time through `writeFrameAsync` →
  `http2flushFrameWriter.writeFrame` → `(*http2bufferedWriter).Flush`. With 64 streams on one connection
  this chain is the bottleneck: the server uses only 3.05 of its 6 cores, and half of the samples
  (`runtime.schedule`/`findRunnable`, flat `usleep`, `kevent`, `pthread_cond_wait/signal`) are threads
  spinning, parking and waking around the channel handoffs (`selectgo`, `lock2`) — 34µs per request of
  pure coordination, vs 7µs for gRPC.
- Spreading the same load over more connections removes the bottleneck: 2 connections → 1.8x,
  4 connections → 2.3x (89k ok/s, equal to HTTP/1.1 at c=64, and 117k vs 90k at c=256).
- Past that point the cost moves to syscalls: with fewer streams per connection each `Flush` carries fewer
  frames, so the write path costs 7 → 15 → 31µs per request for 1 → 4 → 8 connections, and h2c8 falls back to
  61k ok/s. The client shows the same trend (client CPU/req 52 → 48 → 65µs), so it is not server-only.
- gRPC runs over the same HTTP/2 framing on a single connection at 154k ok/s because grpc-go's transport
  is built for multiplexing: a dedicated `loopyWriter` batches frames from all streams into one
  `bufWriter.Flush` (2.2µs/request) and `framer.readFrame` reads through a buffer (1.9µs/request).

Limitations: only the server was profiled; the client transport (one read loop per connection) likely
contributes the same kind of serialization, which the client CPU/req trend supports but does not prove.
The scheduler time on macOS includes idle-spinning threads, so "CPU/req" overstates useful work for
the 1-connection case; the throughput cap and the idle cores are the primary evidence.

## Decision for the full matrix

`h2c4` (h2c over 4 connections) is added next to `grpc`, `rest`, `h2c` for publish small and batch at
c=64 and c=256 (`FULL_H2C_EXTRA`, default `h2c4`). The connection count changes h2c results by more than
2x, so the matrix should show both the default single-connection client and a tuned one. 4 rather than 8
because 8 connections were clearly worse at both concurrencies (61k vs 88k at c=64, 70k vs 117k at c=256).
