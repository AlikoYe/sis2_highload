# Benchmark summary

Inputs: bench_investigate_20261021-233628.csv, bench_quick_20261021-230911.csv, bench_sample_20261021-231010.csv

Medians across repetitions. Closed-mode cells: **ok/s · p99 · server CPU per request**. A digit suffix (e.g. `h2c4`) = that protocol over that many connections. Batch ok/s counts calls (100 events each).

## (a) Closed loop, memory store

### publish/small, 5s runs

| concurrency | grpc | rest | h2c |
|---|---|---|---|
| c=1 | 20.5k · 101µs · 45.2µs | 20.6k · 104µs · 38.4µs | 14.7k · 138µs · 76.5µs |
| c=64 | 153.5k · 960µs · 22.6µs | 89.3k · 1.53ms · 48.2µs | 38.5k · 2.21ms · 78.0µs |

### publish/small, 30s runs

| concurrency | grpc | grpc4 | rest | h2c | h2c4 | h2c8 |
|---|---|---|---|---|---|---|
| c=64 | 154.9k · 948µs · 23.0µs | 182.3k · 1.07ms · 25.2µs | 89.4k · 1.54ms · 48.7µs | 38.8k · 2.27ms · 78.2µs | 89.3k · 1.79ms · 54.6µs | 61.4k · 2.15ms · 75.8µs |
| c=256 | – | – | 89.6k · 4.83ms · 49.6µs | 62.5k · 9.62ms · 65.9µs | – | 72.4k · 7.12ms · 65.1µs |

## (d) Open loop (fixed arrival rate)

Cells: **p50 / p99 · dropped**. Latency is measured from the scheduled send time.

| store, rate | grpc | rest | h2c |
|---|---|---|---|
| memory, 1000/s (5s) | 155µs / 306µs · 0 | 181µs / 593µs · 0 | 213µs / 448µs · 0 |

## Ratios vs REST/HTTP1.1 (same configuration)

>1 throughput is better; <1 p99 and CPU/req are better.

| configuration | protocol | throughput × | p99 × | server CPU/req × |
|---|---|---:|---:|---:|
| memory publish/small closed c=1 5s | grpc | 0.99 | 0.97 | 1.18 |
| memory publish/small closed c=1 5s | h2c | 0.72 | 1.33 | 1.99 |
| memory publish/small closed c=64 5s | grpc | 1.72 | 0.63 | 0.47 |
| memory publish/small closed c=64 5s | h2c | 0.43 | 1.44 | 1.62 |
| memory publish/small closed c=64 30s | grpc | 1.73 | 0.62 | 0.47 |
| memory publish/small closed c=64 30s | grpc4 | 2.04 | 0.70 | 0.52 |
| memory publish/small closed c=64 30s | h2c | 0.43 | 1.48 | 1.60 |
| memory publish/small closed c=64 30s | h2c4 | 1.00 | 1.17 | 1.12 |
| memory publish/small closed c=64 30s | h2c8 | 0.69 | 1.40 | 1.55 |
| memory publish/small closed c=256 30s | h2c | 0.70 | 1.99 | 1.33 |
| memory publish/small closed c=256 30s | h2c8 | 0.81 | 1.48 | 1.31 |
| memory publish/small open 1000/s 5s | grpc | 1.00 | 0.52 | 1.25 |
| memory publish/small open 1000/s 5s | h2c | 1.00 | 0.76 | 1.58 |

## All groups

p50/p95/p99 show median (min–max across reps). CPU per successful request.

| configuration | protocol | reps | ok/s | p50 | p95 | p99 | server CPU/req | client CPU/req | errors | retries | dropped |
|---|---|---:|---:|---|---|---|---:|---:|---:|---:|---:|
| memory publish/small closed c=1 5s | grpc | 1 | 20.5k | 45µs | 67µs | 101µs | 45.2µs | 44.1µs | 0 | 0 | 0 |
| memory publish/small closed c=1 5s | rest | 1 | 20.6k | 44µs | 65µs | 104µs | 38.4µs | 42.3µs | 0 | 0 | 0 |
| memory publish/small closed c=1 5s | h2c | 1 | 14.7k | 63µs | 90µs | 138µs | 76.5µs | 53.8µs | 0 | 0 | 0 |
| memory publish/small closed c=64 5s | grpc | 1 | 153.5k | 385µs | 710µs | 960µs | 22.6µs | 20.9µs | 0 | 0 | 0 |
| memory publish/small closed c=64 5s | rest | 1 | 89.3k | 689µs | 1.01ms | 1.53ms | 48.2µs | 49.6µs | 0 | 0 | 0 |
| memory publish/small closed c=64 5s | h2c | 1 | 38.5k | 1.65ms | 1.88ms | 2.21ms | 78.0µs | 51.9µs | 0 | 0 | 0 |
| memory publish/small closed c=64 30s | grpc | 2 | 154.9k | 382µs (382µs–383µs) | 704µs (699µs–710µs) | 948µs (934µs–963µs) | 23.0µs | 20.5µs | 0 | 0 | 0 |
| memory publish/small closed c=64 30s | grpc4 | 1 | 182.3k | 310µs | 689µs | 1.07ms | 25.2µs | 23.2µs | 0 | 0 | 0 |
| memory publish/small closed c=64 30s | rest | 2 | 89.4k | 687µs (684µs–690µs) | 1.01ms (994µs–1.03ms) | 1.54ms (1.51ms–1.56ms) | 48.7µs | 49.6µs | 0 | 0 | 0 |
| memory publish/small closed c=64 30s | h2c | 2 | 38.8k | 1.64ms (1.61ms–1.66ms) | 1.87ms (1.85ms–1.89ms) | 2.27ms (2.25ms–2.28ms) | 78.2µs | 51.4µs | 0 | 0 | 0 |
| memory publish/small closed c=64 30s | h2c4 | 1 | 89.3k | 685µs | 1.22ms | 1.79ms | 54.6µs | 48.4µs | 0 | 0 | 0 |
| memory publish/small closed c=64 30s | h2c8 | 1 | 61.4k | 1.05ms | 1.45ms | 2.15ms | 75.8µs | 65.3µs | 0 | 0 | 0 |
| memory publish/small closed c=256 30s | rest | 1 | 89.6k | 2.81ms | 3.35ms | 4.83ms | 49.6µs | 50.1µs | 0 | 0 | 0 |
| memory publish/small closed c=256 30s | h2c | 1 | 62.5k | 6.59ms | 8.24ms | 9.62ms | 65.9µs | 45.1µs | 0 | 0 | 0 |
| memory publish/small closed c=256 30s | h2c8 | 1 | 72.4k | 4.10ms | 5.12ms | 7.12ms | 65.1µs | 57.1µs | 0 | 0 | 0 |
| memory publish/small open 1000/s 5s | grpc | 1 | 1000 | 155µs | 201µs | 306µs | 160.0µs | 899.8µs | 0 | 0 | 0 |
| memory publish/small open 1000/s 5s | rest | 1 | 1000 | 181µs | 287µs | 593µs | 128.0µs | 876.2µs | 0 | 0 | 0 |
| memory publish/small open 1000/s 5s | h2c | 1 | 1000 | 213µs | 291µs | 448µs | 202.0µs | 878.4µs | 0 | 0 | 0 |
