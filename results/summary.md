# Benchmark summary

Inputs: bench_final_20261021-235445.csv

Medians across repetitions. Closed-mode cells: **ok/s · p99 · server CPU per request**. A digit suffix (e.g. `h2c4`) = that protocol over that many connections. Batch ok/s counts calls (100 events each).

## (a) Closed loop, memory store

### batch/batch, 20s runs

| concurrency | grpc | rest | h2c | h2c4 |
|---|---|---|---|---|
| c=1 | 3812 · 681µs · 321.0µs | 2501 · 779µs · 345.9µs | 2250 · 809µs · 473.1µs | – |
| c=64 | 13.4k · 15.09ms · 249.5µs | 11.7k · 16.73ms · 376.1µs | 9573 · 21.65ms · 465.1µs | 10.8k · 15.79ms · 440.1µs |
| c=256 | 14.1k · 28.27ms · 240.4µs | 12.3k · 63.71ms · 370.9µs | 10.2k · 91.45ms · 455.8µs | 11.2k · 74.24ms · 429.3µs |

### publish/large, 20s runs

| concurrency | grpc | rest | h2c |
|---|---|---|---|
| c=1 | 19.5k · 116µs · 50.7µs | 15.9k · 155µs · 63.5µs | 11.8k · 191µs · 106.8µs |
| c=64 | 147.0k · 1.05ms · 28.5µs | 81.2k · 2.46ms · 65.9µs | 28.8k · 4.78ms · 118.4µs |
| c=256 | 176.2k · 3.04ms · 26.8µs | 83.1k · 7.47ms · 66.0µs | 46.1k · 14.72ms · 95.7µs |

### publish/small, 20s runs

| concurrency | grpc | rest | h2c | h2c4 |
|---|---|---|---|---|
| c=1 | 22.3k · 103µs · 41.3µs | 22.8k · 100µs · 34.8µs | 16.3k · 138µs · 69.3µs | – |
| c=64 | 156.8k · 923µs · 22.8µs | 89.8k · 1.51ms · 48.7µs | 39.4k · 2.24ms · 77.0µs | 87.9k · 1.81ms · 55.5µs |
| c=256 | 202.5k · 2.87ms · 21.6µs | 90.3k · 4.66ms · 49.1µs | 64.2k · 8.86ms · 65.0µs | 118.7k · 6.05ms · 42.8µs |

## (b) GetStatus, memory store

### status/small, 20s runs

| concurrency | grpc | rest | h2c |
|---|---|---|---|
| c=64 | 151.9k · 837µs · 23.6µs | 90.3k · 1.59ms · 45.8µs | 59.2k · 1.64ms · 51.3µs |
| c=256 | 177.0k · 2.08ms · 22.9µs | 93.3k · 4.15ms · 43.0µs | 79.1k · 5.90ms · 48.1µs |

## (c) Closed loop, Postgres store

### publish/small, 20s runs

| concurrency | grpc | rest | h2c |
|---|---|---|---|
| c=1 | 2731 · 527µs · 132.7µs | 2762 · 529µs · 122.7µs | 2354 · 655µs · 199.2µs |
| c=64 | 18.0k · 4.83ms · 94.7µs | 17.5k · 5.08ms · 102.6µs | 16.9k · 5.10ms · 133.1µs |

## (d) Open loop (fixed arrival rate)

Cells: **p50 / p99 · dropped**. Latency is measured from the scheduled send time.

| store, rate | grpc | rest | h2c |
|---|---|---|---|
| memory, 3000/s (20s) | 75µs / 156µs · 0 | 73µs / 164µs · 0 | 98µs / 178µs · 0 |
| postgres, 3000/s (20s) | 381µs / 774µs · 0 | 344µs / 631µs · 0 | 380µs / 633µs · 0 |

## (e) Reliability (server drops 5% of responses after commit)

| protocol | config | reps | ok/s | p50 | p99 | retries | errors |
|---|---|---:|---:|---:|---:|---:|---:|
| grpc | publish/small c=64 | 1 | 43.4k | 84µs | 41.89ms | 45652 | 100 |
| rest | publish/small c=64 | 1 | 44.4k | 67µs | 41.50ms | 46469 | 123 |
| h2c | publish/small c=64 | 1 | 35.7k | 375µs | 42.40ms | 37542 | 83 |

## Ratios vs REST/HTTP1.1 (same configuration)

>1 throughput is better; <1 p99 and CPU/req are better.

| configuration | protocol | throughput × | p99 × | server CPU/req × |
|---|---|---:|---:|---:|
| memory batch/batch closed c=1 20s | grpc | 1.52 | 0.87 | 0.93 |
| memory batch/batch closed c=1 20s | h2c | 0.90 | 1.04 | 1.37 |
| memory batch/batch closed c=64 20s | grpc | 1.15 | 0.90 | 0.66 |
| memory batch/batch closed c=64 20s | h2c | 0.82 | 1.29 | 1.24 |
| memory batch/batch closed c=64 20s | h2c4 | 0.92 | 0.94 | 1.17 |
| memory batch/batch closed c=256 20s | grpc | 1.14 | 0.44 | 0.65 |
| memory batch/batch closed c=256 20s | h2c | 0.83 | 1.44 | 1.23 |
| memory batch/batch closed c=256 20s | h2c4 | 0.91 | 1.17 | 1.16 |
| memory publish/large closed c=1 20s | grpc | 1.23 | 0.75 | 0.80 |
| memory publish/large closed c=1 20s | h2c | 0.74 | 1.23 | 1.68 |
| memory publish/large closed c=64 20s | grpc | 1.81 | 0.43 | 0.43 |
| memory publish/large closed c=64 20s | h2c | 0.35 | 1.95 | 1.80 |
| memory publish/large closed c=256 20s | grpc | 2.12 | 0.41 | 0.41 |
| memory publish/large closed c=256 20s | h2c | 0.55 | 1.97 | 1.45 |
| memory publish/small closed c=1 20s | grpc | 0.98 | 1.03 | 1.18 |
| memory publish/small closed c=1 20s | h2c | 0.72 | 1.38 | 1.99 |
| memory publish/small closed c=64 20s | grpc | 1.75 | 0.61 | 0.47 |
| memory publish/small closed c=64 20s faults | grpc | 0.98 | 1.01 | 0.88 |
| memory publish/small closed c=64 20s | h2c | 0.44 | 1.48 | 1.58 |
| memory publish/small closed c=64 20s faults | h2c | 0.80 | 1.02 | 1.86 |
| memory publish/small closed c=64 20s | h2c4 | 0.98 | 1.20 | 1.14 |
| memory publish/small closed c=256 20s | grpc | 2.24 | 0.62 | 0.44 |
| memory publish/small closed c=256 20s | h2c | 0.71 | 1.90 | 1.32 |
| memory publish/small closed c=256 20s | h2c4 | 1.31 | 1.30 | 0.87 |
| memory status/small closed c=64 20s | grpc | 1.68 | 0.52 | 0.52 |
| memory status/small closed c=64 20s | h2c | 0.66 | 1.03 | 1.12 |
| memory status/small closed c=256 20s | grpc | 1.90 | 0.50 | 0.53 |
| memory status/small closed c=256 20s | h2c | 0.85 | 1.42 | 1.12 |
| memory publish/small open 3000/s 20s | grpc | 1.00 | 0.95 | 1.59 |
| memory publish/small open 3000/s 20s | h2c | 1.00 | 1.09 | 1.67 |
| postgres publish/small closed c=1 20s | grpc | 0.99 | 1.00 | 1.08 |
| postgres publish/small closed c=1 20s | h2c | 0.85 | 1.24 | 1.62 |
| postgres publish/small closed c=64 20s | grpc | 1.03 | 0.95 | 0.92 |
| postgres publish/small closed c=64 20s | h2c | 0.97 | 1.00 | 1.30 |
| postgres publish/small open 3000/s 20s | grpc | 1.00 | 1.23 | 1.64 |
| postgres publish/small open 3000/s 20s | h2c | 1.00 | 1.00 | 1.55 |

## All groups

p50/p95/p99 show median (min–max across reps). CPU per successful request.

| configuration | protocol | reps | ok/s | p50 | p95 | p99 | server CPU/req | client CPU/req | errors | retries | dropped |
|---|---|---:|---:|---|---|---|---:|---:|---:|---:|---:|
| memory batch/batch closed c=1 20s | grpc | 3 | 3812 | 201µs (196µs–204µs) | 312µs (310µs–316µs) | 681µs (622µs–700µs) | 321.0µs | 227.5µs | 0 | 0 | 0 |
| memory batch/batch closed c=1 20s | rest | 3 | 2501 | 342µs (338µs–355µs) | 474µs (473µs–491µs) | 779µs (736µs–788µs) | 345.9µs | 241.5µs | 0 | 0 | 0 |
| memory batch/batch closed c=1 20s | h2c | 3 | 2250 | 381µs (380µs–385µs) | 545µs (543µs–550µs) | 809µs (801µs–817µs) | 473.1µs | 323.7µs | 0 | 0 | 0 |
| memory batch/batch closed c=64 20s | grpc | 3 | 13.4k | 3.37ms (3.35ms–3.39ms) | 6.04ms (5.97ms–6.08ms) | 15.09ms (14.92ms–15.10ms) | 249.5µs | 329.7µs | 0 | 0 | 0 |
| memory batch/batch closed c=64 20s | rest | 3 | 11.7k | 3.31ms (3.28ms–3.33ms) | 11.63ms (11.56ms–12.16ms) | 16.73ms (16.56ms–17.79ms) | 376.1µs | 404.4µs | 0 | 0 | 0 |
| memory batch/batch closed c=64 20s | h2c | 3 | 9573 | 5.16ms (5.16ms–5.20ms) | 15.31ms (14.87ms–15.41ms) | 21.65ms (21.57ms–22.18ms) | 465.1µs | 390.6µs | 0 | 0 | 0 |
| memory batch/batch closed c=64 20s | h2c4 | 3 | 10.8k | 4.39ms (4.36ms–4.43ms) | 11.46ms (11.11ms–11.82ms) | 15.79ms (15.27ms–16.51ms) | 440.1µs | 419.7µs | 0 | 0 | 0 |
| memory batch/batch closed c=256 20s | grpc | 3 | 14.1k | 14.20ms (14.05ms–14.26ms) | 22.82ms (22.82ms–23.21ms) | 28.27ms (27.65ms–28.86ms) | 240.4µs | 324.9µs | 0 | 0 | 0 |
| memory batch/batch closed c=256 20s | rest | 3 | 12.3k | 14.40ms (14.22ms–14.95ms) | 43.10ms (42.56ms–43.52ms) | 63.71ms (63.52ms–65.73ms) | 370.9µs | 377.5µs | 0 | 0 | 0 |
| memory batch/batch closed c=256 20s | h2c | 3 | 10.2k | 20.94ms (20.32ms–21.65ms) | 60.19ms (55.10ms–60.73ms) | 91.45ms (86.02ms–92.73ms) | 455.8µs | 415.8µs | 0 | 0 | 0 |
| memory batch/batch closed c=256 20s | h2c4 | 3 | 11.2k | 16.93ms (16.31ms–16.93ms) | 51.84ms (51.45ms–52.58ms) | 74.24ms (72.58ms–76.16ms) | 429.3µs | 400.5µs | 0 | 0 | 0 |
| memory publish/large closed c=1 20s | grpc | 3 | 19.5k | 45µs (44µs–46µs) | 70µs (70µs–75µs) | 116µs (116µs–121µs) | 50.7µs | 47.3µs | 0 | 0 | 0 |
| memory publish/large closed c=1 20s | rest | 3 | 15.9k | 56µs (56µs–61µs) | 78µs (76µs–85µs) | 155µs (152µs–159µs) | 63.5µs | 45.3µs | 0 | 0 | 0 |
| memory publish/large closed c=1 20s | h2c | 3 | 11.8k | 78µs (77µs–78µs) | 106µs (105µs–106µs) | 191µs (190µs–192µs) | 106.8µs | 61.7µs | 0 | 0 | 0 |
| memory publish/large closed c=64 20s | grpc | 3 | 147.0k | 380µs (380µs–381µs) | 790µs (789µs–792µs) | 1.05ms (1.04ms–1.07ms) | 28.5µs | 23.8µs | 0 | 0 | 0 |
| memory publish/large closed c=64 20s | rest | 3 | 81.2k | 703µs (702µs–704µs) | 1.65ms (1.64ms–1.65ms) | 2.46ms (2.42ms–2.46ms) | 65.9µs | 59.1µs | 0 | 0 | 0 |
| memory publish/large closed c=64 20s | h2c | 3 | 28.8k | 2.14ms (2.12ms–2.15ms) | 2.83ms (2.79ms–2.83ms) | 4.78ms (4.75ms–4.86ms) | 118.4µs | 61.7µs | 0 | 0 | 0 |
| memory publish/large closed c=256 20s | grpc | 3 | 176.2k | 1.28ms (1.28ms–1.29ms) | 2.34ms (2.32ms–2.35ms) | 3.04ms (3.01ms–3.06ms) | 26.8µs | 22.0µs | 0 | 0 | 0 |
| memory publish/large closed c=256 20s | rest | 3 | 83.1k | 2.92ms (2.91ms–2.92ms) | 5.05ms (5.00ms–5.09ms) | 7.47ms (7.38ms–7.78ms) | 66.0µs | 59.1µs | 0 | 0 | 0 |
| memory publish/large closed c=256 20s | h2c | 3 | 46.1k | 8.47ms (8.44ms–8.63ms) | 11.99ms (11.88ms–12.04ms) | 14.72ms (14.18ms–14.81ms) | 95.7µs | 55.4µs | 0 | 0 | 0 |
| memory publish/small closed c=1 20s | grpc | 3 | 22.3k | 40µs (40µs–46µs) | 64µs (62µs–73µs) | 103µs (100µs–112µs) | 41.3µs | 39.5µs | 0 | 0 | 0 |
| memory publish/small closed c=1 20s | rest | 3 | 22.8k | 40µs (39µs–45µs) | 58µs (57µs–66µs) | 100µs (98µs–107µs) | 34.8µs | 37.7µs | 0 | 0 | 0 |
| memory publish/small closed c=1 20s | h2c | 3 | 16.3k | 56µs (54µs–64µs) | 83µs (82µs–93µs) | 138µs (127µs–142µs) | 69.3µs | 48.2µs | 0 | 0 | 0 |
| memory publish/small closed c=64 20s | grpc | 3 | 156.8k | 379µs (376µs–386µs) | 692µs (688µs–711µs) | 923µs (922µs–960µs) | 22.8µs | 20.7µs | 0 | 0 | 0 |
| memory publish/small closed c=64 20s faults | grpc | 1 | 43.4k | 84µs | 759µs | 41.89ms | 39.4µs | 37.0µs | 100 | 45652 | 0 |
| memory publish/small closed c=64 20s | rest | 3 | 89.8k | 685µs (685µs–687µs) | 998µs (977µs–1.07ms) | 1.51ms (1.50ms–1.62ms) | 48.7µs | 49.4µs | 0 | 0 | 0 |
| memory publish/small closed c=64 20s faults | rest | 1 | 44.4k | 67µs | 544µs | 41.50ms | 44.6µs | 43.9µs | 123 | 46469 | 0 |
| memory publish/small closed c=64 20s | h2c | 3 | 39.4k | 1.61ms (1.60ms–1.62ms) | 1.83ms (1.82ms–1.91ms) | 2.24ms (2.21ms–2.47ms) | 77.0µs | 51.1µs | 0 | 0 | 0 |
| memory publish/small closed c=64 20s faults | h2c | 1 | 35.7k | 375µs | 1.70ms | 42.40ms | 82.9µs | 54.6µs | 83 | 37542 | 0 |
| memory publish/small closed c=64 20s | h2c4 | 3 | 87.9k | 699µs (683µs–703µs) | 1.22ms (1.20ms–1.30ms) | 1.81ms (1.74ms–1.97ms) | 55.5µs | 49.4µs | 0 | 0 | 0 |
| memory publish/small closed c=256 20s | grpc | 3 | 202.5k | 1.12ms (1.12ms–1.14ms) | 2.08ms (2.05ms–2.17ms) | 2.87ms (2.81ms–3.00ms) | 21.6µs | 18.5µs | 0 | 0 | 0 |
| memory publish/small closed c=256 20s | rest | 3 | 90.3k | 2.79ms (2.79ms–2.79ms) | 3.28ms (3.26ms–3.58ms) | 4.66ms (4.59ms–6.25ms) | 49.1µs | 49.7µs | 100 | 0 | 0 |
| memory publish/small closed c=256 20s | h2c | 3 | 64.2k | 6.50ms (6.48ms–6.52ms) | 7.97ms (7.91ms–8.00ms) | 8.86ms (8.85ms–9.10ms) | 65.0µs | 44.7µs | 0 | 0 | 0 |
| memory publish/small closed c=256 20s | h2c4 | 3 | 118.7k | 1.96ms (1.95ms–1.99ms) | 4.13ms (4.10ms–4.13ms) | 6.05ms (5.93ms–6.16ms) | 42.8µs | 38.8µs | 0 | 0 | 0 |
| memory status/small closed c=64 20s | grpc | 1 | 151.9k | 392µs | 694µs | 837µs | 23.6µs | 19.6µs | 0 | 0 | 0 |
| memory status/small closed c=64 20s | rest | 1 | 90.3k | 679µs | 1.13ms | 1.59ms | 45.8µs | 47.7µs | 0 | 0 | 0 |
| memory status/small closed c=64 20s | h2c | 1 | 59.2k | 1.07ms | 1.44ms | 1.64ms | 51.3µs | 41.2µs | 0 | 0 | 0 |
| memory status/small closed c=256 20s | grpc | 1 | 177.0k | 1.44ms | 1.80ms | 2.08ms | 22.9µs | 18.3µs | 0 | 0 | 0 |
| memory status/small closed c=256 20s | rest | 1 | 93.3k | 2.71ms | 3.11ms | 4.15ms | 43.0µs | 44.6µs | 0 | 0 | 0 |
| memory status/small closed c=256 20s | h2c | 1 | 79.1k | 4.62ms | 5.44ms | 5.90ms | 48.1µs | 36.4µs | 0 | 0 | 0 |
| memory publish/small open 3000/s 20s | grpc | 3 | 3000 | 75µs (75µs–76µs) | 102µs (101µs–102µs) | 156µs (154µs–162µs) | 83.7µs | 551.9µs | 0 | 0 | 0 |
| memory publish/small open 3000/s 20s | rest | 3 | 3000 | 73µs (73µs–76µs) | 113µs (110µs–118µs) | 164µs (162µs–200µs) | 52.7µs | 526.8µs | 0 | 0 | 0 |
| memory publish/small open 3000/s 20s | h2c | 3 | 3000 | 98µs (98µs–100µs) | 131µs (130µs–136µs) | 178µs (175µs–221µs) | 87.8µs | 541.1µs | 0 | 0 | 0 |
| postgres publish/small closed c=1 20s | grpc | 3 | 2731 | 355µs (334µs–367µs) | 457µs (449µs–458µs) | 527µs (514µs–533µs) | 132.7µs | 96.7µs | 0 | 0 | 0 |
| postgres publish/small closed c=1 20s | rest | 3 | 2762 | 360µs (348µs–402µs) | 437µs (434µs–479µs) | 529µs (522µs–592µs) | 122.7µs | 74.8µs | 0 | 0 | 0 |
| postgres publish/small closed c=1 20s | h2c | 3 | 2354 | 417µs (346µs–425µs) | 506µs (470µs–577µs) | 655µs (550µs–821µs) | 199.2µs | 85.2µs | 0 | 0 | 0 |
| postgres publish/small closed c=64 20s | grpc | 3 | 18.0k | 3.51ms (3.51ms–3.51ms) | 4.16ms (4.15ms–4.19ms) | 4.83ms (4.77ms–5.72ms) | 94.7µs | 50.9µs | 0 | 0 | 0 |
| postgres publish/small closed c=64 20s | rest | 3 | 17.5k | 3.61ms (3.59ms–3.62ms) | 4.27ms (4.27ms–4.29ms) | 5.08ms (4.89ms–5.09ms) | 102.6µs | 63.9µs | 0 | 0 | 0 |
| postgres publish/small closed c=64 20s | h2c | 3 | 16.9k | 3.75ms (3.75ms–3.79ms) | 4.46ms (4.43ms–4.56ms) | 5.10ms (5.01ms–5.43ms) | 133.1µs | 65.6µs | 0 | 0 | 0 |
| postgres publish/small open 3000/s 20s | grpc | 3 | 3000 | 381µs (371µs–387µs) | 442µs (439µs–490µs) | 774µs (705µs–916µs) | 165.5µs | 580.0µs | 0 | 0 | 0 |
| postgres publish/small open 3000/s 20s | rest | 3 | 3000 | 344µs (343µs–347µs) | 415µs (414µs–418µs) | 631µs (595µs–639µs) | 101.0µs | 552.4µs | 0 | 0 | 0 |
| postgres publish/small open 3000/s 20s | h2c | 3 | 3000 | 380µs (369µs–382µs) | 458µs (452µs–465µs) | 633µs (624µs–656µs) | 156.5µs | 560.3µs | 0 | 0 | 0 |
