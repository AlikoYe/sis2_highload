#!/usr/bin/env bash
# Runs the protocol benchmark matrix. Usage: scripts/bench.sh quick|sample|full
# Env: SERVER_PROCS / LOADGEN_PROCS (GOMAXPROCS, default half the cores each),
#      DATABASE_URL, MEMORY_MAX_EVENTS (default 1000000).
set -euo pipefail

PRESET=${1:-quick}
cd "$(dirname "$0")/.."

CORES=$(sysctl -n hw.ncpu 2>/dev/null || nproc)
SERVER_PROCS=${SERVER_PROCS:-$((CORES / 2))}
LOADGEN_PROCS=${LOADGEN_PROCS:-$((CORES / 2))}
DATABASE_URL=${DATABASE_URL:-postgres://notif:notif@localhost:5433/notif?sslmode=disable}
MEMORY_MAX_EVENTS=${MEMORY_MAX_EVENTS:-1000000}
GRPC_PORT=50051
HTTP_PORT=8080
H2C_PORT=8081
PROTOCOLS="grpc rest h2c"

case $PRESET in
quick) DURATION=5s WARMUP=1s REPS=1 ;;
sample) DURATION=30s WARMUP=5s REPS=1 ;;
full) DURATION=30s WARMUP=5s REPS=3 ;;
*)
	echo "usage: $0 quick|sample|full" >&2
	exit 2
	;;
esac

TS=$(date +%Y%m%d-%H%M%S)
OUT=results/bench_${PRESET}_${TS}.csv
CPU_OUT=results/bench_${PRESET}_${TS}_server_cpu.csv
LOG_DIR=results/logs/${PRESET}_${TS}
mkdir -p "$LOG_DIR"

echo "building binaries"
go build -o bin/server ./cmd/server
go build -o bin/loadgen ./cmd/loadgen

pg_version() {
	docker compose exec -T postgres postgres --version 2>/dev/null || echo "not running"
}

write_env() {
	{
		echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
		echo "preset: $PRESET (duration $DURATION, warmup $WARMUP, reps $REPS)"
		echo "go: $(go version)"
		echo "os: $(sw_vers -productName 2>/dev/null) $(sw_vers -productVersion 2>/dev/null) $(uname -m)"
		echo "cpu: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || grep -m1 'model name' /proc/cpuinfo)"
		echo "cores: $CORES logical ($(sysctl -n hw.perflevel0.physicalcpu 2>/dev/null || echo '?') performance + $(sysctl -n hw.perflevel1.physicalcpu 2>/dev/null || echo '?') efficiency)"
		echo "memory: $(($(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1024 / 1024 / 1024)) GiB"
		echo "docker: $(docker --version 2>/dev/null || echo 'n/a')"
		echo "postgres: $(pg_version)"
		echo "GOMAXPROCS: server=$SERVER_PROCS loadgen=$LOADGEN_PROCS"
		echo "memory store cap: $MEMORY_MAX_EVENTS events (fresh server per run)"
	} >results/env.txt
}

SERVER_PID=""

cleanup() {
	if [ -n "$SERVER_PID" ]; then
		kill -INT "$SERVER_PID" 2>/dev/null || true
		wait "$SERVER_PID" 2>/dev/null || true
	fi
}
trap cleanup EXIT

# start_server STORE [extra server flags...]
start_server() {
	local store=$1
	shift
	local args="-store $store -grpc-addr :$GRPC_PORT -http-addr :$HTTP_PORT -h2c-addr :$H2C_PORT -log-level warn"
	if [ "$store" = postgres ]; then
		args="$args -db-url $DATABASE_URL"
	else
		args="$args -memory-max-events $MEMORY_MAX_EVENTS"
	fi
	# shellcheck disable=SC2086
	GOMAXPROCS=$SERVER_PROCS bin/server $args "$@" >>"$LOG_DIR/server.log" 2>&1 &
	SERVER_PID=$!
	for _ in $(seq 1 100); do
		if curl -sf "localhost:$HTTP_PORT/readyz" >/dev/null; then
			return 0
		fi
		if ! kill -0 "$SERVER_PID" 2>/dev/null; then
			echo "server exited during startup, see $LOG_DIR/server.log" >&2
			exit 1
		fi
		sleep 0.1
	done
	echo "server not ready after 10s" >&2
	exit 1
}

stop_server() {
	kill -INT "$SERVER_PID"
	wait "$SERVER_PID" || true
	SERVER_PID=""
}

ensure_postgres() {
	docker compose up -d --wait postgres >/dev/null
	for f in migrations/*.sql; do
		docker compose exec -T postgres psql -q -v ON_ERROR_STOP=1 -U notif -d notif <"$f" >/dev/null 2>&1
	done
}

port_for() {
	case $1 in
	grpc) echo $GRPC_PORT ;;
	rest) echo $HTTP_PORT ;;
	h2c) echo $H2C_PORT ;;
	esac
}

# run LABEL STORE PROTOCOL OP PAYLOAD MODE LOAD [extra server flags...]
# Each run gets a fresh server (and an empty table for Postgres) so runs do not
# inherit heap, caches or table bloat from earlier ones.
run() {
	local label=$1 store=$2 protocol=$3 op=$4 payload=$5 mode=$6 load=$7
	shift 7
	if [ "$store" = postgres ]; then
		docker compose exec -T postgres psql -q -U notif -d notif -c 'TRUNCATE deliveries' >/dev/null
	fi
	start_server "$store" "$@"
	local loadflag="-concurrency $load"
	if [ "$mode" = open ]; then
		loadflag="-rate $load"
	fi
	# shellcheck disable=SC2086
	GOMAXPROCS=$LOADGEN_PROCS bin/loadgen -protocol "$protocol" -target "localhost:$(port_for "$protocol")" \
		-op "$op" -payload "$payload" -mode "$mode" $loadflag -duration "$DURATION" -warmup "$WARMUP" \
		-label "$label" -store "$store" -out "$OUT" -server-pid "$SERVER_PID" -server-cpu-out "$CPU_OUT" |
		tee -a "$LOG_DIR/loadgen.log"
	stop_server
}

matrix_quick() {
	local r=1 p c
	for p in $PROTOCOLS; do
		for c in 1 64; do
			run "mem-$p-publish-small-c$c-r$r" memory "$p" publish small closed "$c"
		done
		run "mem-$p-open1000-r$r" memory "$p" publish small open 1000
	done
}

matrix_sample() {
	local p
	for p in $PROTOCOLS; do
		run "sample-mem-$p-publish-small-c64" memory "$p" publish small closed 64
	done
}

matrix_full() {
	local r p c spec op payload
	for r in $(seq 1 "$REPS"); do
		for p in $PROTOCOLS; do
			for spec in publish:small publish:large batch:batch; do
				op=${spec%%:*}
				payload=${spec##*:}
				for c in 1 16 64 256; do
					run "mem-$p-$op-$payload-c$c-r$r" memory "$p" "$op" "$payload" closed "$c"
				done
			done
			for c in 16 256; do
				run "mem-$p-status-c$c-r$r" memory "$p" status small closed "$c"
			done
			run "mem-$p-open3000-r$r" memory "$p" publish small open 3000
			run "mem-$p-faults5pct-c64-r$r" memory "$p" publish small closed 64 -fault-fail-after=0.05
		done
		for p in $PROTOCOLS; do
			for c in 1 64; do
				run "pg-$p-publish-small-c$c-r$r" postgres "$p" publish small closed "$c"
			done
			run "pg-$p-open3000-r$r" postgres "$p" publish small open 3000
		done
	done
}

if [ "$PRESET" = full ]; then
	ensure_postgres
fi
write_env
echo "preset=$PRESET -> $OUT"
"matrix_$PRESET"
echo "done: $OUT (server CPU: $CPU_OUT, logs: $LOG_DIR)"
