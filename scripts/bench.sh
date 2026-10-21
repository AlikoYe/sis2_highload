#!/usr/bin/env bash
# Runs the protocol benchmark matrix.
# Usage: scripts/bench.sh quick|sample|investigate|full [--estimate]
# Env: SERVER_PROCS / LOADGEN_PROCS (GOMAXPROCS, default half the cores each),
#      DATABASE_URL, MEMORY_MAX_EVENTS (default 1000000),
#      FULL_H2C_EXTRA (extra h2c variant in the full matrix, default h2c4; empty disables).
set -euo pipefail

PRESET=${1:-quick}
ESTIMATE_ONLY=0
if [ "${2:-}" = --estimate ]; then
	ESTIMATE_ONLY=1
fi
cd "$(dirname "$0")/.."

# Keep the machine awake for the long run (display, idle, disk, system, user).
if [ "$PRESET" = full ] && [ $ESTIMATE_ONLY = 0 ] && [ -z "${BENCH_CAFFEINATED:-}" ] && command -v caffeinate >/dev/null; then
	export BENCH_CAFFEINATED=1
	exec caffeinate -dimsu "$0" "$@"
fi

CORES=$(sysctl -n hw.ncpu 2>/dev/null || nproc)
SERVER_PROCS=${SERVER_PROCS:-$((CORES / 2))}
LOADGEN_PROCS=${LOADGEN_PROCS:-$((CORES / 2))}
DATABASE_URL=${DATABASE_URL:-postgres://notif:notif@localhost:5433/notif?sslmode=disable}
MEMORY_MAX_EVENTS=${MEMORY_MAX_EVENTS:-1000000}
# h2c over 4 connections beat 1 and 8 at c=64 and c=256; see results/h2c_investigation.md.
FULL_H2C_EXTRA=${FULL_H2C_EXTRA-h2c4}
GRPC_PORT=50051
HTTP_PORT=8080
H2C_PORT=8081
PPROF_PORT=6060
PROFILE_S=20
RUN_OVERHEAD_S=3 # server start/stop, client setup, seeding (measured ~2-3s)
PROTOCOLS="grpc rest h2c"

case $PRESET in
quick) DURATION_S=5 WARMUP_S=1 REPS=1 ;;
sample | investigate) DURATION_S=30 WARMUP_S=5 REPS=1 ;;
full) DURATION_S=30 WARMUP_S=5 REPS=3 ;;
*)
	echo "usage: $0 quick|sample|investigate|full [--estimate]" >&2
	exit 2
	;;
esac

TS=$(date +%Y%m%d-%H%M%S)
OUT=results/bench_${PRESET}_${TS}.csv
CPU_OUT=results/bench_${PRESET}_${TS}_server_cpu.csv
LOG_DIR=results/logs/${PRESET}_${TS}
COUNT_ONLY=0
RUN_COUNT=0
SERVER_PID=""

pg_version() {
	docker compose exec -T postgres postgres --version 2>/dev/null || echo "not running"
}

write_env() {
	{
		echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
		echo "preset: $PRESET (duration ${DURATION_S}s, warmup ${WARMUP_S}s, reps $REPS)"
		echo "go: $(go version)"
		echo "os: $(sw_vers -productName 2>/dev/null) $(sw_vers -productVersion 2>/dev/null) $(uname -m)"
		echo "cpu: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || grep -m1 'model name' /proc/cpuinfo)"
		echo "cores: $CORES logical ($(sysctl -n hw.perflevel0.physicalcpu 2>/dev/null || echo '?') performance + $(sysctl -n hw.perflevel1.physicalcpu 2>/dev/null || echo '?') efficiency)"
		echo "memory: $(($(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1024 / 1024 / 1024)) GiB"
		echo "docker: $(docker --version 2>/dev/null || echo 'n/a')"
		echo "postgres: $(pg_version)"
		echo "GOMAXPROCS: server=$SERVER_PROCS loadgen=$LOADGEN_PROCS"
		echo "memory store cap: $MEMORY_MAX_EVENTS events (fresh server per run)"
		echo "protocol order: interleaved per configuration, rotated each repetition"
	} >results/env.txt
}

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

# run LABEL STORE VARIANT OP PAYLOAD MODE LOAD [extra server flags...]
# VARIANT is a protocol with an optional connection count, e.g. grpc, h2c8.
# Each run gets a fresh server (and an empty table for Postgres) so runs do not
# inherit heap, caches or table bloat. PROFILE=1 captures a server CPU profile
# for PROFILE_S seconds starting after the warm-up.
run() {
	local label=$1 store=$2 variant=$3 op=$4 payload=$5 mode=$6 load=$7
	shift 7
	if [ $COUNT_ONLY = 1 ]; then
		RUN_COUNT=$((RUN_COUNT + 1))
		return
	fi
	local protocol conns
	case $variant in
	grpc*) protocol=grpc ;;
	rest*) protocol=rest ;;
	h2c*) protocol=h2c ;;
	*)
		echo "unknown variant $variant" >&2
		exit 1
		;;
	esac
	conns=${variant#"$protocol"}
	conns=${conns:-1}
	if [ "$store" = postgres ]; then
		docker compose exec -T postgres psql -q -U notif -d notif -c 'TRUNCATE deliveries' >/dev/null
	fi
	local profile=${PROFILE:-0}
	if [ "$profile" = 1 ]; then
		start_server "$store" -pprof-addr "localhost:$PPROF_PORT" "$@"
	else
		start_server "$store" "$@"
	fi
	local prof_pid=""
	if [ "$profile" = 1 ]; then
		mkdir -p results/profiles
		(sleep "$WARMUP_S" && curl -sf "http://localhost:$PPROF_PORT/debug/pprof/profile?seconds=$PROFILE_S" \
			-o "results/profiles/$label.pprof") &
		prof_pid=$!
	fi
	local loadflag="-concurrency $load"
	if [ "$mode" = open ]; then
		loadflag="-rate $load"
	fi
	# shellcheck disable=SC2086
	GOMAXPROCS=$LOADGEN_PROCS bin/loadgen -protocol "$protocol" -conns "$conns" -target "localhost:$(port_for "$protocol")" \
		-op "$op" -payload "$payload" -mode "$mode" $loadflag -duration "${DURATION_S}s" -warmup "${WARMUP_S}s" \
		-label "$label" -store "$store" -out "$OUT" -server-pid "$SERVER_PID" -server-cpu-out "$CPU_OUT" |
		tee -a "$LOG_DIR/loadgen.log"
	if [ -n "$prof_pid" ]; then
		wait "$prof_pid"
		go tool pprof -top -nodecount=30 bin/server "results/profiles/$label.pprof" >"results/profiles/$label.top.txt" 2>/dev/null
	fi
	stop_server
}

# rotate "a b c" K -> list rotated left by K positions.
rotate() {
	local -a items
	read -r -a items <<<"$1"
	local n=${#items[@]} k=$(($2 % ${#items[@]})) i out=""
	for ((i = 0; i < n; i++)); do
		out="$out ${items[$(((i + k) % n))]}"
	done
	echo "$out"
}

# for_protocols REP "VARIANTS" SCENARIO STORE OP PAYLOAD MODE LOAD [server flags...]
# Runs the variants back to back for one configuration, order rotated per repetition.
for_protocols() {
	local r=$1 variants=$2 scenario=$3 store=$4 op=$5 payload=$6 mode=$7 load=$8
	shift 8
	local tag="c$load" v
	if [ "$mode" = open ]; then
		tag="open$load"
	fi
	for v in $(rotate "$variants" $((r - 1))); do
		run "$scenario-$v-$op-$payload-$tag-r$r" "$store" "$v" "$op" "$payload" "$mode" "$load" "$@"
	done
}

matrix_quick() {
	local c
	for c in 1 64; do
		for_protocols 1 "$PROTOCOLS" mem memory publish small closed "$c"
	done
	for_protocols 1 "$PROTOCOLS" mem memory publish small open 1000
}

matrix_sample() {
	for_protocols 1 "$PROTOCOLS" sample memory publish small closed 64
}

matrix_investigate() {
	for_protocols 1 "h2c h2c4 h2c8 rest grpc grpc4" inv memory publish small closed 64
	for_protocols 1 "h2c h2c8 rest" inv memory publish small closed 256
	PROFILE=1 for_protocols 1 "$PROTOCOLS" prof memory publish small closed 64
}

matrix_full() {
	local r c spec op payload variants
	for r in $(seq 1 "$REPS"); do
		for spec in publish:small publish:large batch:batch; do
			op=${spec%%:*}
			payload=${spec##*:}
			for c in 1 16 64 256; do
				variants=$PROTOCOLS
				if [ -n "$FULL_H2C_EXTRA" ] && [ "$spec" != publish:large ] && { [ "$c" = 64 ] || [ "$c" = 256 ]; }; then
					variants="$PROTOCOLS $FULL_H2C_EXTRA"
				fi
				for_protocols "$r" "$variants" mem memory "$op" "$payload" closed "$c"
			done
		done
		for c in 16 256; do
			for_protocols "$r" "$PROTOCOLS" mem memory status small closed "$c"
		done
		for_protocols "$r" "$PROTOCOLS" mem memory publish small open 3000
		for_protocols "$r" "$PROTOCOLS" rel memory publish small closed 64 -fault-fail-after=0.05
		for c in 1 64; do
			for_protocols "$r" "$PROTOCOLS" pg postgres publish small closed "$c"
		done
		for_protocols "$r" "$PROTOCOLS" pg postgres publish small open 3000
	done
}

COUNT_ONLY=1
"matrix_$PRESET"
COUNT_ONLY=0
TOTAL_S=$((RUN_COUNT * (DURATION_S + WARMUP_S + RUN_OVERHEAD_S)))
printf 'preset=%s: %d runs x (%ds + %ds warm-up + ~%ds overhead) = ~%dh%02dm\n' \
	"$PRESET" "$RUN_COUNT" "$DURATION_S" "$WARMUP_S" "$RUN_OVERHEAD_S" $((TOTAL_S / 3600)) $((TOTAL_S % 3600 / 60))
if [ $ESTIMATE_ONLY = 1 ]; then
	exit 0
fi

mkdir -p "$LOG_DIR"
echo "building binaries"
go build -o bin/server ./cmd/server
go build -o bin/loadgen ./cmd/loadgen
if [ "$PRESET" = full ]; then
	ensure_postgres
fi
write_env
echo "writing $OUT"
"matrix_$PRESET"
echo "done: $OUT (server CPU: $CPU_OUT, logs: $LOG_DIR)"
