#!/usr/bin/env bash
#
# Requests per second with and without the cache, measured with ab
# (ApacheBench).
#
#   bench/bench.sh
#   REQUESTS=1000000 CONCURRENCY=256 RUNS=1 bench/bench.sh
#   GOMAXPROCS=1 bench/bench.sh
#
# Caddy is built from the working tree with the versions of go.mod, started
# with the Caddyfile next to this script on 127.0.0.1, and stopped at the end.
# Every scenario is a route of that Caddyfile: read it for what each one is.
# The first scenario of each group is the baseline the others are compared
# to. The cached ones are requested before they are measured, so what is
# measured is hits; the last column is the Cache-Status of such a request,
# which tells where from. Every scenario is measured RUNS times and the run
# with the median number of requests per second is the one reported: single
# runs of the same scenario differ by 10% or more.
#
# ab is a single thread, and a Caddy with many cores to itself answers small
# responses faster than ab can ask for them: the scenarios that reach the
# same ceiling show the speed of ab, not theirs. GOMAXPROCS, which Caddy
# inherits, makes Caddy the slower of the two, so that the differences
# between the scenarios show. Both run on the same machine and compete for
# it: compare the numbers of one run with each other, not with those of
# another machine.
#
# Environment:
#   REQUESTS       requests per run (100000)
#   RUNS           runs per scenario (3)
#   CONCURRENCY    concurrent connections (64)
#   KEEPALIVE      0 for one connection per request (1). macOS has about
#                  16000 ephemeral ports and takes a while to free them:
#                  lower REQUESTS accordingly.
#   PORT           port of the benchmarked site (9180)
#   UPSTREAM_PORT  port of the upstream of the reverse_proxy scenarios (9181)
#   CADDY          a caddy binary to benchmark instead of building one; it
#                  needs the cache module
#   GOMAXPROCS     number of threads Caddy runs Go code on (all cores)

set -euo pipefail

REQUESTS=${REQUESTS:-100000}
RUNS=${RUNS:-3}
CONCURRENCY=${CONCURRENCY:-64}
KEEPALIVE=${KEEPALIVE:-1}
PORT=${PORT:-9180}
UPSTREAM_PORT=${UPSTREAM_PORT:-9181}
CADDY=${CADDY:-}

here=$(cd "$(dirname "$0")" && pwd)
base="http://127.0.0.1:$PORT"

for tool in ab curl; do
	command -v "$tool" >/dev/null || { echo "bench: $tool not found" >&2; exit 1; }
done

work=$(mktemp -d)
caddy_pid=
cleanup() {
	if [ -n "$caddy_pid" ]; then
		kill "$caddy_pid" 2>/dev/null || true
		wait "$caddy_pid" 2>/dev/null || true
	fi
	rm -rf "$work"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

if [ -z "$CADDY" ]; then
	command -v go >/dev/null || { echo "bench: go not found, and CADDY is not set" >&2; exit 1; }
	echo "Building Caddy from the working tree..." >&2
	CADDY=$work/caddy
	(cd "$here" && CGO_ENABLED=0 go build -o "$CADDY" caddy.go)
fi

BENCH_PORT=$PORT BENCH_UPSTREAM_PORT=$UPSTREAM_PORT \
	BENCH_CACHE=$work/cache BENCH_ROOT=$here/../fixtures \
	"$CADDY" run --config "$here/Caddyfile" --adapter caddyfile >"$work/caddy.log" 2>&1 &
caddy_pid=$!

# Caddy listens before it is done starting its apps, but a request waits for
# the cache to be ready, so one answer is enough.
tries=0
until curl -sf -o /dev/null "$base/plain"; do
	tries=$((tries + 1))
	if ! kill -0 "$caddy_pid" 2>/dev/null || [ "$tries" -ge 100 ]; then
		echo "bench: Caddy did not start, is port $PORT or $UPSTREAM_PORT taken? Its log:" >&2
		cat "$work/caddy.log" >&2
		exit 1
	fi
	sleep 0.1
done

ab_flags=(-q -c "$CONCURRENCY")
if [ "$KEEPALIVE" != 0 ]; then
	ab_flags+=(-k)
fi

format="%-34s %10s %9s %8s %8s %8s %7s  %s\n"
failures=0
baseline=

# group <title> starts a group of scenarios, the first of which is the
# baseline of the others.
group() {
	baseline=
	printf '\n%s\n' "$1"
}

# field <label> <n> prints the n-th word after the label of a line of the
# report of ab, or nothing if there is no such line.
field() {
	awk -F: -v label="$1" -v n="$2" '$1 == label { split($2, w, " "); print w[n]; exit }' "$work/ab.txt"
}

# percentile <n> prints the time, in milliseconds, n percent of the requests
# were served within. The report has them rounded to the millisecond, which
# is of no use here; the CSV file does not.
percentile() {
	awk -F, -v n="$1" '$1 == n { printf "%.2f", $2; exit }' "$work/percentiles.csv"
}

# bench <name> <path> [options] measures one scenario. The options are given
# to both ab and curl: they are meant for -H.
bench() {
	local name=$1 url=$base$2
	shift 2

	# Warm up: the response is stored, written to disk if it waits for that
	# (min_uses), and requested enough to be copied to memory, which happens
	# in the background, hence the pause.
	if ! ab "${ab_flags[@]}" -n $((CONCURRENCY * 20)) "$@" "$url" >"$work/ab.txt" 2>&1; then
		echo "bench: ab failed on $url:" >&2
		cat "$work/ab.txt" >&2
		exit 1
	fi
	sleep 0.5

	local status
	status=$(curl -s -o /dev/null -D - "$@" "$url" | tr -d '\r' |
		awk 'tolower($1) == "cache-status:" { sub(/^[^ ]* /, ""); sub(/; key=.*/, ""); sub(/ttl=[0-9-]*; /, ""); print }')

	local run rps failed non2xx ratio
	: >"$work/runs.txt"
	for run in $(seq "$RUNS"); do
		if ! ab "${ab_flags[@]}" -n "$REQUESTS" -e "$work/percentiles.$run.csv" "$@" "$url" >"$work/ab.txt" 2>&1; then
			echo "bench: ab failed on $url:" >&2
			cat "$work/ab.txt" >&2
			exit 1
		fi
		failed=$(field "Failed requests" 1)
		non2xx=$(field "Non-2xx responses" 1)
		failures=$((failures + ${failed:-0} + ${non2xx:-0}))
		echo "$(field "Requests per second" 1) $run $((${failed:-0} + ${non2xx:-0}))" >>"$work/runs.txt"
		mv "$work/ab.txt" "$work/ab.$run.txt"
	done

	# The median run, the lower of the two in the middle if RUNS is even.
	read -r rps run failed < <(sort -n "$work/runs.txt" | sed -n "$(((RUNS + 1) / 2))p")
	cp "$work/ab.$run.txt" "$work/ab.txt"
	cp "$work/percentiles.$run.csv" "$work/percentiles.csv"

	if [ -z "$baseline" ]; then
		baseline=$rps
		ratio=baseline
	else
		ratio=$(awk -v a="$rps" -v b="$baseline" 'BEGIN { printf "%.2fx", a / b }')
	fi

	# shellcheck disable=SC2059
	printf "$format" "$name" "$(printf '%.0f' "$rps")" "$ratio" \
		"$(percentile 50)" "$(percentile 99)" \
		"$(field "Transfer rate" 1 | awk '{ printf "%.1f", $1 / 1024 }')" \
		"$failed" "${status:--}"
}

echo "$("$CADDY" version | cut -d' ' -f1), $(ab -V | sed -n 's/^This is \(ApacheBench, Version [0-9.]*\).*/\1/p')"
echo "median of $RUNS runs of $REQUESTS requests, $CONCURRENCY connections, keep-alive $([ "$KEEPALIVE" != 0 ] && echo on || echo off), GOMAXPROCS ${GOMAXPROCS:-unset}"
echo
# shellcheck disable=SC2059
printf "$format" "" "req/s" "" "p50 ms" "p99 ms" "MiB/s" "failed" "Cache-Status"

group "Text, 13 bytes (respond)"
bench "no cache" /plain
bench "cache" /cache/plain
bench "cache, max_memory off" /disk/plain
bench "cache, min_uses 2" /min-uses/plain
bench "cache, Vary" /vary/plain -H "X-Bench: a"
bench "cache, key template" /template/plain
bench "cache, no-store response" /no-store/plain

group "fixtures/test.png, 42KiB (file_server)"
bench "no cache" /file/test.png
bench "cache" /cache/file/test.png
bench "cache, max_memory off" /disk/file/test.png
bench "cache, min_uses 2" /min-uses/file/test.png
bench "cache, slice 16Ki" /slice/file/test.png

group "fixtures/test.png, 42KiB (reverse_proxy to file_server)"
bench "no cache" /proxy/test.png
bench "cache" /cache/proxy/test.png
bench "cache, max_memory off" /disk/proxy/test.png

if [ "$failures" -gt 0 ]; then
	echo >&2
	echo "bench: $failures requests failed or were not answered with a 2xx status" >&2
	exit 1
fi
