#!/usr/bin/env bash
#
# Requests per second of this module next to those of other caches: nginx's
# proxy_cache, Varnish, and caddyserver/cache-handler (Souin) with three of
# its storages. Everything runs in Docker, which is all the host needs, and
# on the loopback interface of one container.
#
#   bench/compare.sh
#   bench/compare.sh | tee results.txt
#   CORES="1 2 8" REQUESTS=200000 bench/compare.sh
#
# What it does:
#
#   1. Builds an image with a Caddy built from the working tree, a Caddy
#      with caddyserver/cache-handler, and Alpine's nginx, Varnish, hitch
#      and ab.
#   2. For each number of cores in CORES, puts every cache in front of the
#      same upstream, a Caddy without a cache answering a text of 13 bytes
#      and fixtures/test.png, and measures hits with ab. The no cache rows
#      are what the same server does without its cache: by itself, then as
#      a mere proxy of the upstream. Everything is measured twice: in plain
#      HTTP, and over TLS with sendfile off, as a server that terminates TLS
#      does without help from the kernel. Varnish has no TLS of its own and
#      gets it from hitch, the TLS proxy of the Varnish project, on the same
#      cores.
#   3. Runs bench.sh, the benchmark of this module alone, in the same image:
#      once as it is, once with GOMAXPROCS=1.
#
# The processes are pinned, each to physical cores of its own: ab to one,
# the server under test to the next CORES ones, the upstream to up to four
# of the last ones. ab is a single thread: with enough cores, the fastest
# servers answer faster than it asks, and their rows show the speed of ab.
# One core is where the servers are compared; more cores tell how they scale.
#
# Environment:
#   CORES        numbers of cores to give the server under test, each a run
#                of its own ("1 4")
#   REQUESTS     requests per run (100000)
#   RUNS         runs per scenario, the median one is reported (3)
#   CONCURRENCY  concurrent keep-alive connections (64)
#   TIMELIMIT    seconds after which a run of compare.sh is cut short, for
#                the servers that take minutes to answer REQUESTS (10)
#   IMAGE        name given to the image (cache-bench)

set -euo pipefail

if [ "${1:-}" != inside ]; then
	#
	# On the host: build the image, and run this script and bench.sh in it.
	#
	CORES=${CORES:-1 4}
	IMAGE=${IMAGE:-cache-bench}
	repo=$(cd "$(dirname "$0")/.." && pwd)

	command -v docker >/dev/null || { echo "compare: docker not found" >&2; exit 1; }

	echo "Building the image $IMAGE..." >&2
	# The output of the build is only of interest when it fails.
	buildlog=$(mktemp)
	trap 'rm -f "$buildlog"' EXIT
	docker build --progress=plain -t "$IMAGE" -f - "$repo" >"$buildlog" 2>&1 <<'EOF' || { cat "$buildlog" >&2; exit 1; }
FROM golang:1.27-alpine3.24 AS ours
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -o /caddy-ours bench/caddy.go && \
    go list -m -f '{{.Version}}' github.com/caddyserver/caddy/v2 >/caddy-version

# The original module, on the same version of Caddy. Its versions are pinned
# so that two runs of the benchmark compare the same things.
FROM golang:1.27-alpine3.24 AS souin
WORKDIR /souin
COPY --from=ours /caddy-version /caddy-version
COPY <<GO main.go
package main

import (
	caddycmd "github.com/caddyserver/caddy/v2/cmd"

	_ "github.com/caddyserver/cache-handler"
	_ "github.com/caddyserver/caddy/v2/modules/standard"
	_ "github.com/darkweak/storages/otter/caddy"
	_ "github.com/darkweak/storages/simplefs/caddy"
)

func main() {
	caddycmd.Main()
}
GO
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go mod init souinbench && \
    go get github.com/caddyserver/caddy/v2@$(cat /caddy-version) \
        github.com/caddyserver/cache-handler@v0.17.0 \
        github.com/darkweak/storages/otter/caddy@v0.0.20 \
        github.com/darkweak/storages/simplefs/caddy@v0.0.20 && \
    go mod tidy && CGO_ENABLED=0 go build -o /caddy-souin . && \
    grep -E "caddyserver/cache-handler |darkweak/souin " go.mod >/souin-versions.txt

# bash is what these scripts are written for, and util-linux-misc has the
# taskset whose output they read.
FROM alpine:3.24
RUN apk add --no-cache nginx varnish hitch openssl apache2-utils curl bash util-linux-misc
COPY --from=ours /caddy-ours /usr/local/bin/caddy-ours
COPY --from=souin /caddy-souin /usr/local/bin/caddy-souin
COPY --from=souin /souin-versions.txt /souin-versions.txt
# On the filesystem of the container rather than mounted from the host: with
# Docker in a virtual machine, a mount is slow enough to open files on to be
# what the servers that serve them are measured by.
COPY fixtures /fixtures
COPY bench /bench
EOF

	# --init, for the script not to be the process 1 of the container, which
	# ignores the signal an interruption sends.
	run=(docker run --rm --init -e REQUESTS -e RUNS -e CONCURRENCY -e TIMELIMIT -e KEEPALIVE)

	for cores in $CORES; do
		echo >&2
		echo "Comparison, server on $cores core(s)..." >&2
		"${run[@]}" -e CORES="$cores" "$IMAGE" /bench/compare.sh inside
		echo
	done

	echo >&2
	echo "bench.sh..." >&2
	echo "bench.sh, in the same image"
	echo
	"${run[@]}" -e CADDY=/usr/local/bin/caddy-ours "$IMAGE" /bench/bench.sh
	echo
	"${run[@]}" -e CADDY=/usr/local/bin/caddy-ours -e GOMAXPROCS=1 "$IMAGE" /bench/bench.sh
	exit
fi

#
# In the container: one comparison, with the server under test on CORES cores.
#
CORES=${CORES:-1}
REQUESTS=${REQUESTS:-100000}
RUNS=${RUNS:-3}
CONCURRENCY=${CONCURRENCY:-64}
TIMELIMIT=${TIMELIMIT:-10}

schemes="http https"
conf=/tmp/conf
work=$(mktemp -d)
mkdir -p $conf

# base <scheme> is where the server under test listens for that scheme.
base() {
	case $1 in
	http) echo http://127.0.0.1:9180 ;;
	https) echo https://127.0.0.1:9443 ;;
	esac
}

# The certificate every server presents: a self-signed one, which ab and
# curl are told not to check.
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 2 \
	-subj /CN=127.0.0.1 -addext subjectAltName=IP:127.0.0.1 \
	-keyout $conf/key.pem -out $conf/cert.pem 2>/dev/null
# hitch wants the key and the certificate in one file.
cat $conf/key.pem $conf/cert.pem >$conf/hitch.pem

# One CPU per physical core among those the container may use, so that no
# two of the pinned processes are the two threads of one core.
cpus=()
seen=
for n in $(taskset -cp $$ | sed 's/.*: //' | tr , '\n' | while IFS=- read -r first last; do seq "$first" "${last:-$first}"; done); do
	siblings=$(cat "/sys/devices/system/cpu/cpu$n/topology/thread_siblings_list" 2>/dev/null || echo "$n")
	case " $seen " in *" $siblings "*) continue ;; esac
	seen="$seen $siblings"
	cpus+=("$n")
done
if [ "${#cpus[@]}" -lt $((CORES + 2)) ]; then
	echo "compare: $CORES core(s) for the server, one for ab and one for the upstream take $((CORES + 2)) cores, there are ${#cpus[@]}" >&2
	exit 1
fi
upstream_count=$((${#cpus[@]} - 1 - CORES))
[ "$upstream_count" -gt 4 ] && upstream_count=4
ab_cpus=${cpus[0]}
server_cpus=$(IFS=,; echo "${cpus[*]:1:CORES}")
upstream_cpus=$(IFS=,; echo "${cpus[*]:${#cpus[@]}-upstream_count}")

# The upstream every cache is put in front of. Any Host will do: nginx and
# Varnish do not send it the same one.
cat >$conf/upstream.Caddyfile <<'EOF'
{
	admin off
	persist_config off
	default_bind 127.0.0.1
}

http://:9181 {
	route /plain {
		respond "Hello, world!"
	}
	root * /fixtures
	file_server
}
EOF

# Caddy with this module. The same routes are served in plain HTTP and over
# TLS; the scheme is part of the cache key, so the two do not share entries.
cat >$conf/ours.Caddyfile <<'EOF'
{
	admin off
	persist_config off
	default_bind 127.0.0.1
	auto_https disable_redirects
	cache {
		path /tmp/ours/default
		ttl 1h
	}
}

(routes) {
	route /direct/plain {
		respond "Hello, world!"
	}
	route /direct/* {
		uri strip_prefix /direct
		root * /fixtures
		file_server
	}
	route /proxy/* {
		uri strip_prefix /proxy
		reverse_proxy 127.0.0.1:9181
	}
	route /cache/* {
		cache
		uri strip_prefix /cache
		reverse_proxy 127.0.0.1:9181
	}
	route /disk/* {
		cache {
			path /tmp/ours/disk
			max_memory off
		}
		uri strip_prefix /disk
		reverse_proxy 127.0.0.1:9181
	}
}

http://127.0.0.1:9180 {
	import routes
}

https://127.0.0.1:9443 {
	tls /tmp/conf/cert.pem /tmp/conf/key.pem
	import routes
}
EOF

# Caddy with caddyserver/cache-handler. Its storage is a global option: there
# is one run per storage, each with its own imported file.
cat >$conf/souin.Caddyfile <<'EOF'
{
	admin off
	persist_config off
	default_bind 127.0.0.1
	auto_https disable_redirects
	cache {
		ttl 1h
		import /tmp/conf/souin-storage
	}
}

(routes) {
	route /cache/* {
		cache
		uri strip_prefix /cache
		reverse_proxy 127.0.0.1:9181
	}
}

http://127.0.0.1:9180 {
	import routes
}

https://127.0.0.1:9443 {
	tls /tmp/conf/cert.pem /tmp/conf/key.pem
	import routes
}
EOF

# worker_processes is given on the command line: one per core.
cat >$conf/nginx.conf <<'EOF'
pid /tmp/nginx.pid;
error_log /tmp/nginx-error.log warn;

events {
	worker_connections 4096;
}

http {
	include /etc/nginx/mime.types;
	access_log off;
	sendfile on;
	tcp_nopush on;
	# A connection of ab is not to be closed after the default 1000 requests.
	keepalive_requests 100000000;

	proxy_cache_path /tmp/nginx-cache levels=1:2 keys_zone=bench:10m max_size=1g inactive=1h use_temp_path=off;

	upstream origin {
		server 127.0.0.1:9181;
		keepalive 64;
	}

	server {
		listen 127.0.0.1:9180 reuseport;
		include /tmp/conf/nginx-locations.conf;
	}

	server {
		listen 127.0.0.1:9443 ssl reuseport;
		ssl_certificate /tmp/conf/cert.pem;
		ssl_certificate_key /tmp/conf/key.pem;
		# nginx cannot use sendfile on a TLS connection anyway, short of
		# kernel TLS, which this does not set up.
		sendfile off;
		include /tmp/conf/nginx-locations.conf;
	}
}
EOF

cat >$conf/nginx-locations.conf <<'EOF'
location = /direct/plain {
	default_type text/plain;
	return 200 "Hello, world!";
}
location /direct/ {
	alias /fixtures/;
}

location /proxy/ {
	proxy_pass http://origin/;
	proxy_http_version 1.1;
	proxy_set_header Connection "";
}

location /cache/ {
	proxy_pass http://origin/;
	proxy_http_version 1.1;
	proxy_set_header Connection "";
	proxy_cache bench;
	proxy_cache_valid 200 1h;
	add_header X-Cache-Status $upstream_cache_status;
}
EOF

cat >$conf/varnish.vcl <<'EOF'
vcl 4.1;

backend origin {
	.host = "127.0.0.1";
	.port = "9181";
}

sub vcl_recv {
	if (req.url ~ "^/pass/") {
		set req.url = regsub(req.url, "^/pass", "");
		return (pass);
	}
	set req.url = regsub(req.url, "^/cache", "");
}

sub vcl_deliver {
	if (obj.hits > 0) {
		set resp.http.X-Cache = "hit";
	} else {
		set resp.http.X-Cache = "miss";
	}
}
EOF

pids=()
servers=()
trap 'kill "${pids[@]}" 2>/dev/null || true' EXIT

# start <cpus> <command...> runs a process of the server under test in the
# background on those CPUs; stop ends all of them.
start() {
	local on=$1
	shift
	taskset -c "$on" "$@" >>"$work/server.log" 2>&1 &
	pids+=("$!")
	servers+=("$!")
}

# ready <path> waits for the server under test to answer it for every scheme.
ready() {
	local scheme tries
	for scheme in $schemes; do
		tries=0
		until curl -skf -o /dev/null "$(base "$scheme")$1"; do
			tries=$((tries + 1))
			if [ "$tries" -ge 300 ]; then
				echo "compare: no answer from $(base "$scheme")$1, the log of the server:" >&2
				cat "$work/server.log" >&2
				exit 1
			fi
			sleep 0.1
		done
	done
}

# stop ends the server under test. nginx, Varnish and hitch answer from
# processes of their own, which take a moment to follow: the next server
# needs the ports.
stop() {
	local scheme
	kill "${servers[@]}" 2>/dev/null || true
	wait "${servers[@]}" 2>/dev/null || true
	servers=()
	for scheme in $schemes; do
		while curl -sk -o /dev/null -m 1 "$(base "$scheme")/"; do sleep 0.2; done
	done
	: >"$work/server.log"
}

# field <label> <n> prints the n-th word after the label of a line of the
# report of ab.
field() {
	awk -F: -v label="$1" -v n="$2" '$1 == label { split($2, w, " "); print w[n]; exit }' "$work/ab.txt"
}

format="%-44s %9s %8s %8s %8s %7s  %s\n"
failures=0

# bench <name> <path> [<name over TLS>] measures <path>/plain and
# <path>/test.png for every scheme, and adds a row to the table of each.
bench() {
	local name=$1 path=$2 tlsname=${3:-$1} scheme file url status run rps failed non2xx reopened rate length row
	for scheme in $schemes; do for file in plain test.png; do
		url=$(base "$scheme")$path/$file

		# Warm up, and ask the cache what it says of the response: every
		# server has its header for that.
		ab -q -k -c "$CONCURRENCY" -n $((CONCURRENCY * 20)) "$url" >/dev/null 2>&1
		sleep 0.5
		status=$(curl -sk -o /dev/null -D - "$url" | tr -d '\r' | awk '
			tolower($1) ~ /^(cache-status|x-cache-status|x-cache):$/ {
				sub(/^[^ ]* /, ""); sub(/ttl=[0-9-]*; /, ""); sub(/key=[^;]*(; |$)/, ""); sub(/; $/, ""); print
			}')

		: >"$work/runs.txt"
		for run in $(seq "$RUNS"); do
			if ! taskset -c "$ab_cpus" ab -q -k -c "$CONCURRENCY" -t "$TIMELIMIT" -n "$REQUESTS" -e "$work/percentiles.$run.csv" "$url" >"$work/ab.txt" 2>&1; then
				echo "compare: ab failed on $url:" >&2
				cat "$work/ab.txt" >&2
				exit 1
			fi
			failed=$(field "Failed requests" 1)
			non2xx=$(field "Non-2xx responses" 1)
			failed=$((${failed:-0} + ${non2xx:-0}))
			failures=$((failures + failed))
			reopened=$(($(field "Complete requests" 1) - $(field "Keep-Alive requests" 1)))
			echo "$(field "Requests per second" 1) $run $failed $reopened $(field "Transfer rate" 1) $(field "Document Length" 1)" >>"$work/runs.txt"
		done
		read -r rps run failed reopened rate length < <(sort -n "$work/runs.txt" | sed -n "$(((RUNS + 1) / 2))p")

		# What would make a row say something else than it seems to: a server
		# that closes the connections, or one that answers something else.
		[ "$reopened" -gt 0 ] && status="$status (connections reopened: $reopened)"
		case $file:$length in plain:13 | test.png:43366) ;; *) status="$status (WRONG LENGTH: $length)" ;; esac

		row=$name
		[ "$scheme" = https ] && row=$tlsname
		# shellcheck disable=SC2059
		printf "$format" "$row" "$(printf '%.0f' "$rps")" \
			"$(awk -F, '$1 == 50 { printf "%.2f", $2 }' "$work/percentiles.$run.csv")" \
			"$(awk -F, '$1 == 99 { printf "%.2f", $2 }' "$work/percentiles.$run.csv")" \
			"$(awk -v rate="$rate" 'BEGIN { printf "%.1f", rate / 1024 }')" \
			"$failed" "${status:--}" >>"$work/table.$scheme.$file"
		echo "  $row, $scheme, $file: $(printf '%.0f' "$rps") req/s" >&2
	done; done
}

taskset -c "$upstream_cpus" caddy-ours run --config $conf/upstream.Caddyfile --adapter caddyfile >"$work/upstream.log" 2>&1 &
pids+=("$!")
until curl -sf -o /dev/null http://127.0.0.1:9181/plain; do sleep 0.1; done

start "$server_cpus" caddy-ours run --config $conf/ours.Caddyfile --adapter caddyfile
ready /direct/plain
bench "Caddy, no cache: respond / file_server" /direct
bench "Caddy, no cache: reverse_proxy" /proxy
bench "this module" /cache
bench "this module, max_memory off" /disk
stop

for storage in default otter simplefs; do
	case $storage in
	default) : >$conf/souin-storage ;;
	otter) echo otter >$conf/souin-storage ;;
	simplefs) printf 'simplefs {\n\tconfiguration {\n\t\tsize 100000\n\t\tpath /tmp/souin-simplefs\n\t}\n}\n' >$conf/souin-storage ;;
	esac
	start "$server_cpus" caddy-souin run --config $conf/souin.Caddyfile --adapter caddyfile
	ready /cache/plain
	bench "caddyserver/cache-handler, $storage storage" /cache
	stop
done

mkdir -p /tmp/nginx-cache
start "$server_cpus" nginx -c $conf/nginx.conf -g "worker_processes $CORES; daemon off;"
ready /direct/plain
bench "nginx, no cache: return / static file" /direct
bench "nginx, no cache: proxy_pass" /proxy
bench "nginx, proxy_cache" /cache
stop

start "$server_cpus" varnishd -F -n /tmp/varnish -a 127.0.0.1:9180 -f $conf/varnish.vcl -s malloc,256m -t 3600
start "$server_cpus" hitch --daemon=off --user=hitch --group=hitch --workers="$CORES" \
	--frontend='[127.0.0.1]:9443' --backend='[127.0.0.1]:9180' $conf/hitch.pem
ready /cache/plain
bench "Varnish, pass (no cache)" /pass "Varnish behind hitch, pass (no cache)"
bench "Varnish, malloc storage" /cache "Varnish behind hitch, malloc storage"
stop

model=$(sed -n 's/^model name[^:]*: *//p' /proc/cpuinfo | head -n 1)
echo "${model:+$model, }$(uname -m), ${#cpus[@]} cores: server on CPU $server_cpus, ab on CPU $ab_cpus, upstream on CPU $upstream_cpus"
echo "Caddy $(caddy-ours version | cut -d' ' -f1), caddyserver/cache-handler $(awk '/cache-handler/ { print $2 }' /souin-versions.txt) (Souin $(awk '/souin/ { print $2 }' /souin-versions.txt)), $(nginx -v 2>&1 | sed 's/.*: //'), $(varnishd -V 2>&1 | sed -n '1s/.*(\(varnish-[0-9.]*\).*/\1/p'), $(hitch --version 2>&1 | head -n 1)"
echo "median of $RUNS runs of $REQUESTS requests or $TIMELIMIT seconds, $CONCURRENCY keep-alive connections"
echo
# shellcheck disable=SC2059
printf "$format" "" "req/s" "p50 ms" "p99 ms" "MiB/s" "failed" "the cache says"
echo
echo "Text, 13 bytes"
cat "$work/table.http.plain"
echo
echo "fixtures/test.png, 42KiB"
cat "$work/table.http.test.png"
echo
echo "Text, 13 bytes, over TLS, sendfile off"
cat "$work/table.https.plain"
echo
echo "fixtures/test.png, 42KiB, over TLS, sendfile off"
cat "$work/table.https.test.png"

if [ "$failures" -gt 0 ]; then
	echo >&2
	echo "compare: $failures requests failed or were not answered with a 2xx status" >&2
	exit 1
fi
