#!/usr/bin/env bash
#
# A/B the server memory cost of many open HTTP connections, net/http vs nbio.
#
# For each engine this starts the httpengine server from this directory and
# drives it with "weed benchmark.connections", which holds CONNS separate TCP
# connections open and samples the server's own metrics while it does.
#
#   CONNS=50000 HOLD=30s ./run.sh
#
# Every connection costs a file descriptor on both sides, so raise the limit
# before a large run: ulimit -n 200000
set -euo pipefail

CONNS=${CONNS:-20000}
HOLD=${HOLD:-30s}
INTERVAL=${INTERVAL:-0}          # 0 holds the connections idle
DIALERS=${DIALERS:-256}
ADDR=${ADDR:-127.0.0.1:8081}
METRICS=${METRICS:-127.0.0.1:9101}
ENGINES=${ENGINES:-"nethttp nbio"}

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT

WEED=${WEED:-$out/weed}
if [ ! -x "$WEED" ]; then
    echo "building weed ..."
    (cd "$root" && go build -o "$out/weed" ./weed/)
fi

echo "building httpengine ..."
(cd "$here" && go build -o "$out/httpengine" .)

for engine in $ENGINES; do
    echo
    echo "==================== $engine ===================="
    "$out/httpengine" -engine="$engine" -addr="$ADDR" -metrics="$METRICS" &
    server=$!
    trap 'kill $server 2>/dev/null || true; rm -rf "$out"' EXIT

    for _ in $(seq 50); do
        if curl -sf "http://$METRICS/metrics" >/dev/null 2>&1; then break; fi
        sleep 0.1
    done

    "$WEED" benchmark.connections \
        -server="$ADDR" -path=/healthz -metrics="$METRICS" \
        -n="$CONNS" -c="$DIALERS" -hold="$HOLD" -interval="$INTERVAL"

    kill $server 2>/dev/null || true
    wait $server 2>/dev/null || true
    trap 'rm -rf "$out"' EXIT
done
