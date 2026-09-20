#!/usr/bin/env bash
# Run a three-replica Keystone cluster on loopback.
#
# Client ports 9001-9003, peer ports 9101-9103, admin token "secret".
# Ctrl-C stops all three and drains them the way a real deploy would.
set -euo pipefail

BIN="${BIN:-./bin}"
DATA="${DATA:-/tmp/keystone-local}"
PEERS="n1=http://127.0.0.1:9101,n2=http://127.0.0.1:9102,n3=http://127.0.0.1:9103"
CLIENTS="n1=http://127.0.0.1:9001,n2=http://127.0.0.1:9002,n3=http://127.0.0.1:9003"

if [ ! -x "$BIN/keystoned" ]; then
  echo "build first: make build" >&2
  exit 1
fi

rm -rf "$DATA"
mkdir -p "$DATA"/n1 "$DATA"/n2 "$DATA"/n3

pids=()
cleanup() {
  echo
  echo "draining replicas..."
  # SIGTERM is a drain request: each replica stops taking client traffic,
  # hands off leadership, then stops replicating.
  kill "${pids[@]}" 2>/dev/null || true
  for p in "${pids[@]}"; do wait "$p" 2>/dev/null || true; done
  echo "stopped."
}
trap cleanup EXIT INT TERM

for i in 1 2 3; do
  "$BIN/keystoned" \
    -id "n$i" \
    -peers "$PEERS" \
    -client-peers "$CLIENTS" \
    -client-addr "127.0.0.1:900$i" \
    -peer-addr "127.0.0.1:910$i" \
    -data-dir "$DATA/n$i" \
    -fsync=false \
    -admin-token secret \
    -log-level info \
    > "$DATA/n$i.log" 2>&1 &
  pids+=($!)
done

sleep 3

cat <<'BANNER'

  Keystone is running on 127.0.0.1:9001-9003.

    export KEYSTONE_ENDPOINTS=http://127.0.0.1:9001,http://127.0.0.1:9002,http://127.0.0.1:9003
    export KEYSTONE_ADMIN_TOKEN=secret

    keystonectl status
    keystonectl put hello world
    keystonectl get hello
    keystonectl watch

  Logs are in /tmp/keystone-local/nN.log.  Note: -fsync=false is set for
  local speed; never run a real cluster that way.

  Ctrl-C to drain and stop.

BANNER

wait
