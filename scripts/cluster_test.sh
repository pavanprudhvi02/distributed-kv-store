#!/usr/bin/env bash
# End-to-end check against real processes: starts a 3-node cluster with
# 4 shards on localhost, writes and reads through kvctl, kills a node,
# keeps writing, restarts it and checks it caught up.
#
#   scripts/cluster_test.sh [build-dir]
set -euo pipefail

BUILD=${1:-build}
KVD="$BUILD/kvd"
KVCTL="$BUILD/kvctl"
DATA=$(mktemp -d)
CLUSTER="n1=127.0.0.1:7101,n2=127.0.0.1:7102,n3=127.0.0.1:7103"
ADDRS="127.0.0.1:7101,127.0.0.1:7102,127.0.0.1:7103"
declare -A PIDS

start() {
  local id=$1 port=$2
  "$KVD" --id "$id" --listen "127.0.0.1:$port" --cluster "$CLUSTER" --shards 4 --data "$DATA/$id" \
    > "$DATA/$id.log" 2>&1 &
  PIDS[$id]=$!
}

cleanup() {
  for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  if [[ "${KEEP_DATA:-0}" != 1 ]]; then rm -rf "$DATA"; fi
}
trap cleanup EXIT

fail() {
  echo "FAIL: $*"
  for f in "$DATA"/*.log; do echo "--- $f"; tail -20 "$f"; done
  exit 1
}

ctl() { "$KVCTL" --cluster "$ADDRS" "$@"; }

echo "== starting 3 nodes"
start n1 7101
start n2 7102
start n3 7103

echo "== writing 200 keys"
for i in $(seq 1 200); do ctl put "key:$i" "value-$i" > /dev/null || fail "put key:$i"; done

echo "== reading them back"
for i in 1 50 99 150 200; do
  got=$(ctl get "key:$i") || fail "get key:$i"
  [[ "$got" == "value-$i" ]] || fail "key:$i returned '$got'"
done

echo "== delete"
ctl del key:7 > /dev/null || fail "del"
if ctl get key:7 > /dev/null; then fail "key:7 still present after delete"; fi

ctl status

echo "== killing n1 (it leads some shards)"
kill "${PIDS[n1]}"
wait "${PIDS[n1]}" 2>/dev/null || true
unset 'PIDS[n1]'

echo "== writes and reads continue on the remaining majority"
for i in $(seq 201 300); do ctl put "key:$i" "value-$i" > /dev/null || fail "put key:$i with n1 down"; done
got=$(ctl get key:42) || fail "get key:42 with n1 down"
[[ "$got" == "value-42" ]] || fail "key:42 returned '$got' with n1 down"

echo "== restarting n1; it must catch up from the others"
start n1 7101
caught_up=0
for _ in $(seq 1 50); do
  # n1's applied index for every shard must reach that shard's leader's commit index.
  if "$KVCTL" --cluster "127.0.0.1:7101,127.0.0.1:7102,127.0.0.1:7103" status | awk '
      /node n1/ {n1=1; next} /node / {n1=0}
      /shard/ { if (n1) a[$2]=$NF; else if ($3=="leader") c[$2]=$(NF-2) }
      END { if (length(a)!=4) exit 1; for (s in c) if (a[s] < c[s]) exit 1; exit 0 }'; then
    caught_up=1
    break
  fi
  sleep 0.3
done
[[ $caught_up == 1 ]] || fail "n1 did not catch up"

echo "== all data still readable"
for i in 3 120 250 300; do
  got=$(ctl get "key:$i") || fail "get key:$i after restart"
  [[ "$got" == "value-$i" ]] || fail "key:$i returned '$got' after restart"
done

ctl status
echo "PASS"
