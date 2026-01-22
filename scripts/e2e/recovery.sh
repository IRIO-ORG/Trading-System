#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/helpers.sh"

PF_PID=""
LOG_TAIL_PID=""
AFE_ADDR=""

trap 'stop_cluster_log_tail; cleanup_port_forward' EXIT

require_cmd bazel
require_cmd kubectl

ensure_port_forward
start_cluster_log_tail

log "=== E2E #2: recovery / crash simulation ==="
log "Plan: snapshot interval=40s, threshold=10, send initial trades, wait for snapshot, send more trades, crash worker after ~20s, observe restart + subsequent snapshots and DB state."

log "Step 1: Initial traffic (ensure worker has state before first timer snapshot)"
send_trade "GOOG" "SELL" 100 10
send_trade "GOOG" "BUY" 120 3
send_trade "AAPL" "BUY" 200 5
send_trade "AAPL" "SELL" 205 2

log "Waiting 45s so at least one timer snapshot is published (interval=40s)..."
sleep 45

log "Step 2: Traffic after the last snapshot (some should execute; some should remain resting)"
send_trade "GOOG" "SELL" 101 6
send_trade "GOOG" "BUY" 101 11
send_trade "GOOG" "BUY" 99 4
send_trade "GOOG" "SELL" 98 2

send_trade "AAPL" "BUY" 210 7
send_trade "AAPL" "SELL" 200 9
send_trade "AAPL" "SELL" 215 1

log "Sleeping 20s (mid-snapshot window) and then simulating worker crash..."
sleep 20

delete_worker_pod

log "Restarting log tail so it attaches to the new worker pod..."
stop_cluster_log_tail
start_cluster_log_tail

log "Waiting for worker to come back and be ready..."
wait_for_worker_ready

log "Step 3: Post-restart traffic (optional)"
log "Post-restart verification orders (should match pre-crash resting orders if recovery works):"
send_trade "GOOG" "BUY" 105 1
send_trade "AAPL" "BUY" 220 1
send_trade "AAPL" "SELL" 199 1

log "Waiting 45s for the next timer snapshot after restart (interval=40s)..."
sleep 45

log "Dumping Postgres 'trades' table summary (executed-trades worker persistence)"
postgres_dump_trades

log "Done. Inspect logs for:"
log "- snapshot batches ('WORKER: snapshot batch published', lastOffset)"
log "- re-processing/duplicates around the crash time (if any)"
log "- executed-trades persistence ('Persisted executed trade' and DB rows)"
