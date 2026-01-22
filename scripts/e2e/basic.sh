#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/_helpers.sh"

PF_PID=""
LOG_TAIL_PID=""
AFE_ADDR=""

trap 'stop_cluster_log_tail; cleanup_port_forward' EXIT

require_cmd bazel

ensure_port_forward
start_cluster_log_tail

log "=== E2E #1: basic trading execution ==="
log "This will submit a few BUY/SELL orders for multiple symbols with varying prices/sizes, including partial fills."

# GOOG: seed asks, then cross with a larger buy (partial fill), then add bids and cross with a sell.
send_trade "GOOG" "SELL" 100 10
send_trade "GOOG" "SELL" 101 7
send_trade "GOOG" "BUY" 101 12
send_trade "GOOG" "BUY" 100 5
send_trade "GOOG" "SELL" 99 3

# AAPL: seed bids, then sell through multiple bid levels (partial), then create an ask and cross it.
send_trade "AAPL" "BUY" 200 8
send_trade "AAPL" "BUY" 199 6
send_trade "AAPL" "SELL" 199 10
send_trade "AAPL" "SELL" 201 2
send_trade "AAPL" "BUY" 205 1

# MSFT: mixed, leaves both a resting bid and a resting ask.
send_trade "MSFT" "SELL" 300 4
send_trade "MSFT" "SELL" 302 9
send_trade "MSFT" "BUY" 305 6
send_trade "MSFT" "BUY" 301 5
send_trade "MSFT" "SELL" 301 2

log "Submitted all orders. Waiting 8s for the system to process and logs to flush..."
sleep 8

log "Done. Inspect the logs above for 'WORKER: executed' and 'Persisted executed trade'."