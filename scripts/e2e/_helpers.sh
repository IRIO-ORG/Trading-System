#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"

log() {
  printf "[%s] %s\n" "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "$*"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "ERROR: missing required command: $1" >&2
    exit 1
  }
}

ensure_port_forward() {
  # Uses global variables: PF_PID, AFE_ADDR
  if [[ -n "${AFE_SERVICE_EXTERNAL_IP:-}" ]]; then
    AFE_ADDR="${AFE_SERVICE_EXTERNAL_IP}:80"
    log "Using external AFE address: ${AFE_ADDR}"
    return 0
  fi

  require_cmd kubectl
  require_cmd lsof

  local local_port=50051
  # If port is already taken, don't try to port-forward.
  if lsof -iTCP:"${local_port}" -sTCP:LISTEN >/dev/null 2>&1; then
    AFE_ADDR="localhost:${local_port}"
    log "Port ${local_port} already in use; assuming AFE is reachable at ${AFE_ADDR}"
    return 0
  fi

  log "Creating port-forward: svc/afe-service -> localhost:${local_port}"
  kubectl port-forward svc/afe-service "${local_port}:80" >/tmp/afe-portforward.log 2>&1 &
  PF_PID=$!
  AFE_ADDR="localhost:${local_port}"

  # Wait until the port is listening.
  for _ in {1..30}; do
    if lsof -iTCP:"${local_port}" -sTCP:LISTEN >/dev/null 2>&1; then
      log "Port-forward ready at ${AFE_ADDR} (pid=${PF_PID})"
      return 0
    fi
    sleep 1
  done

  echo "ERROR: port-forward did not become ready. Check /tmp/afe-portforward.log" >&2
  exit 1
}

cleanup_port_forward() {
  if [[ -n "${PF_PID:-}" ]]; then
    log "Stopping port-forward (pid=${PF_PID})"
    kill "${PF_PID}" >/dev/null 2>&1 || true
    PF_PID=""
  fi
}

send_trade() {
  local symbol="$1"
  local side="$2"
  local price="$3"
  local size="$4"

  log "SubmitTrade: ${side} ${symbol} price=${price} size=${size}"

  (cd "${ROOT_DIR}" && \
    bazel run //client -- \
      -server_address "${AFE_ADDR}" \
      -endpoint "SubmitTrade" \
      -body "{\"trade\":{\"side\":\"${side}\",\"price\":\"${price}\",\"size\":\"${size}\",\"instrument\":{\"symbol\":\"${symbol}\"}}}")
}

start_cluster_log_tail() {
  # Tails logs from all system components in the background.
  # Uses global variable: LOG_TAIL_PID
  require_cmd kubectl
  log "Tailing logs (afe, worker, executed-trades-worker) in background..."
  kubectl logs -f -l 'app in (afe, worker, executed-trades-worker)' --max-log-requests=10 2>/dev/null &
  LOG_TAIL_PID=$!
}

stop_cluster_log_tail() {
  if [[ -n "${LOG_TAIL_PID:-}" ]]; then
    log "Stopping log tail (pid=${LOG_TAIL_PID})"
    kill "${LOG_TAIL_PID}" >/dev/null 2>&1 || true
    LOG_TAIL_PID=""
  fi
}

get_worker_pod() {
  require_cmd kubectl
  kubectl get pod -l app=worker -o jsonpath='{.items[0].metadata.name}'
}

restart_worker_with_snapshot_settings() {
  # restart_worker_with_snapshot_settings INTERVAL_SECONDS THRESHOLD
  local interval="$1"
  local threshold="$2"
  require_cmd kubectl

  log "Configuring worker snapshot settings: SNAPSHOT_INTERVAL_SECONDS=${interval}, SNAPSHOT_ORDER_THRESHOLD=${threshold}"
  kubectl set env deployment/worker-deployment \
    SNAPSHOT_INTERVAL_SECONDS="${interval}" \
    SNAPSHOT_ORDER_THRESHOLD="${threshold}" >/dev/null
  kubectl rollout restart deployment/worker-deployment >/dev/null
  log "Waiting for worker rollout to complete..."
  kubectl rollout status deployment/worker-deployment --timeout=180s
}

delete_worker_pod() {
  require_cmd kubectl
  local pod
  pod="$(get_worker_pod)"
  log "Deleting worker pod to simulate crash: ${pod}"
  kubectl delete pod "${pod}" --grace-period=0 --force
}

wait_for_worker_ready() {
  require_cmd kubectl
  log "Waiting for worker to be ready..."
  kubectl rollout status deployment/worker-deployment --timeout=180s
}

postgres_dump_trades() {
  # Prints a lightweight summary of the trades table.
  require_cmd kubectl

  # Try the common Bitnami labels first.
  local pg_pod
  pg_pod="$(kubectl get pods -l app.kubernetes.io/instance=my-postgres -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -z "${pg_pod}" ]]; then
    # Fallback: statefulset-style name from the README.
    pg_pod="$(kubectl get pods -o name | grep -E 'pod/my-postgres-postgresql-0' | sed 's#pod/##' || true)"
  fi
  if [[ -z "${pg_pod}" ]]; then
    log "WARNING: could not find Postgres pod to query trades. Skipping DB dump."
    return 0
  fi

  local container
  container="$(kubectl get pod "${pg_pod}" -o jsonpath='{.spec.containers[0].name}')"

  log "Postgres pod detected: ${pg_pod} (container=${container})"
  log "DB summary: trades count + last 30 rows"

  # Uses the default creds from the Helm install in README (postgres/postgres, trading_db).
  kubectl exec -i "${pg_pod}" -c "${container}" -- bash -lc \
    "export PGPASSWORD=postgres; psql -U postgres -d trading_db -c 'SELECT COUNT(*) AS trade_count FROM trades;' && \
     psql -U postgres -d trading_db -c 'SELECT execution_id, symbol, price, size, bid_request_id, ask_request_id, executed_at FROM trades ORDER BY executed_at DESC NULLS LAST LIMIT 30;'"
}