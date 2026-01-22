# End-to-End (E2E) scripts

These scripts are log-driven E2E scenarios. They do not contain assertions; they generate non-trivial traffic patterns and (optionally) simulate a worker crash so you can inspect logs and DB state.

They assume you deployed the system to Kubernetes as described in the top-level `README.md` (Kafka + Postgres via Helm, then `kubectl apply -f k8s/`).

Prerequisites:

- `kubectl` is configured to point at the cluster/namespace where the system is running.
- `bazel` is installed (the scripts use `bazel run //client` to send gRPC requests).
- `lsof` is available (used to detect whether the local port-forward is ready).

How the scripts connect to AFE:

- If `AFE_SERVICE_EXTERNAL_IP` is set, the scripts send gRPC to `${AFE_SERVICE_EXTERNAL_IP}:80`.
- Otherwise, they create a temporary port-forward from `svc/afe-service` to `localhost:50051`.

What to look for in logs:

- Worker logs:
  - `Processing Order ...` (incoming trade requests)
  - `WORKER: accepted (no match) ... offset=...`
  - `WORKER: executed ...` (executions emitted to Kafka)
  - `WORKER: snapshot batch published ... lastOffset=...` (periodic snapshots)
- Executed-trades worker logs:
  - `Persisted executed trade ...` (DB writes)

## Scripts

1. `basic.sh`

Sends a series of BUY/SELL requests for multiple symbols (different prices and sizes, partial fills, and leftovers).

2. `recovery.sh`

Configures the worker snapshot cadence for the run, waits for a snapshot, sends additional traffic, then deletes the worker pod mid-window to simulate a crash. After restart it prints a Postgres summary (`trades` table) so you can inspect what was persisted.

Notes:

- The scripts intentionally avoid pass/fail assertions.
- The recovery script uses `kubectl set env` on the `worker-deployment` to set:
  - `SNAPSHOT_INTERVAL_SECONDS=40`
  - `SNAPSHOT_ORDER_THRESHOLD=0` (disables threshold-triggered snapshots so the timing is predictable)

## Running

From the repo root:

```sh
chmod +x scripts/e2e/*.sh

# Optional: if you already have an external LB IP for AFE
export AFE_SERVICE_EXTERNAL_IP="x.x.x.x"

./scripts/e2e/basic.sh
./scripts/e2e/recovery.sh
```
