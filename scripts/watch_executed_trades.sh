#!/bin/bash

while true
do
  OUTPUT=$(kubectl exec my-postgres-postgresql-0 -- env PGPASSWORD=postgres psql -U postgres -d trading_db -c "SELECT executed_at, symbol, price, size FROM trades ORDER BY executed_at DESC LIMIT 20;" 2>&1)

  clear
  echo "--- Last 20 transactions (refreshed every 1s) ---"
  echo "$OUTPUT"

  sleep 1
done