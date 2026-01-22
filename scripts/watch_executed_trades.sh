#!/bin/bash
while true
do
  clear
  # Wyświetlenie nagłówka dla estetyki
  echo "--- Ostatnie 20 transakcji (odświeżanie co 1s) ---"

  kubectl exec my-postgres-postgresql-0 -- psql -U postgres -d trading_db -c "SELECT executed_at, symbol, price, size FROM trades ORDER BY executed_at DESC LIMIT 20;"

  sleep 1
done