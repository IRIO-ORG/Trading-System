#!/bin/bash
set -e

CLUSTER_NAME="trading-cluster"
ZONE="europe-central2-b"

echo "[1/7] Creating GKE cluster..."
# Check if cluster exists to avoid errors on re-runs
if gcloud container clusters list --filter="name:$CLUSTER_NAME" --format="value(name)" | grep -q "$CLUSTER_NAME"; then
    echo "   Cluster $CLUSTER_NAME already exists, skipping creation."
else
    gcloud container clusters create $CLUSTER_NAME \
        --zone $ZONE \
        --num-nodes 1 \
        --machine-type e2-standard-2 \
        --disk-size 20GB \
        --quiet
fi

echo "[2/7] Getting credentials..."
gcloud container clusters get-credentials $CLUSTER_NAME --zone $ZONE

echo "[3/7] Installing PostgreSQL..."
if helm status my-postgres > /dev/null 2>&1; then
    echo "   Postgres already installed."
else
    helm install my-postgres oci://registry-1.docker.io/bitnamicharts/postgresql \
      --set auth.postgresPassword=postgres \
      --set auth.database=trading_db \
      --wait
fi

echo "[4/7] Installing Kafka..."
if helm status my-kafka > /dev/null 2>&1; then
    echo "   Kafka already installed."
else
    helm install my-kafka oci://registry-1.docker.io/bitnamicharts/kafka -f k8s/kafka-values.yaml --wait
fi

echo "[5/7] Deploying application..."
kubectl apply -f k8s/config.yaml
kubectl apply -f k8s/

echo "[6/7] Waiting for External IP for AFE Service..."
# Loop until the Load Balancer IP is assigned
EXTERNAL_IP=""
while [ -z "$EXTERNAL_IP" ]; do
    echo "   Waiting for load balancer IP..."
    sleep 10
    EXTERNAL_IP=$(kubectl get svc afe-service -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
done
echo "   Service IP assigned: $EXTERNAL_IP"

echo "[7/7] Executing simple trades..."

echo "   1. Sending SELL order (100 GOOG @ 10)..."
bazel run //client -- \
  -server_address "${EXTERNAL_IP}:80" \
  -endpoint "SubmitTrade" \
  -body '{"trade": {"side": "SELL", "price": "100", "size": "10", "instrument": {"symbol": "GOOG"}}}'

echo -e "\n   2. Sending BUY order (100 GOOG @ 10) - should execute..."
bazel run //client -- \
  -server_address "${EXTERNAL_IP}:80" \
  -endpoint "SubmitTrade" \
  -body '{"trade": {"side": "BUY", "price": "100", "size": "10", "instrument": {"symbol": "GOOG"}}}'

echo -e "\nDeployment completed successfully."
echo "You can check the database in a separate terminal tab by running::"
echo "./scripts/watch_executed_trades.sh"