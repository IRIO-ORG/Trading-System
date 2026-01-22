#!/bin/bash
CLUSTER_NAME="trading-cluster"
ZONE="europe-central2-b"

echo "Destroying cluster $CLUSTER_NAME..."
gcloud container clusters delete $CLUSTER_NAME --zone $ZONE --quiet

echo "Cleaning up disks (if any remain)..."
# Sometimes PVC disks remain after cluster deletion and incur costs!
for disk in $(gcloud compute disks list --filter="zone:($ZONE) AND name~gke-$CLUSTER_NAME" --format="value(name)"); do
    echo "Deleting disk: $disk"
    gcloud compute disks delete $disk --zone $ZONE --quiet
done

echo "Environment cleaned up. Money is safe."