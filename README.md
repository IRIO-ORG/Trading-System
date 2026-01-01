# Trading System - Distributed computing infrastructure engineering
This project is part of Distributed computing infrastructure engineering course at University of Warsaw during the 2025/2026 academic year.

The course is realised in collaboration with Google and one of the employees was a mentor.

Authors: Tomasz Głąb, Hubert Krupniewski. Piotr Wieczorek


## How to run on GCP

In order to run the project on GCP with basic cluster setup, follow these steps:

### Create cluster
```sh
gcloud container clusters create trading-cluster \
--zone europe-central2-a \
--num-nodes 1 \
--machine-type e2-standard-2
```

### Get credentials

```sh
gcloud container clusters get-credentials trading-cluster --zone europe-central2-a
```

### Add kafka repository to helm
```sh
# Add bitnami repository
helm repo add bitnami https://charts.bitnami.com/bitnami

# Update helm repositories
helm repo update
```

### Install KRaft Kafka with the usage of helm

> **⚠️ Security Note:** The configuration below uses `PLAINTEXT` for communication. This is intended **strictly for development simplicity**. In a production environment, you must disable plaintext listeners and enforce encryption (TLS/SSL) and authentication (SASL/mTLS).

```sh
helm install my-kafka bitnami/kafka \
  --set kraft.enabled=true \
  --set zookeeper.enabled=false \
  --set controller.replicaCount=1 \
  --set listeners.client.protocol=PLAINTEXT \
  --set listeners.interbroker.protocol=PLAINTEXT \
  --set allowPlaintextListener=true \
  --set image.registry=docker.io \
  --set image.repository=bitnamilegacy/kafka
```

### Deploy to kubernetes

```sh
kubectl apply -f k8s/
```

### Check if it works

```sh
kubectl logs -l app=worker
```

### Clean up
**Remember to delete the cluster when you are done with it, to save the reosurces.**

```sh
gcloud container clusters delete trading-cluster --zone europe-central2-a
```

## Bazel cheatsheet

Fixup BUILD files, for example auto add dependencies:
```sh
bazel run //:gazelle
```

Depend on a new tool:
```sh
bazel run @rules_go//go -- get -tool your_tool_repo.com/x/useful/tool
```

To add a new external dependency from [Bazel Central Registry](https://registry.bazel.build/), add the dependency to `MODULE.bazel`:
```starlark
bazel_dep(name = "my_library", version = "1.0.0")
```
