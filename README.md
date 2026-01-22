# Trading System - Distributed computing infrastructure engineering
This project is part of Distributed computing infrastructure engineering course at University of Warsaw during the 2025/2026 academic year.

The course is realised in collaboration with Google and one of the employees was a mentor.

Authors: Tomasz Głąb, Hubert Krupniewski. Piotr Wieczorek


## How to run on GCP
 
There are two ways to run the project on GCP:

1. Automatically, using ```sh scripts/deploy.sh``` script.
2. Manually, following the instructions below.


### Create cluster
```sh
gcloud container clusters create trading-cluster \
--zone europe-central2-b \
--num-nodes 1 \
--machine-type e2-standard-2
```

### Get credentials

```sh
gcloud container clusters get-credentials trading-cluster --zone europe-central2-b
```

[//]: # (### Add kafka repository to helm)

[//]: # (```sh)

[//]: # (# Add bitnami repository)

[//]: # (helm repo add bitnami https://charts.bitnami.com/bitnami)

[//]: # ()
[//]: # (# Update helm repositories)

[//]: # (helm repo update)

[//]: # (```)

### Install KRaft Kafka with the usage of helm

```sh
helm install my-kafka oci://registry-1.docker.io/bitnamicharts/kafka -f k8s/kafka-values.yaml
```

If you want to update `kafka-values.yaml` and reload Kafka, you can use: 

```sh
helm uninstall my-kafka && kubectl delete pvc data-my-kafka-controller-0
```

### Install postgres DB with the usage of helm:

```sh
helm install my-postgres oci://registry-1.docker.io/bitnamicharts/postgresql \
      --set auth.postgresPassword=postgres \
      --set auth.database=trading_db \
      --wait
```

And then, proceed with install once again.
### Deploy to kubernetes
Command below does the following:
- Starts the job that creates kafka topics.
- Deploys worker, application-frontend, application-frontend service and executed-trades worker.
- Throws an error that `kafka-values.yaml` can't be processed - can be ignored.
```sh
kubectl apply -f k8s/
```


### Check if it works

In a new terminal tab run the following command, to see the logs from all the components:
```sh
kubectl logs -f -l 'app in (afe, worker, executed-trades-worker)' --max-log-requests=10
```

If everything deployed correctly `afe-service` should be accessible from outside of the cluster.
To obtain the `extrernal_ip` you have to run

```sh
kubectl get svc
```

Send a few requests to the system with the use of the client:
```sh
# Remeber to set AFE_SERVICE_EXTERNAL_IP variable or 
# to replace it with the IP address of the service

# Sell GOOG 100 @ 10
bazel run //client -- \
  -server_address "{AFE_SERVICE_EXTERNAL_IP}:80" \
  -endpoint "SubmitTrade" \
  -body '{"trade": {"side": "SELL", "price": "100", "size": "10", "instrument": {"symbol": "GOOG"}}}'

# Buy GOOG 100 @ 10
bazel run //client -- \
  -server_address "{AFE_SERVICE_EXTERNAL_IP}:80" \
  -endpoint "SubmitTrade" \
  -body '{"trade": {"side": "BUY", "price": "100", "size": "10", "instrument": {"symbol": "GOOG
```

Verify if outcome is desired.

Alternatively, you can setup port-forwarding and use the default address and port on the client which is `localhost:50051`.
### Clean up
**Remember to delete the cluster when you are done with it, to save the reosurces.**

Just run:

```sh
./scripts/destroy.sh
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
