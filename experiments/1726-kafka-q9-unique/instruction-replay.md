# Q9 Justin Replay Run Instructions

This run uses the historical stateless-rescaling protocol on c165. It does not
restore Flink state or Kafka offsets after rescaling. Instead,
`scaling-kafka-coordinator.py` stops the producer, resets all three Kafka
topics, waits for the rebuilt execution graph, and restarts deterministic
Nexmark generation from event ID 1.

Use the separate benchmark worktree so the checkpoint-aware checkout remains
unchanged:

```bash
ssh -A c165
cd ~/flink-kubernetes-autoscaling-benchmark
```

The accepted Q9 configuration is 40,000 mixed Nexmark events/s and 100 million
events. The Flink consumer uses fixed bid source P6 and auction source P3;
Join and Rank/Sink start at P1 and are controlled by Justin. Each TaskManager
has 8 CPU cores, 3 GiB process memory, 8 slots, and 1264 MiB managed memory.

## Part A: update, compile, and deploy the corrected Justin operator

### A1. Update the benchmark worktree

Do not discard result directories or unrelated local changes. Inspect the
worktree before pulling:

```bash
cd ~/flink-kubernetes-autoscaling-benchmark
git status --short --branch
git fetch origin
git switch benchmark
git pull --ff-only
git submodule sync --recursive
git submodule update --init --recursive
```

Verify that the superproject contains the Q9 fix and that the Justin submodule
is pinned to the corrected commit:

```bash
git log -1 --oneline
git -C sources/operators/flink-kubernetes-operator-justin log -1 --oneline

test "$(git -C sources/operators/flink-kubernetes-operator-justin rev-parse HEAD)" = \
  e07e1635de857a6ce2438a6b70654283490dfe98
```

The final command must exit successfully. Commit `e07e163` preserves the
memory level of an unchanged vertex when another Q9 vertex scales.

### A2. Validate the observer

```bash
python3 -m unittest \
  scripts/autoscaling/job-monitoring/tests/test_observe_scaling.py
```

The test suite must report `OK`. The observer prints the number of consecutive
Justin metrics windows in which neither horizontal nor vertical rescaling was
requested. This is a count only, not a steady-state classification.

### A3. Compile the corrected operator image

The code change is confined to the Justin Kubernetes Operator. The Flink
runtime and benchmark images do not need to be rebuilt.

Use a distinct image tag and explicitly select the Justin source tree. The
benchmark branch otherwise defaults `OPERATOR_SOURCE_DIR` to the A4S tree.

```bash
export KUBECONFIG=/etc/flink-kubernetes-autoscaling/kubeconfig
export OPERATOR_SOURCE_DIR=sources/operators/flink-kubernetes-operator-justin
export OPERATOR_IMAGE_TAG=justin-benchmark-q9-fix
source scripts/env.sh

sudo bash images/flink-kubernetes-operator/build.sh \
  --source-dir "${OPERATOR_SOURCE_DIR}" \
  --tag "${OPERATOR_IMAGE_TAG}"

sudo docker image inspect "${OPERATOR_LOCAL}" \
  --format '{{.Id}} {{.RepoTags}}'
```

The Docker build compiles the operator modules but skips Java tests. The
focused regression tests for this fix were previously run in the operator
module; A2 separately validates the updated host-side observer.

### A4. Push the operator image

```bash
sudo docker tag \
  "${OPERATOR_LOCAL}" \
  "${OPERATOR_IMAGE}"

sudo docker push "${OPERATOR_IMAGE}"
```

Record the digest printed by `docker push`.

### A5. Stop the checkpoint-aware run before replacing its operator

```bash
source experiments/1726-kafka-q9-unique/run-env.sh

experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh stop || true
scripts/autoscaling/job-monitoring/port-forward.sh stop || true
scripts/autoscaling/job-management/stop-job.sh flink || true

kubectl wait --for=delete flinkdeployment/flink --timeout=180s 2>/dev/null || true
kubectl get flinkdeployments
kubectl get pods -l app=flink
```

Do not deploy the replay operator while a checkpoint-rescale transaction is
active.

### A6. Deploy and verify the replay operator

Re-export the operator settings because `run-env.sh` only contains workload
settings:

```bash
export OPERATOR_SOURCE_DIR=sources/operators/flink-kubernetes-operator-justin
export OPERATOR_IMAGE_TAG=justin-benchmark-q9-fix
source scripts/env.sh

scripts/autoscaling/cluster-management/04-deploy-operator.sh

kubectl rollout status deployment/flink-kubernetes-operator \
  --namespace default \
  --timeout=180s

kubectl get deployment flink-kubernetes-operator \
  --namespace default \
  -o jsonpath='{range .spec.template.spec.containers[*]}{.name}{"  "}{.image}{"\n"}{end}'
```

Both containers must use
`flink-kubernetes-operator:justin-benchmark-q9-fix`. Verify the running image
digest against the registry:

```bash
OPERATOR_POD="$(kubectl get pods \
  --namespace default \
  -l app.kubernetes.io/name=flink-kubernetes-operator \
  -o jsonpath='{.items[0].metadata.name}')"

RUNNING_DIGEST="$(kubectl get pod "${OPERATOR_POD}" \
  --namespace default \
  -o json | jq -r \
  '.status.containerStatuses[] | select(.name == "flink-kubernetes-operator") | .imageID' | \
  sed 's/.*@//')"

REGISTRY_DIGEST="$(curl -fsSI \
  -H 'Accept: application/vnd.docker.distribution.manifest.v2+json' \
  "http://${REGISTRY}/v2/${OPERATOR_IMAGE_NAME}/manifests/${OPERATOR_IMAGE_TAG}" | \
  awk 'tolower($1) == "docker-content-digest:" {print $2}' | tr -d '\r')"

printf 'running=%s\nregistry=%s\n' \
  "${RUNNING_DIGEST}" "${REGISTRY_DIGEST}"

test -n "${RUNNING_DIGEST}"
test "${RUNNING_DIGEST}" = "${REGISTRY_DIGEST}"
```

Do not start Q9 if the final digest comparison fails.

## Part B: run Q9 with at most five terminals

In every terminal:

```bash
ssh -A c165
cd ~/flink-kubernetes-autoscaling-benchmark
source experiments/1726-kafka-q9-unique/run-env.sh
```

Confirm the workload before starting:

```bash
printf 'TPS=%s EVENTS=%s producer_P=%s max_emit=%s\n' \
  "${TPS}" "${EVENTS}" "${PARALLELISM}" "${MAX_EMIT_SPEED}"
```

Expected values are `TPS=40000`, `EVENTS=100000000`, producer P4, and
`max_emit=false`.

### Terminal 1: reset Kafka, deploy Q9, then start the producer

Ensure the previous job and producer are absent:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh stop || true
scripts/autoscaling/job-management/stop-job.sh flink || true
kubectl wait --for=delete flinkdeployment/flink --timeout=180s 2>/dev/null || true
```

Reset Kafka once and deploy the corrected Justin Q9 manifest:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-external-kafka.sh reset

kubectl apply --dry-run=server -k \
  experiments/1726-kafka-q9-unique/jobs/justin

kubectl apply -k experiments/1726-kafka-q9-unique/jobs/justin
kubectl get flinkdeployment flink -w
```

Wait for `JOB STATUS=RUNNING` and `LIFECYCLE STATE=STABLE`, then press
`Ctrl-C`. Do not start the producer until terminals 3 and 4 are observing and
terminal 5 prints `Tracking job`.

Then start the initial producer exactly once:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh start
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh logs
```

After later rescaling actions, terminal 5 restarts the producer automatically.

### Terminal 2: observe the deployment and TaskManagers

```bash
watch -n 5 \
  'kubectl get flinkdeployment flink; kubectl get pods -l app=flink -o wide'
```

During replay rescaling, the job briefly leaves `RUNNING`, the execution graph
is rebuilt, and the TaskManager set may change.

### Terminal 3: live Flink and Prometheus metrics

Use ports 18082 and 19091 to avoid c165's commonly occupied ports 8081 and
9091:

```bash
mkdir -p cluster/.runtime

kubectl port-forward --address 127.0.0.1 \
  svc/flink-rest 18082:8081 \
  > cluster/.runtime/flink-18082-port-forward.log 2>&1 &

kubectl port-forward --address 127.0.0.1 \
  -n manager svc/prom-kube-prometheus-stack-prometheus 19091:9090 \
  > cluster/.runtime/prometheus-19091-port-forward.log 2>&1 &

sleep 2
curl -sf http://localhost:18082/jobs/overview | jq '.jobs[] | {name, state}'
curl -sf http://localhost:19091/-/ready
```

Start the metrics observer:

```bash
scripts/autoscaling/job-monitoring/observe-flink-metrics.py \
  --flink-url http://localhost:18082 \
  --prometheus-url http://localhost:19091 \
  --interval 5 \
  --rate-window 30s \
  --cpu-rate-window 2m \
  --total-events "${EVENTS}" \
  --source-event-share "${SOURCE_EVENT_SHARE}"
```

Replay progress returns near zero after every Kafka reset because the current
configuration is re-evaluated from event ID 1.

### Terminal 4: observe Justin decisions and no-rescale windows

```bash
scripts/autoscaling/job-monitoring/observe-scaling.py \
  --configmap autoscaler-flink \
  --follow
```

It must print `Algorithm: justin`. Every period ends with a line such as:

```text
Consecutive windows without rescaling: 2
```

The counter resets to zero whenever any vertex requests horizontal or
vertical rescaling. It does not check throughput, Kafka lag, producer state,
or long-term state growth.

### Terminal 5: run the Kafka replay coordinator

Terminal 3 owns the Flink REST forward, so reuse it:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/scaling-kafka-coordinator.py \
  --tps "${TPS}" \
  --events "${EVENTS}" \
  --parallelism "${PARALLELISM}" \
  --max-emit-speed "${MAX_EMIT_SPEED}" \
  --producer-rest-port "${PRODUCER_REST_PORT}" \
  --flink-port 18082 \
  --no-port-forward
```

Wait for:

```text
Tracking job: ...
Tracking job name: q9_unique-...
Initial CREATED timestamp: ...
```

Only then start the producer in terminal 1.

For every detected rescale, the expected sequence is:

```text
scaling detected
  -> producer paused and stopped
  -> Kafka topics reset
  -> Flink job returns to RUNNING
  -> producer restarts from event ID 1
```

## Save the result before stopping

Keep terminal 3 and both forwards running. Choose a new result name:

```bash
export RESULT_NAME=20260822-q9-justin-40k-100m-memory-carry-forward-fix

python3 experiments/1724-kafka-q20-unique/analysis/export-experiment-data.py \
  --deployment flink \
  --prom-url http://localhost:19091 \
  --flink-url http://localhost:18082 \
  --step-seconds 15 \
  --name "${RESULT_NAME}" \
  --output-root experiments/1726-kafka-q9-unique/results

scripts/autoscaling/job-monitoring/observe-scaling.py \
  --configmap autoscaler-flink \
  > "experiments/1726-kafka-q9-unique/results/${RESULT_NAME}/scaling-history.txt"

kubectl get flinkdeployment flink -o yaml \
  > "experiments/1726-kafka-q9-unique/results/${RESULT_NAME}/rendered-manifest.yaml"
kubectl get configmap autoscaler-flink -o json \
  > "experiments/1726-kafka-q9-unique/results/${RESULT_NAME}/autoscaler-configmap.json"
```

Verify that the result directory exists before stopping the run.

## Stop the run

Press `Ctrl-C` in terminals 2--5, then run:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh stop
scripts/autoscaling/job-management/stop-job.sh flink

pkill -f '[k]ubectl port-forward.*flink-rest.*18082:8081' || true
pkill -f '[k]ubectl port-forward.*prom-kube-prometheus-stack-prometheus.*19091:9090' || true
```

External Kafka may remain running for another replay experiment. Stop it only
when it is no longer needed:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-external-kafka.sh stop
```
