# Checkpoint-Aware Benchmark Instructions

This runbook has two independent parts:

- **Part A — Build and deploy software:** run only after code or image changes.
- **Part B — Run a benchmark:** run once for every query/policy experiment.

Do not rebuild images while a benchmark is running. Do not run
`scaling-kafka-coordinator.py`; checkpoint-aware rescaling keeps Kafka and the
producer live while Flink restores state and offsets.

## Part A: Build and deploy software

Use one c165 terminal for this entire part. Part A does not need a query,
policy, or run ID.

### A1. Update the repository

Stop if the first command reports local changes:

```bash
ssh -A c165
cd ~/flink-kubernetes-autoscaling

git status --short
git fetch origin
git switch benchmark-checkpoint-rescaling
git pull --ff-only
git submodule sync --recursive
git submodule update --init --recursive

git log -3 --oneline
git submodule status \
  sources/operators/flink-kubernetes-operator-justin
```

The operator submodule must point to the commit recorded by the outer branch.

### A2. Load build settings

These settings select dedicated images and the Justin operator source without
creating benchmark run state:

```bash
export KUBECONFIG=/etc/flink-kubernetes-autoscaling/kubeconfig
export FLINK_RUNTIME_IMAGE_TAG=benchmark-checkpoint-rescale
export FLINK_BENCHMARK_IMAGE_TAG=benchmark-checkpoint-rescale
export OPERATOR_IMAGE_TAG=benchmark-checkpoint-rescale
export OPERATOR_SOURCE_DIR=sources/operators/flink-kubernetes-operator-justin
unset HELM_CHART

source scripts/env.sh

printf 'kubeconfig=%s\nruntime=%s\nbenchmark=%s\noperator_source=%s\noperator=%s\n' \
  "${KUBECONFIG}" \
  "${FLINK_RUNTIME_IMAGE}" \
  "${FLINK_BENCHMARK_IMAGE}" \
  "${OPERATOR_SOURCE_DIR}" \
  "${OPERATOR_IMAGE}"

test "${KUBECONFIG}" = /etc/flink-kubernetes-autoscaling/kubeconfig
test "${OPERATOR_SOURCE_DIR}" = sources/operators/flink-kubernetes-operator-justin
test "${OPERATOR_IMAGE_TAG}" = benchmark-checkpoint-rescale
```

If the shell or tmux session changes, repeat A2 before continuing.

### A3. Build the images

```bash
sudo bash images/flink-runtime/build.sh \
  --source-dir "${FLINK_RUNTIME_SOURCE_DIR}" \
  --tag "${FLINK_RUNTIME_IMAGE_TAG}"

sudo bash images/flink-benchmark-runtime/build.sh \
  --runtime-image "${FLINK_RUNTIME_LOCAL}" \
  --tag "${FLINK_BENCHMARK_IMAGE_TAG}"

sudo bash images/flink-kubernetes-operator/build.sh \
  --source-dir "${OPERATOR_SOURCE_DIR}" \
  --tag "${OPERATOR_IMAGE_TAG}"
```

Confirm all three local images exist:

```bash
sudo docker image inspect \
  "${FLINK_RUNTIME_LOCAL}" \
  "${FLINK_BENCHMARK_LOCAL}" \
  "${OPERATOR_LOCAL}" \
  --format '{{.Id}} {{.RepoTags}}'
```

### A4. Push and pre-pull the images

```bash
sudo docker tag "${FLINK_RUNTIME_LOCAL}" "${FLINK_RUNTIME_IMAGE}"
sudo docker push "${FLINK_RUNTIME_IMAGE}"

sudo docker tag "${FLINK_BENCHMARK_LOCAL}" "${FLINK_BENCHMARK_IMAGE}"
sudo docker push "${FLINK_BENCHMARK_IMAGE}"

sudo docker tag "${OPERATOR_LOCAL}" "${OPERATOR_IMAGE}"
sudo docker push "${OPERATOR_IMAGE}"

scripts/autoscaling/cluster-management/05-prepull-images.sh \
  --target-host c153
```

### A5. Deploy and verify the operator

Rerun A2 first if this is a new shell. Then deploy:

```bash
scripts/autoscaling/cluster-management/04-deploy-operator.sh

# The image tag is intentionally reused. Force a new pod so Always pulls the
# registry image that was just pushed.
kubectl rollout restart deployment/flink-kubernetes-operator \
  --namespace default

kubectl rollout status deployment/flink-kubernetes-operator \
  --namespace default \
  --timeout=180s

kubectl get deployment flink-kubernetes-operator \
  --namespace default \
  -o jsonpath='{range .spec.template.spec.containers[*]}{.name}{"  "}{.image}{"\n"}{end}'

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
  grep -i '^Docker-Content-Digest:' | awk '{print $2}' | tr -d '\r')"

printf 'running=%s\nregistry=%s\n' \
  "${RUNNING_DIGEST}" "${REGISTRY_DIGEST}"

if [[ -n "${RUNNING_DIGEST}" && "${RUNNING_DIGEST}" == "${REGISTRY_DIGEST}" ]]; then
  echo "Operator image digest verified"
else
  echo "Operator image digest mismatch; do not start the benchmark" >&2
fi
```

The deploy command must use the chart under
`sources/operators/flink-kubernetes-operator-justin`. Both reported container
images must end in `flink-kubernetes-operator:benchmark-checkpoint-rescale`.
Because this workflow reuses that mutable tag, `rollout restart` is required
after every push. Do not continue to Part B unless the running and registry
digests are non-empty and identical.

Part A is now complete. Do not repeat it for every benchmark run unless code
or image contents changed.

## Part B: Run one benchmark

Part B uses at most five c165 terminals. Every terminal must independently
load the same query, policy, and run ID. Part B does not build or push images.

### B1. Shared environment for every terminal

```bash
ssh -A c165
cd ~/flink-kubernetes-autoscaling

export QUERY=q20       # q4, q9, q18, q19, or q20
export POLICY=justin   # justin or ds2
export RUN_ID=20260811-q20-justin-01

source experiments/1729-checkpoint-aware-rescaling/run-env.sh
```

Use a new `RUN_ID` for every independent run. Never reuse an ID whose NFS
state directory may still exist. Use a different ID for the matched DS2 run.
If an old ID must be reused, follow B2 before resetting Kafka or deploying.

### B2. Optional: remove retained state before reusing a run ID

Skip this section when using a new `RUN_ID`. For reuse, first inspect the
complete run directory:

```bash
experiments/1729-checkpoint-aware-rescaling/cleanup-run-state.sh status
```

Archive any required evidence, stop the old FlinkDeployment, and wait for all
Flink pods to disappear:

```bash
scripts/autoscaling/job-management/stop-job.sh flink || true
kubectl wait --for=delete flinkdeployment/flink --timeout=180s 2>/dev/null || true
kubectl get flinkdeployments --all-namespaces
kubectl get pods --all-namespaces -l app=flink
```

Only when both final commands are empty, delete the whole run-scoped
checkpoint/savepoint/HA directory with an exact ID confirmation:

```bash
experiments/1729-checkpoint-aware-rescaling/cleanup-run-state.sh \
  delete --confirm-run-id "${RUN_ID}"
```

The helper refuses deletion if it cannot reach Kubernetes or finds any
FlinkDeployment or Flink pod. Never delete individual `chk-*`, `_metadata`,
`shared`, `savepoints`, or `ha` entries because incremental checkpoints may
reference shared files.

### Terminal 1: clean, deploy the job, and start the producer

Stop any previous run. This does not delete retained checkpoint state:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh stop || true
scripts/autoscaling/job-monitoring/port-forward.sh stop || true
scripts/autoscaling/job-management/stop-job.sh flink || true

kubectl wait --for=delete flinkdeployment/flink --timeout=180s 2>/dev/null || true
kubectl get flinkdeployments
kubectl get pods -l app=flink
```

Reset Kafka once, render the selected independent manifest, validate it, and
deploy the consumer job:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-external-kafka.sh reset

experiments/1729-checkpoint-aware-rescaling/render-job.sh

grep -E 'image:|upgradeMode:|state.checkpoints.dir:' \
  "${RENDERED_MANIFEST}"
kubectl apply --dry-run=server -f "${RENDERED_MANIFEST}"
kubectl apply -f "${RENDERED_MANIFEST}"

kubectl get flinkdeployment flink -w
```

The rendered image must be `benchmark-checkpoint-rescale`, upgrade mode must
be `last-state`, and checkpoint storage must contain `${RUN_ID}`. Wait for
`JOB STATUS=RUNNING` and `LIFECYCLE STATE=STABLE`, then press `Ctrl-C`.

Do not start the producer until terminals 3, 4, and 5 are observing the job.
Once they are ready, start the bounded producer exactly once:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh start
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh logs
```

The helper lives under the Q20 experiment for historical reasons but produces
the shared Kafka input used by all five queries.

### Terminal 2: observe the deployment and pods

```bash
watch -n 5 \
  'kubectl get flinkdeployment flink; kubectl get pods -l app=flink -o wide'
```

The FlinkDeployment remains present while the execution graph and TaskManager
resources change.

### Terminal 3: observe live Flink and Prometheus metrics

c165 port 8081 belongs to `kube-state-metrics`. The helper therefore uses
18082 for Flink, 19091 for Prometheus, and 3001 for Grafana:

```bash
scripts/autoscaling/job-monitoring/port-forward.sh start
scripts/autoscaling/job-monitoring/port-forward.sh status

scripts/autoscaling/job-monitoring/observe-flink-metrics.py \
  --interval 5 \
  --rate-window 30s \
  --cpu-rate-window 2m \
  --total-events "${EVENTS}" \
  --source-event-share "${SOURCE_EVENT_SHARE}"
```

`start` waits until both Flink REST and Prometheus respond. If Flink REST is
still starting and the underlying kubectl process exits, the helper retries
until `PORT_FORWARD_READY_TIMEOUT` (60 seconds by default). Forwards are
detached from the current tmux pane, and `stop` only terminates PIDs previously
created and recorded by this helper; it does not use a broad `pkill`.

If a local port becomes occupied, set `FLINK_LOCAL_PORT`,
`PROMETHEUS_LOCAL_PORT`, or `GRAFANA_LOCAL_PORT` before starting the helper.
The observers use the same environment variables.

### Terminal 4: observe checkpoint-rescale transactions

```bash
experiments/1729-checkpoint-aware-rescaling/observe-checkpoint-rescale.py \
  --interval 5
```

This shows the transaction phase, checkpoint ID, frozen target, restore
verification, restart duration, and latched failures.

### Terminal 5: observe autoscaler decisions

```bash
scripts/autoscaling/job-monitoring/observe-scaling.py \
  --configmap autoscaler-flink \
  --follow
```

It must report the algorithm selected by `${POLICY}`.

### Expected rescale sequence

```text
Justin or DS2 decision
  -> fresh checkpoint triggered and completed
  -> target parallelism and optional memory applied
  -> execution graph rebuilt from checkpoint state
  -> Kafka sources resume from checkpointed offsets
  -> accumulated Kafka backlog is drained
```

Kafka topics and the producer remain live throughout this sequence. If a
transaction enters `FAILED`, preserve the job and inspect it before retrying:

```bash
experiments/1729-checkpoint-aware-rescaling/manage-transaction.sh status
experiments/1729-checkpoint-aware-rescaling/manage-transaction.sh retry
```

### Save evidence before stopping

Keep terminal 3 and the port-forwards alive:

```bash
experiments/1724-kafka-q20-unique/analysis/export-experiment-data.py \
  --name "${RUN_ID}" \
  --flink-url http://localhost:18082 \
  --prom-url http://localhost:19091

FLINK_URL=http://localhost:18082 \
  experiments/1729-checkpoint-aware-rescaling/collect-evidence.sh
```

### Stop the run

After evidence has been saved:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh stop
scripts/autoscaling/job-management/stop-job.sh flink
experiments/1724-kafka-q20-unique/external-kafka/run/manage-external-kafka.sh stop
scripts/autoscaling/job-monitoring/port-forward.sh stop
```

This does not delete retained state under `${RUN_STORAGE_ROOT}`. Remove a
complete run directory only after evidence is archived and no deployment can
restore from it.
