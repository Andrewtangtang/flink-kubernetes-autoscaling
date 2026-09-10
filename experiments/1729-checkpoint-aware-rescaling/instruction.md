# Checkpoint-Aware Benchmark Instructions

This runbook has two independent parts:

- **Part A — Build and deploy software:** run only after code or image changes.
- **Part B — Run a benchmark:** run once for every query/policy experiment.

Do not rebuild images while a benchmark is running. Do not run
`scaling-kafka-coordinator.py`; the producer controller pauses the existing
producer container before each rescale checkpoint and resumes the same
container after restore. Kafka topics are never reset during a run.

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
git switch producer-pause-checkpoint-rescaling
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

Part B uses five c165 terminals. Terminal 2 alone decides when the run is
complete; terminals 3-5 are diagnostic views. Every terminal must
independently load the same query, policy, and run ID. Part B does not build
or push images.

### B1. Shared environment for every terminal

```bash
ssh -A c165
cd ~/flink-kubernetes-autoscaling

export QUERY=q20       # q4, q9, q18, q19, or q20
export POLICY=justin   # justin or ds2
export RUN_ID=20260811-q20-justin-01

unset EVENTS           # discard a stale value loaded by an older run
source experiments/1729-checkpoint-aware-rescaling/run-env.sh
```

Use a new `RUN_ID` for every independent run. Never reuse an ID whose NFS
state directory may still exist. Use a different ID for the matched DS2 run.
If an old ID must be reused, follow B2 before resetting Kafka or deploying.
The default bounded horizon is 300 million mixed Nexmark events. It prevents
the producer from ending before the final stability windows; it is not the
required run duration. Stop after terminal 2 reports the experiment end
condition and the evidence has been collected. Use the same `EVENTS` value for
every policy compared on a query. Confirm the loaded value before deployment:

```bash
printf 'EVENTS=%s TPS=%s\n' "${EVENTS}" "${TPS}"
```

`POLICY=justin` or `POLICY=ds2` selects the scaler. `render-job.sh` loads the
independent `jobs/${QUERY}/${POLICY}/experiment.yaml` manifest and writes the
selected autoscaler settings into `${RENDERED_MANIFEST}`. The already deployed
Kubernetes Operator then runs that scaler automatically; do not start a
separate scaler process or the historical coordinator.

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

grep -E 'image:|upgradeMode:|state.checkpoints.dir:|job.autoscaler.(enabled|justin.enabled):' \
  "${RENDERED_MANIFEST}"
kubectl apply --dry-run=server -f "${RENDERED_MANIFEST}"
kubectl apply -f "${RENDERED_MANIFEST}"

kubectl get flinkdeployment flink -w
```

The rendered image must be `benchmark-checkpoint-rescale`, upgrade mode must
be `last-state`, checkpoint storage must contain `${RUN_ID}`, and the Justin
flag must match `${POLICY}`. Applying the manifest is what starts the selected
scaler. Wait for `JOB STATUS=RUNNING` and `LIFECYCLE STATE=STABLE`, then press
`Ctrl-C`.

Do not start the producer until terminals 2 to 5 are observing the job.
Once they are ready, start the bounded producer exactly once:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh start
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh logs
```

The helper lives under the Q20 experiment for historical reasons but produces
the shared Kafka input used by all five queries.

### Terminal 2: observe the complete benchmark

c165 port 8081 belongs to `kube-state-metrics`. The helper therefore uses
18082 for Flink, 19091 for Prometheus, and 3001 for Grafana:

```bash
scripts/autoscaling/job-monitoring/port-forward.sh start
scripts/autoscaling/job-monitoring/port-forward.sh status

scripts/autoscaling/job-monitoring/observe-benchmark.py \
  --interval 5 \
  --rate-window 30s \
  --cpu-rate-window 2m
```

`start` waits until both Flink REST and Prometheus respond. If Flink REST is
still starting and the underlying kubectl process exits, the helper retries
until `PORT_FORWARD_READY_TIMEOUT` (60 seconds by default). Forwards are
detached from the current tmux pane, and `stop` only terminates PIDs previously
created and recorded by this helper; it does not use a broad `pkill`.

If a local port becomes occupied, set `FLINK_LOCAL_PORT`,
`PROMETHEUS_LOCAL_PORT`, or `GRAFANA_LOCAL_PORT` before starting the helper.
The observers use the same environment variables.

This is the only observer required for a normal run. It combines the job and
TaskManager state, live metrics, checkpoint transaction, scaler decision,
producer status, Kafka lag, and capacity-stability result. After the latest
completed rescale it waits one minute and evaluates three consecutive
two-minute windows.
Every accepted window requires a live producer within 5% of `${TPS}`, no
parallelism/memory or transaction change, complete metric coverage, and no
persistent Kafka-lag growth. It reports `CAPACITY_STABLE_CATCHING_UP` while
lag is falling, `CAPACITY_STABLE_LAG_FLAT` when a positive backlog is not
draining, and `FULLY_CAUGHT_UP` when lag is at most two seconds of target input
and its slope is flat. A stopped producer is
`INCONCLUSIVE_PRODUCER_STOPPED`, not steady state.

When three consecutive windows pass, the observer first reports a provisional
`CONFIRMING_STABILITY` candidate. It waits an additional 30-second confirmation
guard; a new transaction, failed window, or resource change during that guard
cancels the candidate. Only after the guard does it latch the accepted result
and print:

```text
*** EXPERIMENT END CONDITION REACHED ***
Accepted: CAPACITY_STABLE_CATCHING_UP at <UTC timestamp>
Save evidence before stopping the producer and Flink job.
```

The accepted result remains visible even if the bounded producer finishes
afterward. If a later rescale occurs, the observer reports
`STABILITY_REVOKED_BY_RESCALE` as a prior event instead of silently erasing the
episode. Add `--exit-when-stable` when automation should print the banner and
exit successfully; the observer never stops the producer or job itself.

The throughput line is a 30-second task-rate average. Replay progress uses
the Kafka reader offset of every source partition, so it remains cumulative
when checkpoint rescaling recreates the execution graph. If those metrics are
temporarily unavailable, the observer labels its current-attempt task counter
as a fallback; do not use that fallback as final completion evidence.

During initial warm-up, `StateLat` can briefly read `0.0` before Flink lazily
registers the state-latency histogram. See "State-latency warm-up" below.

### Terminal 3: observe the deployment and pods

```bash
watch -n 5 \
  'kubectl get flinkdeployment flink; kubectl get pods -l app=flink -o wide'
```

This is the fastest view for pod-level problems such as a TaskManager stuck
`Pending`, node disk pressure, or an image-pull failure.

### Terminal 4: control producer pause and resume

```bash
experiments/1729-checkpoint-aware-rescaling/checkpoint-producer-controller.py \
  --interval 1
```

This controller must be running before the producer starts. It pauses the
existing producer container when a transaction enters
`WAITING_PRODUCER_PAUSE`, writes a transaction-specific acknowledgement to the
FlinkDeployment, and resumes the same container only after restore reaches
`WAITING_PRODUCER_RESUME`. The commands are idempotent. If a transaction fails
after pause, the producer remains paused so evidence can be collected; repair
and retry or resume it manually only after diagnosing the failure.

### Terminal 5: observe autoscaler decisions

```bash
scripts/autoscaling/job-monitoring/observe-scaling.py \
  --configmap autoscaler-flink \
  --follow
```

This prints each per-period decision, including parallelism, memory level,
window-average capacity, cache hit rate, state latency, and H/V flags. Keep
it running for Justin experiments because the memory-level path is visible
there.

### Expected rescale sequence

```text
Justin or DS2 decision
  -> producer paused and pause acknowledged
  -> fresh checkpoint triggered and completed
  -> target parallelism and optional memory applied
  -> execution graph rebuilt from checkpoint state
  -> Kafka sources resume from checkpointed offsets
  -> producer resumed and resume acknowledged
```

The producer process is frozen rather than stopped, so its event counter and
configuration are preserved. Kafka topics remain intact. Events already in
Kafka before the pause may still be consumed while checkpointing, but no new
events are generated during checkpoint/apply/restore. If a transaction enters
`FAILED`, preserve the job and inspect it before retrying:

```bash
experiments/1729-checkpoint-aware-rescaling/manage-transaction.sh status
experiments/1729-checkpoint-aware-rescaling/manage-transaction.sh retry
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh status
```

A successful pre-apply `abort` is detected by the controller and resumes a
paused producer. For a post-apply failure, keep the controller running and use
`retry`; do not manually resume input while the execution graph is still being
repaired.

### State-latency warm-up

Justin's decision blocks can report `StateLat=0.0` for a stateful vertex whose
state latency Flink is measuring normally. Confirmed on the Q20 Join, where
Flink reported `valueStateGetLatency_p90` around 19,000 ns while the scaler
recorded `0.0`.

The cause is metric-name resolution order, not a missing metric. Flink registers
latency-tracking histograms lazily on the first tracked state access. Because
the job reaches `RUNNING` before the producer starts, the first metric-name
query can occur before any `*StateGetLatency_p90` metric exists. RocksDB
block-cache metrics register earlier, which is why `CacheHit` may already have
a value in the same decision block.

The collector now re-queries a RocksDB-backed vertex whose state-get latency
metric has not appeared yet, then caches the complete mapping after discovery.
Required base metrics still fail closed, while optional Justin state metrics
may register later. A warm-up `0.0` must therefore be treated as unavailable;
later decision periods should become nonzero without restarting the Operator.
If they do not, preserve the Operator logs and Flink metric-name response rather
than interpreting zero as a measured latency below the 1 ms threshold.

### Save evidence before stopping

Keep terminal 2 and the port-forwards alive:

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
