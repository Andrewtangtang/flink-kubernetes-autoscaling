# Q20 Checkpoint-Aware Justin and DS2 Pilot

For the current five-terminal operator runbook, see
[`instruction.md`](instruction.md).

This pilot keeps the standalone producer and Kafka topics running while every
Justin or DS2 decision is gated by a fresh completed Flink checkpoint. It is
separate from the paper-reproduction coordinator, which intentionally resets
Kafka and replays bounded input after each stateless rescale.

The first decision uses a one-minute bootstrap restart estimate. After a
checkpoint-gated rescale succeeds, the transaction records the time from
applying its frozen target until the replacement execution graph enters
`RUNNING`. Later decisions use the maximum observed restart duration, capped
at ten minutes, to reserve capacity for backlog expected during the next graph
restart. Existing Kafka lag remains a separate, directly measured catch-up
term with a 30-minute drain target; the same backlog is never added manually.

## Render and inspect

Run from the repository root on c165:

```bash
export KUBECONFIG=/etc/flink-kubernetes-autoscaling/kubeconfig
export POLICY=justin  # or ds2
export RUN_ID=20260810-q20-justin-01

source experiments/1729-checkpoint-aware-rescaling/run-env.sh
experiments/1729-checkpoint-aware-rescaling/render-job.sh

kubectl apply --dry-run=server \
  -f "${RENDERED_MANIFEST}"
```

`RUN_ID` is mandatory, must be reused in every terminal for the run, and must
contain only ASCII letters, digits, dots, underscores, and hyphens. Use a new
value for every independent experiment. The renderer substitutes it into the
job name and these NFS paths:

```text
/mnt/experiments/autoscaling-experiments/flink-state/runs/${RUN_ID}/checkpoints
/mnt/experiments/autoscaling-experiments/flink-state/runs/${RUN_ID}/savepoints
/mnt/experiments/autoscaling-experiments/flink-state/runs/${RUN_ID}/ha
```

Render and archive each policy manifest before comparing the runs. Their
common checkpoint, storage, workload, and restart-tracking settings come from
one JSON patch; their original autoscaling policy configuration stays
policy-specific. Use distinct IDs such as `20260810-q20-justin-01` and
`20260810-q20-ds2-01` for a matched pair.

Justin freezes and applies parallelism plus resource-profile overrides. DS2
uses the same transaction and recovery checks but freezes and applies only
parallelism overrides; checkpoint mode does not change either policy's
decision calculation.

The custom checkpoint runtime and operator images must be built and pushed
before the first cluster run. Do not reuse the shared `justin-modified` tag:

```bash
export POLICY=justin
export RUN_ID=20260810-q20-justin-01
source experiments/1729-checkpoint-aware-rescaling/run-env.sh

images/flink-runtime/build.sh \
  --source-dir sources/flink/flink-1-18-src-from-justin \
  --tag "${FLINK_RUNTIME_IMAGE_TAG}"
images/flink-benchmark-runtime/build.sh \
  --runtime-image "${FLINK_RUNTIME_LOCAL}" \
  --tag "${FLINK_BENCHMARK_IMAGE_TAG}"
images/flink-kubernetes-operator/build.sh \
  --source-dir sources/operators/flink-kubernetes-operator-justin \
  --tag "${OPERATOR_IMAGE_TAG}"

scripts/images/push-images.sh all
scripts/autoscaling/cluster-management/05-prepull-images.sh --target-host c153
scripts/autoscaling/cluster-management/04-deploy-operator.sh
```

## Run

Use separate terminals on c165. Every terminal starts with:

```bash
cd ~/flink-kubernetes-autoscaling
export POLICY=justin  # or ds2; keep the same value in every terminal
export RUN_ID=20260810-q20-justin-01  # use the exact same ID in every terminal
source experiments/1729-checkpoint-aware-rescaling/run-env.sh
```

In terminal 1, reset Kafka once, deploy the consumer, and wait for `RUNNING`.
After terminal 3 starts the port-forwards, replace the watch with the checkpoint
transaction observer:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-external-kafka.sh reset
experiments/1729-checkpoint-aware-rescaling/render-job.sh
kubectl apply --dry-run=server -f "${RENDERED_MANIFEST}"
kubectl apply -f "${RENDERED_MANIFEST}"
watch -n 2 'kubectl get flinkdeployment flink; kubectl get pods -l app=flink -o wide'

# Press Ctrl-C after the job is RUNNING and terminal 3 has started forwarding.
experiments/1729-checkpoint-aware-rescaling/observe-checkpoint-rescale.py
```

In terminal 2, start the bounded producer exactly once:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh start
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh logs
```

In terminal 3, start the shared background port-forwards and display live
Flink/Prometheus metrics:

```bash
scripts/autoscaling/job-monitoring/port-forward.sh start
scripts/autoscaling/job-monitoring/port-forward.sh status
scripts/autoscaling/job-monitoring/observe-flink-metrics.py \
  --total-events "${EVENTS}" \
  --source-event-share "${SOURCE_EVENT_SHARE}"
```

After a successful rescale, `restart_ms` is the measured apply-to-RUNNING
duration that will provide restart headroom for later decisions.

In terminal 4, observe autoscaler decisions:

```bash
scripts/autoscaling/job-monitoring/observe-scaling.py \
  --configmap autoscaler-flink \
  --follow
```

Do not run `scaling-kafka-coordinator.py`: that script stops the producer,
deletes Kafka topics, and replays from event 1 after a rescale. In this pilot,
the producer and Kafka must remain live so restored Kafka offsets and backlog
catch-up can be verified.

Inspect or manually control a failed transaction with:

```bash
experiments/1729-checkpoint-aware-rescaling/manage-transaction.sh status
experiments/1729-checkpoint-aware-rescaling/manage-transaction.sh retry
experiments/1729-checkpoint-aware-rescaling/manage-transaction.sh abort
```

`abort` is accepted only before target overrides are applied. After that
point, preserve the failed job for debugging and use `retry` after repairing
the cause.

Capture the transaction, checkpoints, Kubernetes state, and logs while the
REST port-forward is still running:

```bash
experiments/1724-kafka-q20-unique/analysis/export-experiment-data.py \
  --name "${RUN_ID}"
experiments/1729-checkpoint-aware-rescaling/collect-evidence.sh
```

The existing exporter retains the Prometheus throughput, Kafka lag, busy,
resource, and policy decision timelines; `collect-evidence.sh` adds the
rendered manifest, run/storage identity, checkpoint transaction, and raw
reconciliation evidence under `results/${RUN_ID}`.

## Stop

Stop the producer, delete the FlinkDeployment, and then stop Kafka. Do not run
the replay coordinator during this pilot.

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh stop
scripts/autoscaling/job-management/stop-job.sh flink
experiments/1724-kafka-q20-unique/external-kafka/run/manage-external-kafka.sh stop
```

Stopping or deleting the FlinkDeployment does not automatically delete the
run directory because checkpoints are externalized with
`RETAIN_ON_CANCELLATION`. Never delete `chk-*`, `_metadata`, or `shared/`
individually. After evidence is archived and no deployment can restore this
run, the entire `runs/${RUN_ID}` directory may be removed as one unit; cleanup
is intentionally manual and is not performed by these scripts.
