# Checkpoint-Aware Justin and DS2 Evaluation

For the current five-terminal operator runbook, see
[`instruction.md`](instruction.md).

This experiment preserves the standalone producer container and Kafka topics
while every Justin or DS2 decision is gated by a fresh completed Flink
checkpoint. A transaction-aware controller pauses generation before the
checkpoint and resumes the same process after restore. It is separate from the
paper-reproduction coordinator, which resets Kafka and replays bounded input
after each stateless rescale.

Q4, Q9, Q18, Q19, and Q20 each have complete, independent Justin and DS2
manifests under `jobs/<query>/<policy>/experiment.yaml`. They are materialized
from the normalized `benchmark` configurations and retain each query's fixed
source parallelism and input rate. All ten manifests use the matched 8-CPU,
3-GiB, eight-slot TaskManager profile, 1264-MiB managed-memory pool, vertex cap
12, and pipeline maximum 360. They do not reference the benchmark experiment
directories through Kustomize overlays.

| Query | Producer rate | Fixed Kafka sources |
|---|---:|---|
| Q4 | 40,000 events/s | bid P6, auction P3 |
| Q9 | 40,000 events/s | bid P6, auction P3 |
| Q18 | 97,827 events/s | bid P3 |
| Q19 | 59,783 events/s | bid P1 |
| Q20 | 60,000 events/s | bid P12, auction P1 |

Each run has a 300-million mixed Nexmark event horizon. This keeps the bounded
producer live through repeated checkpoint/rescale cycles and the final
stability observation. The combined observer remains the primary termination
condition, so a run may stop after it reports the experiment end condition;
it does not need to emit all 300 million events. Downstream operators start at
the parallelism recorded in that query's manifest and are controlled by the
selected policy.

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
export QUERY=q20     # q4, q9, q18, q19, or q20
export POLICY=justin  # or ds2
export RUN_ID=20260811-q20-justin-integrated-01

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

Render and archive each query/policy manifest before comparing the runs. Each
manifest contains its own checkpoint, storage, workload, resource, source,
and policy settings. Use distinct IDs such as
`20260811-q20-justin-integrated-01` and
`20260811-q20-ds2-integrated-01` for a matched pair.

Justin freezes and applies parallelism plus resource-profile overrides. DS2
uses the same transaction and recovery checks but freezes and applies only
parallelism overrides; checkpoint mode does not change either policy's
decision calculation.

The custom checkpoint runtime and operator images must be built and pushed
before the first cluster run. Do not reuse the shared `justin-modified` tag:

```bash
export POLICY=justin
export QUERY=q20
export RUN_ID=20260811-q20-justin-integrated-01
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
export QUERY=q20      # keep the same query in every terminal
export RUN_ID=20260811-q20-justin-integrated-01  # exact same ID in every terminal
unset EVENTS
source experiments/1729-checkpoint-aware-rescaling/run-env.sh
```

`POLICY` selects the scaler embedded in the rendered manifest. The Kubernetes
Operator runs it automatically; do not start a separate scaler process.

In terminal 1, reset Kafka once, deploy the consumer, and wait for `RUNNING`:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-external-kafka.sh reset
experiments/1729-checkpoint-aware-rescaling/render-job.sh
kubectl apply --dry-run=server -f "${RENDERED_MANIFEST}"
kubectl apply -f "${RENDERED_MANIFEST}"
kubectl get flinkdeployment flink -w

# Press Ctrl-C after the job is RUNNING and terminal 2 is observing it.
```

In terminal 2, start the shared background port-forwards and the combined
benchmark observer:

```bash
scripts/autoscaling/job-monitoring/port-forward.sh start
scripts/autoscaling/job-monitoring/port-forward.sh status
scripts/autoscaling/job-monitoring/observe-benchmark.py \
  --interval 5 \
  --rate-window 30s \
  --cpu-rate-window 2m
```

Once terminal 2 is observing the job, return to terminal 1 and start the
bounded producer exactly once:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh start
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh logs
```

After a successful rescale, `restart_ms` is the measured apply-to-RUNNING
duration that will provide restart headroom for later decisions.
The observer starts a one-minute post-restore stabilization timer and then
requires three passing two-minute windows before reporting capacity stability.
It distinguishes a falling backlog, a flat positive backlog, and a near-zero
flat backlog instead of treating all above-target consumer throughput as steady.
After three passing windows it latches an `EXPERIMENT END CONDITION REACHED`
banner so a later bounded-producer exit cannot overwrite an already accepted
run. Pass `--exit-when-stable` for a successful automatic observer exit.

Do not run `scaling-kafka-coordinator.py`: that script stops the producer,
deletes Kafka topics, and replays from event 1 after a rescale. Run
`checkpoint-producer-controller.py --interval 1` instead. It freezes the
producer container during checkpoint/apply/restore without resetting Kafka or
the producer's progress.

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

The shared exporter retains the Prometheus throughput, Kafka lag, busy,
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
run, inspect and remove the entire `runs/${RUN_ID}` directory with
`cleanup-run-state.sh`; it requires an exact run-ID confirmation and refuses
to operate while any FlinkDeployment or Flink pod exists.
