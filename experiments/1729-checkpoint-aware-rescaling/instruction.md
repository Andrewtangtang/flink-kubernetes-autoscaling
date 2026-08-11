# Checkpoint-Aware Q20 Run Instructions

This run uses at most five terminals on c165. Unlike the historical replay
experiment, it keeps Kafka and the producer running across each rescale and
restores Flink state and Kafka offsets from a completed checkpoint.

Do **not** run `scaling-kafka-coordinator.py` in this experiment.

## Shared environment

Open every terminal with agent forwarding and use the same policy and run ID:

```bash
ssh -A c165
cd ~/flink-kubernetes-autoscaling

export POLICY=justin
export RUN_ID=20260811-q20-justin-02

source experiments/1729-checkpoint-aware-rescaling/run-env.sh
```

`RUN_ID` must be new for every independent run. Do not reuse an ID whose NFS
state directory may already exist. Use `POLICY=ds2` and a different run ID for
the matched DS2 run.

## Terminal 1: clean, deploy, then start the producer

Pull the latest host-side scripts:

```bash
git pull --ff-only

export POLICY=justin
export RUN_ID=20260811-q20-justin-02
source experiments/1729-checkpoint-aware-rescaling/run-env.sh
```

Stop the previous producer, forwards, and Flink job. This does not reset Kafka
or delete retained NFS checkpoint state:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh stop || true
scripts/autoscaling/job-monitoring/port-forward.sh stop || true
scripts/autoscaling/job-management/stop-job.sh flink || true

kubectl wait --for=delete flinkdeployment/flink --timeout=180s 2>/dev/null || true
kubectl get flinkdeployments
kubectl get pods -l app=flink
```

After the old deployment and pods disappear, reset Kafka once, render the
run-specific manifest, validate it, and deploy it:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-external-kafka.sh reset

experiments/1729-checkpoint-aware-rescaling/render-job.sh

kubectl apply --dry-run=server -f "${RENDERED_MANIFEST}"
kubectl apply -f "${RENDERED_MANIFEST}"

kubectl get flinkdeployment flink -w
```

Wait for `JOB STATUS=RUNNING` and `LIFECYCLE STATE=STABLE`, then press `Ctrl-C`.
Do not start the producer until terminals 3, 4, and 5 are observing the job.

Once those terminals are ready, start the bounded producer exactly once:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh start
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh logs
```

## Terminal 2: observe the deployment and pods

```bash
watch -n 5 \
  'kubectl get flinkdeployment flink; kubectl get pods -l app=flink -o wide'
```

During checkpoint-gated rescaling, the deployment should remain present while
the execution graph changes and TaskManager resources are adjusted.

## Terminal 3: live Flink and Prometheus metrics

c165 port 8081 is used by another user's Java process, so use local port 18082
for Flink. Use 19091 for Prometheus to avoid stale shared forwards.

Start both forwards in the background:

```bash
mkdir -p cluster/.runtime

kubectl port-forward --address 127.0.0.1 \
  svc/flink-rest 18082:8081 \
  > cluster/.runtime/flink-18082-port-forward.log 2>&1 &

kubectl port-forward --address 127.0.0.1 \
  -n manager svc/prom-kube-prometheus-stack-prometheus 19091:9090 \
  > cluster/.runtime/prometheus-19091-port-forward.log 2>&1 &

sleep 2

curl -s http://localhost:18082/jobs/overview |
  jq '.jobs[] | {name, state}'
curl -sf http://localhost:19091/-/ready
```

The first command must show the current Q20 job. Then start the live monitor:

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

This terminal shows source throughput, replay progress, TaskManager CPU and
memory, and per-subtask busy and record rates.

## Terminal 4: observe checkpoint-rescale transactions

```bash
experiments/1729-checkpoint-aware-rescaling/observe-checkpoint-rescale.py \
  --url http://localhost:18082 \
  --interval 5
```

This terminal shows the durable transaction phase, checkpoint ID, frozen
target, restore verification, restart duration, and any latched failure.

## Terminal 5: observe autoscaler decisions

```bash
scripts/autoscaling/job-monitoring/observe-scaling.py \
  --configmap autoscaler-flink \
  --follow
```

For this run it must print `Algorithm: justin`. Start the producer in terminal
1 only after this observer and terminals 3 and 4 are ready.

## Expected behavior during a rescale

```text
Justin decision
  -> fresh checkpoint triggered and completed
  -> target parallelism/memory applied
  -> execution graph rebuilt from checkpoint state
  -> Kafka sources resume from checkpointed offsets
  -> accumulated Kafka backlog is drained
```

Kafka topics and the producer must remain live throughout this sequence.

## Save evidence before stopping

Keep terminal 3 and both port-forwards alive while collecting evidence:

```bash
experiments/1724-kafka-q20-unique/analysis/export-experiment-data.py \
  --name "${RUN_ID}" \
  --flink-url http://localhost:18082 \
  --prom-url http://localhost:19091

FLINK_URL=http://localhost:18082 \
  experiments/1729-checkpoint-aware-rescaling/collect-evidence.sh
```

## Stop the run

After evidence has been saved:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh stop
scripts/autoscaling/job-management/stop-job.sh flink
experiments/1724-kafka-q20-unique/external-kafka/run/manage-external-kafka.sh stop

pkill -f 'kubectl port-forward.*flink-rest.*18082:8081' || true
pkill -f 'kubectl port-forward.*prom-kube-prometheus-stack-prometheus.*19091:9090' || true
```

Retained external checkpoint data under `${RUN_STORAGE_ROOT}` is not deleted by
this stop sequence. Remove a complete run directory only after its evidence is
archived and no deployment can restore from it.
