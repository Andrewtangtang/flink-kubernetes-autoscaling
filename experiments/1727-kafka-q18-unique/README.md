# Q18 DS2 and Justin re-evaluation

This experiment runs the Nexmark `q18_unique` SQL query at the Figure 6 target
of 90K input events/s. Q18 reads the bid Kafka topic, keeps the latest bid per
`(bidder, auction)` with a `ROW_NUMBER` deduplication, and writes to the
blackhole sink.

The two policy overlays use the same job graph and compute resources.

## Settings

- events: 100M at total producer TPS 97,827 (approximately 90K bid events/s);
- parallelism: source P3 fixed, Deduplicate initially P1, pipeline max 360,
  vertex cap 18;
- TaskManager: 4 CPU, 2 GiB, 4 slots;
- Justin: max memory level 4 and managed-memory fraction 0.8;
- DS2: managed-memory fraction 0.4.

The Q18 probe on c165 identified the bid source as
`cbc357ccb763df2852fee8c4fc7d55f2` and the Deduplicate vertex as
`90bea66de1c231edf33913ecd54406c1`. The source is excluded from autoscaling;
the Deduplicate vertex is initially P1 and remains policy-controlled.

## Render and compare

```bash
kubectl kustomize experiments/1727-kafka-q18-unique/jobs/justin > /tmp/q18-justin.yaml
kubectl kustomize experiments/1727-kafka-q18-unique/jobs/ds2 > /tmp/q18-ds2.yaml
diff -u /tmp/q18-ds2.yaml /tmp/q18-justin.yaml
```

The policy diff should contain only the job name, Justin flag, and Justin
memory settings.

## Run

On c165, source the environment in each terminal:

```bash
cd ~/flink-kubernetes-autoscaling
source experiments/1727-kafka-q18-unique/run-env.sh
```

Reset Kafka and deploy one policy:

```bash
export POLICY=justin  # or ds2
experiments/1724-kafka-q20-unique/external-kafka/run/manage-external-kafka.sh reset
kubectl apply -k "experiments/1727-kafka-q18-unique/jobs/${POLICY}"
kubectl get flinkdeployment flink -w
```

In a monitor terminal, start the shared REST forward and live metrics:

```bash
scripts/autoscaling/job-monitoring/port-forward.sh start
scripts/autoscaling/job-monitoring/observe-flink-metrics.py \
  --interval 5 --rate-window 30s --cpu-rate-window 2m
```

The live monitor labels record-rate values with their Prometheus averaging
window. In another terminal, follow policy snapshots and exact parallelism
calculation inputs:

```bash
scripts/autoscaling/job-monitoring/observe-scaling.py \
  --configmap autoscaler-flink --follow
```

`WinAvgCap`/`WinAvg` is the autoscaler's metrics-window average true processing
capacity. The decision table also reports target data rate, catch-up rate,
target processing capacity, the unrounded estimate
`RawP = CurP * TargetCap / WinAvg`, and the rounded/key-group-aligned
recommendation. For Justin, this is explicitly labeled as the DS2 base
parallelism calculation that Justin may replace with a memory-level decision.

After Flink is RUNNING, start the producer/coordinator:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/scaling-kafka-coordinator.py \
  --tps "${TPS}" --events "${EVENTS}" \
  --parallelism "${PARALLELISM}" \
  --max-emit-speed "${MAX_EMIT_SPEED}" \
  --producer-rest-port "${PRODUCER_REST_PORT}"
```

The coordinator starts and restarts the standalone producer around replay-based
rescaling. Stop it with Ctrl-C, then stop the producer and Flink job:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh stop || true
scripts/autoscaling/job-management/stop-job.sh flink || true
```
