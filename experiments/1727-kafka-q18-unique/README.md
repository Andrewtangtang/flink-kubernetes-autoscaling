# Q18 DS2 and Justin re-evaluation

This experiment runs the Nexmark `q18_unique` SQL query at the Figure 6 target
of 90K input events/s. Q18 reads the bid Kafka topic, keeps the latest bid per
`(bidder, auction)` with a `ROW_NUMBER` deduplication, and writes to the
blackhole sink.

The two policy overlays use the same reconstructed job graph and resources;
only the Justin policy flag and job name differ. The base intentionally reuses
the Q9 infrastructure manifest so checkpoint, RocksDB, node placement, and
Prometheus settings remain identical across the state-heavy queries.

## Reconstruction assumptions

The paper does not publish Q18's source parallelism, vertex IDs, or total event
horizon. This manifest follows the existing Figure 6 reconstruction:

- 100M generated Nexmark events;
- `pipeline.max-parallelism=18`;
- bid Kafka source fixed at P1 and excluded from autoscaling;
- the first stateful Q18 vertex fixed initially at P1 and autoscaled thereafter;
- bid share `0.92`, from the benchmark generator configuration.

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

The policy diff should contain only the Justin flag and job name.

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

In another terminal, follow policy snapshots:

```bash
scripts/autoscaling/job-monitoring/observe-scaling.py \
  --configmap autoscaler-flink --follow
```

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
