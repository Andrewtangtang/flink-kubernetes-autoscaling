# Q20 checkpoint-aware rescaling pilot

This example runs Q20 with Justin or DS2, fixed Kafka sources (bid P12,
auction P1), and a fresh checkpoint before rescaling. It does not pause the
producer or reset Kafka during a run. The 24-hour periodic checkpoint interval
keeps background checkpoints out of this pilot.

The Justin Operator submodule currently points to commit `0c011f9` in
Andrew's fork. Build the Operator with
`OPERATOR_SOURCE_DIR=sources/operators/flink-kubernetes-operator-justin`;
the repository default selects A4S. Replace this fork gitlink with an
upstream-reachable commit before submitting the PR.

Before running, provide compatible Flink/benchmark/Operator images, the
`flink` service account, and a shared RWX checkpoint PVC. Kafka must be
reachable by both Flink and the producer. Prepare empty `nexmark-person`,
`nexmark-auction`, and `nexmark-bid` topics with at least 12 partitions.
Use a fresh run ID and reset topics **between** Justin and DS2, never during
a run. Do not use the old replay coordinator; it resets Kafka on rescale.

From the repository root, set the run values and render one policy:

```bash
export POLICY=justin
export RUN_ID=q20-justin-01
export NAMESPACE=default
export FLINK_IMAGE=registry.example/flink-benchmark:checkpoint
export KAFKA_BOOTSTRAP=kafka.example:9092
export CHECKPOINT_PVC=flink-state
export RUN_DIR="experiment-data/${RUN_ID}"

python3 experiments/1729-checkpoint-aware-rescaling/render-job.py \
  --policy "$POLICY" --run-id "$RUN_ID" --image "$FLINK_IMAGE" \
  --kafka-bootstrap "$KAFKA_BOOTSTRAP" --checkpoint-pvc "$CHECKPOINT_PVC" \
  --output "$RUN_DIR/manifest.yaml"
kubectl -n "$NAMESPACE" apply --dry-run=server -f "$RUN_DIR/manifest.yaml"
kubectl -n "$NAMESPACE" apply -f "$RUN_DIR/manifest.yaml"
```

When the job is `RUNNING`, start a separate Q20 `insert_kafka_unique`
producer at 60,000 mixed events/s for 300 million events, with
`max-emit-speed=false` and `first-event-id=1`. Keep it running through
rescaling; preserve its log. Use the same producer setup for DS2. The
existing lab producer script is c153-specific and invokes sudo, so it is
not part of this portable example.

Forward Flink REST in a separate shell, then run the observer and save its
output:

```bash
kubectl -n "$NAMESPACE" port-forward svc/flink-rest 18082:8081
```

```bash
python3 experiments/1729-checkpoint-aware-rescaling/observe-checkpoint-rescale.py \
  --namespace "$NAMESPACE" --url http://localhost:18082 --interval 5 \
  | tee "$RUN_DIR/transaction.log"
```

To inspect a transaction without changing it:

```bash
NAMESPACE="$NAMESPACE" \
  experiments/1729-checkpoint-aware-rescaling/manage-transaction.sh status
```

Save the rendered manifest, transaction log, producer log, Operator/JobManager
logs, Flink checkpoint history, and autoscaler ConfigMap before deleting the
job. Verify the transaction reaches `COMPLETED`, the gated checkpoint
completes, the job returns to `RUNNING`, and source parallelism stays P12/P1.
The checkpoint ID in the transaction alone does not prove which checkpoint
Flink restored; check the JobManager restore logs. Checkpoint mode
`EXACTLY_ONCE` does not by itself establish exactly-once sink output.

```bash
kubectl -n "$NAMESPACE" get configmap autoscaler-flink -o yaml \
  > "$RUN_DIR/autoscaler-configmap.yaml"
kubectl -n "$NAMESPACE" delete -f "$RUN_DIR/manifest.yaml"
```

For the matched second run, set `POLICY=ds2` and a new `RUN_ID`, then repeat
the commands after resetting Kafka to the same empty-topic state. Retain the
checkpoint/HA directory until its evidence is no longer needed.
