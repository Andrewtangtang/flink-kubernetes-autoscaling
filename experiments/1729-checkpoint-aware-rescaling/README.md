# Q20 checkpoint-aware rescaling pilot

This example runs Q20 with Justin or DS2, fixed Kafka sources (bid P12,
auction P1), and a fresh checkpoint before rescaling. A controller pauses the
Docker producer before that checkpoint and resumes the same container after
the restored job is running. Kafka is never reset during a run. The 24-hour
periodic checkpoint interval keeps background checkpoints out of this pilot.

Build the Operator from the pinned submodule with
`OPERATOR_SOURCE_DIR=sources/operators/flink-kubernetes-operator-justin`;
the repository default selects A4S.

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

When the job is `RUNNING`, prepare a separate Q20 `insert_kafka_unique`
producer for 60,000 mixed events/s and 300 million events, with
`max-emit-speed=false` and `first-event-id=1`. The controller needs `kubectl`
access to the FlinkDeployment/ConfigMap and Docker access to that producer
container. Configure Docker access locally or with `DOCKER_HOST`. Start the
controller before starting producer input, and keep it running until the
transaction completes:

```bash
python3 experiments/1729-checkpoint-aware-rescaling/checkpoint-producer-controller.py \
  --namespace "$NAMESPACE" --deployment flink --container insert-kafka \
  --interval 1 | tee "$RUN_DIR/producer-controller.log"
```

Replace `insert-kafka` with the actual container name. The controller uses
Docker `pause`/`unpause`, verifies the container state before acknowledging
the Operator, and records a pause intent on the FlinkDeployment. If pause
succeeds but its acknowledgement fails, restarting the controller retries
safely; if that transaction is aborted, it resumes the container even without
a pause acknowledgement. A `FAILED` transaction is deliberately fail-closed:
the controller does not resume the producer; inspect and retry/abort before
manual intervention. Without a running controller, the Operator waits for an
ACK and eventually fails the transaction on the configured timeout. Docker
pause can disrupt producer network connections during a long restore, so
verify the producer remains healthy and event output advances after resume.
Then start the producer and keep its **container** alive through rescaling;
preserve its log. Use the same producer/controller setup for DS2.

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

Save the rendered manifest, transaction and controller logs, producer log,
Operator/JobManager logs, Flink checkpoint history, and autoscaler ConfigMap
before deleting the job. Verify the transaction reaches `COMPLETED`, the
producer is paused before checkpoint trigger and running again only after
restore, the job returns to `RUNNING`, and source parallelism stays P12/P1.
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
