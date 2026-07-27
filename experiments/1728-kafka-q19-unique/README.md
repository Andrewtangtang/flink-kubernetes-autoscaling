# Q19 DS2 and Justin re-evaluation

This experiment runs the Nexmark `q19_unique` SQL query at the Figure 6 target
of 55K bid events/s. Q19 partitions bids by auction, orders each auction by
price, and keeps its top 10 bids in a stateful Rank operator.

Justin and DS2 each have a standalone `experiment.yaml`; edit the selected
policy file directly instead of changing another query's base manifest.

## Settings

- events: 100M mixed Nexmark events;
- producer: 59,783 events/s, yielding approximately 55K bid events/s at the
  configured 92% bid share;
- parallelism: bid source P1 fixed, Rank initially P1;
- maximum parallelism: 18 for both the pipeline and autoscaled vertex;
- TaskManager: 4 CPU, 2 GiB, 4 slots;
- Justin: max memory level 4 and managed-memory fraction 0.4;
- DS2: managed-memory fraction 0.4.

The non-composite maximum parallelism is intentional. It reproduces the Q19
setup artifact discussed in Section 6.3: P4 is not a legal aligned choice, so
the autoscaler can move from P3 to P6 even when P4 would have been sufficient.

## Verify the job graph

The manifests expect the bid source vertex to be
`cbc357ccb763df2852fee8c4fc7d55f2` and the Rank vertex to be
`90bea66de1c231edf33913ecd54406c1`. These IDs are inferred from the equivalent
two-vertex Q18 graph and must be verified before starting the producer.

Deploy a policy without starting Kafka input:

```bash
cd ~/flink-kubernetes-autoscaling
source experiments/1728-kafka-q19-unique/run-env.sh
export POLICY=justin  # or ds2
kubectl apply -k "experiments/1728-kafka-q19-unique/jobs/${POLICY}"
kubectl get flinkdeployment flink -w
```

After the JobManager is running:

```bash
scripts/autoscaling/job-monitoring/port-forward.sh start
JOB_ID=$(curl -s http://localhost:8081/jobs/overview | jq -r '.jobs[0].jid')
curl -s "http://localhost:8081/jobs/${JOB_ID}/plan" |
  jq '.plan.nodes[] | {id, parallelism, description}'
```

The plan must contain exactly one bid source at P1 and one Rank/sink vertex at
P1 with the IDs above. If either ID differs, stop the deployment and update
both policy manifests before producing data.

## Run

Reset Kafka before each policy run:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-external-kafka.sh reset
```

Monitor task and policy metrics in separate terminals:

```bash
scripts/autoscaling/job-monitoring/observe-flink-metrics.py \
  --interval 5 --rate-window 30s --cpu-rate-window 2m \
  --task-regex 'Rank'
```

```bash
scripts/autoscaling/job-monitoring/observe-scaling.py \
  --configmap autoscaler-flink --follow
```

Start the replay coordinator after verifying the graph:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/scaling-kafka-coordinator.py \
  --tps "${TPS}" --events "${EVENTS}" \
  --parallelism "${PARALLELISM}" \
  --max-emit-speed "${MAX_EMIT_SPEED}" \
  --producer-rest-port "${PRODUCER_REST_PORT}"
```

Stop the coordinator with Ctrl-C, then clean up:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh stop || true
scripts/autoscaling/job-management/stop-job.sh flink || true
```
