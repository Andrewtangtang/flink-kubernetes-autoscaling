# Q4 DS2 and Justin Re-evaluation

This experiment runs the state-heavy `q4_unique` Flink SQL query at the
paper's 40K events/s target. Justin and DS2 use one common manifest base; the
two Kustomize overlays change only the policy flag and job name.

## Reconstructed configuration

The repository did not contain a Q4 deployment manifest. A temporary,
autoscaling-disabled graph probe on 2026-07-24 identified these stable job
vertices:

| Vertex | Operator | Initial parallelism | Autoscaled |
|---|---|---:|:---:|
| `cbc357ccb7...` | auction Kafka source | 12 | no |
| `6cdc5bb954...` | bid Kafka source | 1 | no |
| `8b481b930a...` | interval inner join | 1 | yes |
| `d1c51a4d3e...` | maximum bid per auction | 1 | yes |
| `0091c84691...` | average winning bid per category + sink | 1 | yes |

The prepared manifests use `pipeline.max-parallelism=18`. Section 6.3
explicitly identifies 18 as Q19's cap, while Section 4.6 refers only to "our
cap" without stating whether every state-heavy query used the same value.
Using 18 for Q4 is therefore a reconstruction assumption, not a value stated
explicitly for Q4.

The two sources currently reuse the fixed Q20 Kafka-source settings because
the `_unique` queries share the same Kafka DDL and physical source IDs. The
paper does not state Q4's source parallelisms, so auction P12 and bid P1 are
also reconstruction assumptions.

These assumptions are accepted for the local DS2-versus-Justin evaluation.
Both policies use exactly the same reconstructed graph and limits, which makes
the local pair internally comparable. Results must still be labeled as a
reconstructed evaluation rather than an exact reproduction of the author's
unpublished Q4 configuration.

The paper specifies the 40K events/s target but not a total event count.
`run-env.sh` uses a controlled 100M-event horizon, matching the completed Q20
reproduction and providing more than 40 minutes of input after the final
restart. Use the same event count for the paired Justin and DS2 runs.

## Prerequisites

Run from `~/flink-kubernetes-autoscaling` on c165 with a live forwarded SSH
agent:

```bash
echo "${SSH_AUTH_SOCK}"
ssh-add -l
ssh c153 hostname
```

Before producing 100M events, verify that c153 has at least 45 GiB free:

```bash
ssh c153 'df -h / /opt/flink-kubernetes-autoscaling'
```

Do not start the producer if this check fails. The last preflight on
2026-07-24 showed only 19 GiB free and 166.7 GiB of reclaimable Docker build
cache. Reclaiming shared-host Docker data is a separate, explicitly approved
operation.

## Render and compare policies

```bash
kubectl kustomize experiments/1725-kafka-q4-unique/jobs/justin > /tmp/q4-justin.yaml
kubectl kustomize experiments/1725-kafka-q4-unique/jobs/ds2 > /tmp/q4-ds2.yaml

diff -u /tmp/q4-ds2.yaml /tmp/q4-justin.yaml
```

The rendered diff must contain only the Justin enable flag and job name.

## Start one run

Choose exactly one policy:

```bash
export POLICY=justin
# export POLICY=ds2
```

Load the common environment in every terminal:

```bash
cd ~/flink-kubernetes-autoscaling
source experiments/1725-kafka-q4-unique/run-env.sh
```

Reset Kafka and submit the selected consumer:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-external-kafka.sh reset
kubectl apply -k "experiments/1725-kafka-q4-unique/jobs/${POLICY}"
kubectl get flinkdeployment flink -w
```

Start the shared port forwards and live Prometheus monitor in a monitoring
terminal **before** starting the coordinator:

```bash
cd ~/flink-kubernetes-autoscaling
source experiments/1725-kafka-q4-unique/run-env.sh
scripts/autoscaling/job-monitoring/port-forward.sh start
scripts/autoscaling/job-monitoring/observe-flink-metrics.py \
  --interval 5 \
  --rate-window 30s
```

Starting the shared forward first avoids interrupting the coordinator's Flink
REST connection. The coordinator detects the healthy localhost port 8081 and
reuses it.

Start the coordinator in a second terminal:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/scaling-kafka-coordinator.py \
  --tps "${TPS}" \
  --events "${EVENTS}" \
  --parallelism "${PARALLELISM}" \
  --max-emit-speed "${MAX_EMIT_SPEED}" \
  --producer-rest-port "${PRODUCER_REST_PORT}"
```

Observe policy decisions in a third terminal:

```bash
scripts/autoscaling/job-monitoring/observe-scaling.py \
  --configmap autoscaler-flink \
  --follow
```

The upper table summarizes each TaskManager pod's node, CPU, memory, busy time,
and task traffic. The lower table shows placement, busy time, and records/s for
every subtask. Use `--task-regex 'Join|GroupAggregate'` to focus on Q4's
policy-controlled operators. This live display complements the third
terminal's five-minute policy snapshots.

After the coordinator reports the tracked Q4 job, start the initial producer
from the first terminal:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh start
```

## Stop one run

Stop the coordinator with `Ctrl-C`, then run:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh stop || true
scripts/autoscaling/job-management/stop-job.sh flink || true
```

Keep Kafka running between the paired Justin and DS2 runs, but reset its
topics before the second run. Stop it after both policies finish:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-external-kafka.sh stop || true
```
