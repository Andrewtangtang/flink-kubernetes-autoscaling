# Experiment Flow

The benchmark compares Justin and DS2 on five Kafka-backed SQL queries. The
Q20 runbook is at `experiments/1724-kafka-q20-unique/README.md`; the same
producer and coordinator are reused for the other queries.

| Query | Directory | Mixed producer rate | Approximate query input | Fixed Kafka sources |
|---|---|---:|---:|---|
| Q4 | `1725-kafka-q4-unique` | 40,000/s | 39,200/s | bid P6, auction P3 |
| Q9 | `1726-kafka-q9-unique` | 40,000/s | 39,200/s | bid P6, auction P3 |
| Q18 | `1727-kafka-q18-unique` | 97,827/s | 90,000/s | bid P3 |
| Q19 | `1728-kafka-q19-unique` | 59,783/s | 55,000/s | bid P1 |
| Q20 | `1724-kafka-q20-unique` | 60,000/s | 58,800/s | bid P12, auction P1 |

Each run generates 100 million mixed events. Every manifest specifies 8 CPU,
3 GiB memory, eight slots, and 1264 MiB managed memory per TaskManager; a
two-minute metrics window; a three-minute stabilization interval; pipeline
maximum parallelism 360; and autoscaler vertex maximum 12. Source vertices
are excluded from autoscaling. The query input is the portion of the mixed
stream read by that query's Kafka sources, not the producer's full output.
Vertex IDs are query-plan-specific: Q4/Q9 and Q20 use the same two ID values
for opposite source names, so verify each override against that query's job
plan before changing its source parallelism.

Set the query-specific producer rate without duplicating five environment
files:

```bash
export QUERY=q20  # q4, q9, q18, q19, or q20
source experiments/benchmark-run-env.sh
```

Set `TARGET_HOST` to the external Kafka/producer host and `TARGET_IP` if its
name is not resolvable. Set `KUBECONFIG` for the cluster. The manifests use
the repository's lab image, Kafka alias, node labels, and shared storage paths;
adapt those cluster-specific values before running elsewhere.

Before applying a manifest on another cluster, build and publish the benchmark
runtime image, then set `spec.image` to its reachable registry reference.
Provide a `kafka-external:9092` Service/Endpoints alias or change the job's
`--bootstrap-servers` argument. Ensure the JobManager and TaskManagers can all
access the same checkpoint/savepoint directory; the sample `hostPath` assumes
the NFS export is mounted at the path in the manifest on every Flink node.
Change the `tier` node selectors, `flink` service account, and RocksDB SSD
`hostPath` to match the target cluster. The example values are not portable
defaults. Use a Bash shell when sourcing `benchmark-run-env.sh`.

## Run Any Query

Follow the [Q20 runbook](../experiments/1724-kafka-q20-unique/README.md) for
setup, startup, monitoring, and cleanup. For another query, change `QUERY` and
submit the matching directory from the table above. For example, to run Q9
with DS2:

```bash
export QUERY=q9
source experiments/benchmark-run-env.sh
scripts/autoscaling/job-management/submit-job.sh \
  experiments/1726-kafka-q9-unique/jobs/ds2/experiment.yaml
```

Use `jobs/justin/experiment.yaml` for Justin. Run only one policy at a time;
stop the old consumer and reset Kafka before the next run. Keep using the
`1724-kafka-q20-unique/external-kafka/` producer, Kafka, and coordinator
scripts for all five queries: that directory holds the shared tools, not a
Q20-only producer. Source the same `QUERY` configuration in each terminal
that starts the producer or coordinator.

At a high level, the experiment separates data generation from the autoscaled
Flink job:

1. A Kafka broker runs outside the Kubernetes cluster (on `c153` in the lab).
2. A standalone Flink producer job runs on that host. It runs the
   `insert_kafka_unique` query and writes Nexmark events into Kafka.
3. The in-cluster Flink consumer job is submitted as a `FlinkDeployment`. It
   runs the selected query and reads from Kafka through
   `kafka-external:9092`.
4. The Flink Kubernetes Operator watches that `FlinkDeployment`. Its autoscaler
   decides when to rescale the consumer job, and the operator applies those
   changes by updating the Flink deployment, restarting the job when needed, and
   creating or removing TaskManager pods.

The producer is not managed by the Flink Kubernetes Operator. It is kept outside
the cluster so the experiment can control the input stream independently from
the consumer job being autoscaled.

## Repository Layout

- `experiments/<query>/jobs/{ds2,justin}/experiment.yaml`: standalone
  consumer `FlinkDeployment` manifests for each query and policy.
- `external-kafka/setup/`: one-time setup for the external Kafka host and the
  Kubernetes Service/Endpoints alias.
- `external-kafka/run/`: commands for the external Kafka broker, standalone
  Flink producer, and scaling coordinator.
- `analysis/`: scripts for exporting metrics/logs and plotting completed runs.
- `experiment-data/`: generated export output, ignored by Git.

## Data Path

Kafka runs on the external host. The setup script creates a Kubernetes Service
and Endpoints object named `kafka-external`, which points in-cluster consumers
at the external Kafka broker.

```text
standalone Flink producer on external host
  -> external Kafka on the same host
  -> kafka-external:9092 inside Kubernetes
  -> selected query's FlinkDeployment consumer job
```

The producer uses the benchmark runtime image and runs Flink in local mode on
the external host. The consumer uses the same benchmark runtime image inside
Kubernetes.

## Autoscaling Path

The consumer job is the autoscaling target. Submit **one** Justin or DS2
manifest at a time; Q20 is shown here:

```bash
scripts/autoscaling/job-management/submit-job.sh experiments/1724-kafka-q20-unique/jobs/justin/experiment.yaml
scripts/autoscaling/job-management/submit-job.sh experiments/1724-kafka-q20-unique/jobs/ds2/experiment.yaml
```

Both manifests enable the Flink autoscaler. The Justin manifest sets
`job.autoscaler.justin.enabled: 'true'`; the DS2 manifest sets it to `'false'`.

During a run, the operator reads metrics from the running Flink job and records
autoscaler decisions. When a scaling decision is applied, Flink's adaptive
scheduler moves the job through a fast restart/reconfiguration cycle. Kubernetes
then starts or removes TaskManager pods to match the new resource shape.

Use these scripts to watch the job and autoscaler state:

```bash
scripts/autoscaling/job-management/job-status.sh
scripts/autoscaling/job-monitoring/observe-scaling.py --follow
scripts/autoscaling/status.sh
```

Stop the in-cluster consumer job with:

```bash
scripts/autoscaling/job-management/stop-job.sh
```

## External Kafka Sequence

Run the setup scripts once for a producer host. For the current setup that host
is `c153`; override `TARGET_HOST`, `TARGET_IP`, and `HOST_TAG` if you move the
producer to another machine.

```bash
experiments/1724-kafka-q20-unique/external-kafka/setup/01-prepare-generator-host.sh
experiments/1724-kafka-q20-unique/external-kafka/setup/02-apply-kafka-service.sh apply
```

After that one-time setup, normal experiment runs use the scripts under
`external-kafka/run/`. Reset or start Kafka, start or stop the producer, and run
the scaling coordinator from there:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-external-kafka.sh reset
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh start
```

Useful repeated-run commands:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-external-kafka.sh start
experiments/1724-kafka-q20-unique/external-kafka/run/manage-external-kafka.sh stop
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh start
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh stop
```

The scaling coordinator watches the in-cluster consumer job through the Flink
REST API. When it detects a rescale, it pauses the standalone producer, stops
it, resets Kafka topics, waits for the consumer job to return to `RUNNING`, and
then restarts the producer from the beginning of the Nexmark event stream.

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/scaling-kafka-coordinator.py \
  --tps "${TPS}" \
  --events "${EVENTS}" \
  --parallelism "${PARALLELISM}" \
  --max-emit-speed "${MAX_EMIT_SPEED}" \
  --producer-rest-port "${PRODUCER_REST_PORT}"
```

That reset behavior keeps each post-rescale run aligned with an empty Kafka
topic and a producer that starts again from event id `1`.

## Export and Plot Results

After a run, export metrics, logs, and metadata:

```bash
experiments/1724-kafka-q20-unique/analysis/export-experiment-data.py
```

Plot an exported run directory:

```bash
experiments/1724-kafka-q20-unique/analysis/plot-experiment.py experiments/1724-kafka-q20-unique/experiment-data/<run-dir>
```
