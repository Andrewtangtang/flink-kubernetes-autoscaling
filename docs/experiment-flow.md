# Experiment Flow

The retained experiment is the q20_unique SQL benchmark under
`experiments/1724-kafka-q20-unique`.

At a high level, the experiment separates data generation from the autoscaled
Flink job:

1. A Kafka broker runs outside the Kubernetes cluster, currently on `c153`.
2. A standalone Flink producer job also runs on `c153`. It runs the
   `insert_kafka_unique` query and writes Nexmark events into Kafka.
3. The in-cluster Flink consumer job is submitted as a `FlinkDeployment`. It
   runs the `q20_unique` query and reads from Kafka through
   `kafka-external:9092`.
4. The Flink Kubernetes Operator watches that `FlinkDeployment`. Its autoscaler
   decides when to rescale the consumer job, and the operator applies those
   changes by updating the Flink deployment, restarting the job when needed, and
   creating or removing TaskManager pods.

The producer is not managed by the Flink Kubernetes Operator. It is kept outside
the cluster so the experiment can control the input stream independently from
the consumer job being autoscaled.

## Repository Layout

- `jobs/`: q20_unique consumer `FlinkDeployment` YAMLs for DS2 and Justin.
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
standalone Flink producer on c153
  -> external Kafka on c153
  -> kafka-external:9092 inside Kubernetes
  -> q20_unique FlinkDeployment consumer job
```

The producer uses the benchmark runtime image and runs Flink in local mode on
the external host. The consumer uses the same benchmark runtime image inside
Kubernetes.

## Autoscaling Path

The consumer job is the autoscaling target. Submit either the Justin or DS2
consumer manifest from `experiments/1724-kafka-q20-unique/jobs/`:

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
  --tps 50000 \
  --events 300000000
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
