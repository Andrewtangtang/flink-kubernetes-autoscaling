# q20_unique Kafka Experiment

This experiment runs `q20_unique` as the autoscaled Flink consumer job. The
input stream is produced outside Kubernetes:

```text
standalone Flink producer on c153
  -> external Kafka on c153
  -> kafka-external:9092 inside Kubernetes
  -> q20_unique FlinkDeployment consumer job
```

The consumer job is submitted to the Flink Kubernetes Operator. The producer is
a separate local-mode Flink job on the external Kafka host, so the input stream
can be stopped, restarted, and replayed independently of the autoscaled
consumer.

Run commands from the repository root:

```bash
cd /flink-kubernetes-autoscaling
```

## One-Time External Kafka Setup

Run the setup scripts once on a new producer host, or again when the host,
registry image, or `kafka-external` service target changes.

```bash
export TARGET_HOST=c153
export HOST_TAG=c153

experiments/1724-kafka-q20-unique/external-kafka/setup/01-prepare-generator-host.sh
experiments/1724-kafka-q20-unique/external-kafka/setup/02-apply-kafka-service.sh apply
```

The prepare script verifies SSH access, `sudo -n docker`, Docker storage and
registry settings, and the Kafka and benchmark runtime images. It does not
start Kafka or the producer job.

The service script creates the in-cluster `kafka-external:9092` alias used by
the consumer job YAMLs.

## Shared Run Configuration

Keep the producer start command and the scaling coordinator on the same
configuration. The coordinator restarts the producer after every detected
rescale, so mismatched values change the workload partway through a run.

```bash
export TARGET_HOST=c153
export HOST_TAG=c153
export PRODUCER_REST_PORT=18081

export PARALLELISM=4
export SLOTS=4
export TM_CORES=16
export DOCKER_CPUS=16
export JM_PROCESS_MEMORY=2048m
export TM_PROCESS_MEMORY=8192m

export TPS=60000
export EVENTS=100000000
export MAX_EMIT_SPEED=false
```

`PARALLELISM`, `TPS`, `EVENTS`, `MAX_EMIT_SPEED`, and `PRODUCER_REST_PORT` are
also passed to the coordinator as CLI arguments. `SLOTS`, `TM_CORES`,
`DOCKER_CPUS`, and the memory settings are environment variables; export them
before starting both the producer and coordinator so coordinator-triggered
producer restarts inherit the same container and Flink runtime sizing.

`SOURCE_EVENT_SHARE=0.98` records that Q20 consumes the auction and bid topics,
which together contain 98% of the default Nexmark event mix. The live Flink
metrics monitor uses this value with `EVENTS` to display an approximate global
event position and replay completion percentage.

For the Figure 6 reconstruction, retain the author-provided policy-specific
pipeline caps: Justin uses `pipeline.max-parallelism=12`, while DS2 uses 20.
Both jobs fix the bid source at P1 and the auction source at P12, exclude both
sources from autoscaling, and start the stateful Join at P1. The policy-specific
caps are part of the original artifact configuration rather than a fair-policy
normalization.

## Start A Run

Pre-pull the Kubernetes job image on the cluster nodes and the Kafka/producer
images on the external host. This keeps image transfer work out of the job and
producer startup path.

```bash
scripts/autoscaling/cluster-management/05-prepull-images.sh --target-host "${TARGET_HOST}"
```

Reset external Kafka. This starts the broker if needed and recreates the
three q20_unique Kafka topics with empty state:
`nexmark-person`, `nexmark-auction`, and `nexmark-bid`.

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-external-kafka.sh reset
```

Submit one consumer job to the operator. Use the DS2 manifest:

```bash
scripts/autoscaling/job-management/submit-job.sh \
  experiments/1724-kafka-q20-unique/jobs/q20_unique-sql-ssd-kafka-ds2-rocksdb-options.yaml
```

Or use the Justin autoscaler manifest:

```bash
scripts/autoscaling/job-management/submit-job.sh \
  experiments/1724-kafka-q20-unique/jobs/q20_unique-sql-ssd-kafka-justin-rocksdb-options.yaml
```

Wait for the consumer to be running before starting the producer:

```bash
scripts/autoscaling/job-management/job-status.sh
kubectl get flinkdeployment flink -w
```

Look for the `flink` deployment to report a running job before sending data
into Kafka.

Start the coordinator in a second terminal with the same run configuration
exported:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/scaling-kafka-coordinator.py \
  --tps "${TPS}" \
  --events "${EVENTS}" \
  --parallelism "${PARALLELISM}" \
  --max-emit-speed "${MAX_EMIT_SPEED}" \
  --producer-rest-port "${PRODUCER_REST_PORT}"
```

After the coordinator prints `Tracking job`, start the standalone producer
with the shared run configuration:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh start
```

The coordinator watches the consumer job through the Flink REST API. When it
detects a rescale, it pauses and stops the producer, resets Kafka topics, waits
for the consumer to return to `RUNNING`, and restarts the producer from
`first-event-id=1` with the same workload settings.

The coordinator manages producer restarts after scaling. It does not start the
initial producer run, so start the producer once manually after it has attached
to the intended Flink job.

## Useful Checks

Check the external Kafka and producer containers:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-external-kafka.sh status
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh status
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh logs
```

Watch the in-cluster consumer and autoscaler:

```bash
scripts/autoscaling/job-management/job-status.sh
scripts/autoscaling/job-monitoring/observe-scaling.py --follow
scripts/autoscaling/status.sh
```

The producer REST UI is on the external host at `http://c153:18081` when
`PRODUCER_REST_PORT=18081`.

## Stop A Run

Stop the coordinator with `Ctrl-C`, then stop the producer and consumer:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh stop || true
scripts/autoscaling/job-management/stop-job.sh flink || true
```

Stop Kafka only when you are done with the external broker:

```bash
experiments/1724-kafka-q20-unique/external-kafka/run/manage-external-kafka.sh stop || true
```

Export metrics and logs after a completed run:

```bash
experiments/1724-kafka-q20-unique/analysis/export-experiment-data.py
```
