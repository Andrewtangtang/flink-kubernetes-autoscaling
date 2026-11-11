# flink-kubernetes-autoscaling

This repository collects the source inputs, image builds, cluster setup, and
experiment manifests for the Flink Kubernetes autoscaling benchmark stack.

It is organized around these top-level areas:

- `sources/`: source inputs grouped by role so future forks can live beside the current defaults.
- `images/`: image definitions and per-image build entrypoints.
- `cluster/`: static cluster, monitoring, and operator configuration.
- `scripts/autoscaling/`: operational cluster and job commands.
- `experiments/`: concrete experiment flows and job manifests.

The current source defaults are:

- `sources/flink/flink-1-18-src-from-justin`: vendored Flink 1.18 source used for the base runtime image.
- `sources/benchmarks/nexmark-datastream-benchmarks-from-justin`: vendored original DataStream Nexmark benchmark suite.
- `sources/benchmarks/nexmark-sql-benchmarks`: SQL Nexmark benchmark submodule with the custom Kafka connector submodule.
- `sources/libs/rocksdb-options`: minimal RocksDB options Maven project for the Kubernetes Justin experiments.
- `sources/operators/flink-kubernetes-operator-justin`: vendored Flink Kubernetes Operator with the Justin autoscaling, checkpoint-gated rescaling, and A4S changes.
- `sources/libs/frocksdb`: vendored FRocksDB 6.20.3 (QuickMRC base plus the SHARDS MRC estimator) for the `rocksdbjni` jar.

The image tags are:

```text
flink-runtime:base-1-18-from-justin
flink-benchmark-runtime:sql-three-tables-unique
flink-kubernetes-operator:justin-modified
```

Default registry tags are derived from `scripts/env.sh` and
`cluster/config/env.sh`. Build scripts create local tags only;
`scripts/images/push-images.sh` is the only script that pushes registry tags.

## A4S (SHARDS MRC)

Branch `a4s-shards-mrc` adds the A4S autoscaler to the checkpoint-preserving
DS2/Justin stack. A4S uses an online SHARDS miss-rate curve, with SHARDS_adj.
Other branches do not contain these changes.

Where the A4S changes live:

| Area | Path |
|---|---|
| SHARDS estimator in the block cache (C++ and JNI) | `sources/libs/frocksdb/cache/shards_mrc_estimator.{h,cc}`, `sources/libs/frocksdb/java/src/main/java/org/rocksdb/LRUCache.java` |
| Shared histogram and MRC library | `sources/flink/flink-1-18-src-from-justin/flink-a4s/` |
| RocksDB wiring and metrics | `sources/flink/flink-1-18-src-from-justin/flink-state-backends/flink-statebackend-rocksdb/` |
| `/a4s-metrics` REST endpoint | `sources/flink/flink-1-18-src-from-justin/flink-runtime/` (`A4SAggregatingVertexMetricsHandler`) |
| A4S decision logic in the autoscaler | `sources/operators/flink-kubernetes-operator-justin/flink-autoscaler/` (`a4s/`, `ScalingMetricCollector`, `ScalingMetricEvaluator`, `ScalingExecutor`) |
| Experiment manifests and run settings | `experiments/1729-checkpoint-aware-rescaling/jobs/<query>/a4s/`, `run-env.sh` |

`sources/flink/flink-runtime-1-18-a4s/` and
`sources/operators/flink-kubernetes-operator-a4s/` hold the original A4S port.
They are kept for reference only and are not part of the build.

### Switching policies

DS2, Justin and A4S are built into the same operator and runtime. Each job
picks its policy through its manifest, so switching needs no rebuild.
`POLICY` selects `experiments/1729-checkpoint-aware-rescaling/jobs/<query>/<policy>/experiment.yaml`,
and the three manifests differ only in these settings:

| `POLICY` | `job.autoscaler.justin.enabled` | `job.autoscaler.a4s.enabled` | `state.backend.rocksdb.memory.managed` | Also set |
|---|---|---|---|---|
| `ds2` | `false` | absent | `false` | — |
| `justin` | `true` | absent | `false` | — |
| `a4s` | `true` | `true` | `true` | histogram metric, SHARDS settings, per-slot memory cap |

The operator applies them in order:
- `justin.enabled: false` runs DS2, i.e. the upstream autoscaler.
- `justin.enabled: true` adds Justin's memory policy on top of DS2.
- `a4s.enabled: true` replaces Justin's policy with A4S.
- Only A4S turns on managed memory, so only A4S jobs create the SHARDS
  estimator.

Run one benchmark per policy, with a new `RUN_ID` each time.
`instruction.md` has the full procedure: producer, observers, evidence.

```bash
# From the repository root. Pick the policy and run (q4, q9, q18, q19 or q20; ds2, justin or a4s)
export QUERY=q20 POLICY=ds2 RUN_ID=20261111-q20-ds2-01
unset EVENTS
source experiments/1729-checkpoint-aware-rescaling/run-env.sh   # sets image tags, TPS, RENDERED_MANIFEST

bash experiments/1729-checkpoint-aware-rescaling/render-job.sh
kubectl apply --dry-run=server -f "${RENDERED_MANIFEST}"
kubectl apply -f "${RENDERED_MANIFEST}"

# Next policy: stop the previous job, clear values run-env.sh only sets when unset
# (otherwise a DS2 shell keeps DS2's image tags for A4S), then repeat
scripts/autoscaling/job-management/stop-job.sh flink
unset EVENTS FLINK_RUNTIME_IMAGE_TAG FLINK_BENCHMARK_IMAGE_TAG OPERATOR_IMAGE_TAG CHECKPOINT_INTERVAL
export POLICY=a4s RUN_ID=20261111-q20-a4s-01   # POLICY: ds2, justin or a4s; new RUN_ID per run
source experiments/1729-checkpoint-aware-rescaling/run-env.sh
bash experiments/1729-checkpoint-aware-rescaling/render-job.sh && kubectl apply -f "${RENDERED_MANIFEST}"
```

By default, `run-env.sh` deploys A4S on the `a4s-shards-mrc` images and
DS2/Justin on the older `benchmark-checkpoint-rescale` images, so the
baseline images are never overwritten. For a like-for-like comparison, run all
three policies on the same images:

```bash
export FLINK_RUNTIME_IMAGE_TAG=a4s-shards-mrc \
       FLINK_BENCHMARK_IMAGE_TAG=a4s-shards-mrc \
       OPERATOR_IMAGE_TAG=a4s-shards-mrc
export POLICY=justin RUN_ID=20261111-q20-justin-a4simg-01
source experiments/1729-checkpoint-aware-rescaling/run-env.sh
```

The operator image is deployed once for the whole cluster (`instruction.md`
part A5), not per job.

Settings such as memory, the A4S latencies and the SHARDS sampling rate live
in each manifest's `flinkConfiguration`. `docs/a4s-shards-mrc.md` (section
Configuration) explains where each one is, what it does, and which memory
settings must change together.

Checkpoint-preserving rescaling is on by default for every policy. To compare
without it, keep the same setting for all policies and give a periodic
checkpoint interval:

```bash
export CHECKPOINT_RESCALE=false CHECKPOINT_INTERVAL=1m
source experiments/1729-checkpoint-aware-rescaling/run-env.sh
```

### Documents


| Document | Read it for |
|---|---|
| [`docs/a4s-shards-mrc.md`](docs/a4s-shards-mrc.md) | Start here. It covers how A4S works (overview and diagram), how to build and run it, running on another cluster, comparing with DS2/Justin, how to test, limitations, all configuration keys, and an appendix with per-component details and the key APIs. |
| [`experiments/1729-checkpoint-aware-rescaling/instruction.md`](experiments/1729-checkpoint-aware-rescaling/instruction.md) | The step-by-step c165 runbook for any policy: build and deploy the images, run one benchmark, collect evidence. |
| [`scripts/autoscaling/README.md`](scripts/autoscaling/README.md), [`cluster/README.md`](cluster/README.md) | Setting up the cluster itself: prerequisites, node labels, monitoring, operator. |
| [`docs/image-builds.md`](docs/image-builds.md) | General image build and tag conventions. |

## Initial Setup

```bash
git submodule update --init --recursive
source scripts/env.sh
```

## Build Commands

```bash
images/flink-runtime/build.sh
images/flink-benchmark-runtime/build.sh
images/flink-kubernetes-operator/build.sh
```

Per-image build scripts live under `images/`. Stack-level image commands live
under `scripts/images/`.

`scripts/images/build-all-default.sh` builds the configured default benchmark
runtime and operator images. It rebuilds the base Flink runtime only when
passed `--with-runtime`.

To preview commands without building:

```bash
scripts/images/build-all-default.sh --dry-run
scripts/images/push-images.sh --dry-run
```

To build a variant without editing scripts, pass source and tag overrides:

```bash
images/flink-runtime/build.sh \
  --source-dir sources/flink/my-custom-flink \
  --tag custom-flink-tag

images/flink-kubernetes-operator/build.sh \
  --source-dir sources/operators/my-custom-operator \
  --tag custom-operator-tag
```

Cluster manifests live under `cluster/`. Operational cluster and job commands
live under `scripts/autoscaling/`. The q20_unique c153 external Kafka flow lives
under `experiments/1724-kafka-q20-unique/`.
