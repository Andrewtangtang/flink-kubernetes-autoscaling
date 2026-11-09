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
