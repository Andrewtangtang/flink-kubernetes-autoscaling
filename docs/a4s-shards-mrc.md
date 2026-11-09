# A4S on the checkpoint-preserving stack (SHARDS MRC)

## TL;DR

This document describes the A4S Flink autoscaler as implemented on branch
`a4s-shards-mrc`. It covers how the components connect, what can be tuned, how
to build and run it, and how to compare it with DS2 and Justin.

- **What A4S does.** A4S co-decides parallelism and managed memory for each
  stateful operator. It uses a miss-rate curve (MRC) of the operator's RocksDB
  block cache to find, for each parallelism P, the least memory that still
  sustains the target rate. The algorithm follows Kexiang Wang's thesis
  (UofT 2023).
- **Where the MRC comes from.** It comes from an online **SHARDS** estimator
  inside FRocksDB's shared block cache: fixed-rate spatial sampling at R = 0.01,
  with byte-weighted reuse distances. The **SHARDS_adj** correction is on by
  default. This replaces the QuickMRC estimator of the original A4S port.
- **What A4S reuses.** It runs inside the Justin operator and reuses Justin's
  per-vertex resource-profile path. It also reuses the checkpoint-gated
  rescaling transaction: fresh checkpoint, producer pause, in-place rescale.
  So DS2, Justin and A4S share one realization path.
- **Where everything lives.** The Flink runtime, the K8s operator and FRocksDB
  are all tracked directories of this repository. Only the Nexmark SQL
  benchmarks remain a submodule.

| Component | Path | Base |
|---|---|---|
| Flink runtime 1.18 | `sources/flink/flink-1-18-src-from-justin/` | Justin's runtime, plus `flink-a4s`, `/a4s-metrics` and the SHARDS wiring |
| K8s operator 1.13 | `sources/operators/flink-kubernetes-operator-justin/` | Justin operator with checkpoint transaction (`ec2e63c`), plus the A4S decision layer |
| FRocksDB 6.20.3 | `sources/libs/frocksdb/` | `RyanL123/frocksdb@quickmrc` (`d439c0fbf`), plus the SHARDS estimator and JNI |
| Experiment | `experiments/1729-checkpoint-aware-rescaling/` | `POLICY=a4s` manifests for q4, q9, q18, q19, q20 |

`sources/flink/flink-runtime-1-18-a4s/` and
`sources/operators/flink-kubernetes-operator-a4s/` hold the original A4S port.
They are reference material only. That port runs on a different runtime and
has no checkpoint transaction, so its results are not comparable with DS2 or
Justin on this stack.

## A4S

### Model (thesis §3.2–3.3)

For a stateful operator with target rate T, cache-hit latency `l_hit` and
cache-miss latency `l_miss`, Little's law gives the largest miss rate that P
tasks can tolerate (Eq 3.4):

```
m*(P) = (P / T - l_hit) / (l_miss - l_hit)
```

Inverting the MRC at `m*(P)` gives the least operator-total cache that sustains
T. The resulting points form the memory-parallelism curve (MPC).

- `Pmin = ⌈l_hit · T⌉`: below it, even an all-hit cache cannot keep up.
- `Pmax = ⌈l_max · T⌉`: optional, bounded by the per-record latency budget
  `l_max`.

The decision algorithm:

- **Alg 1.** Start every stateful operator at Pmin. If a placement fails,
  raise one operator's P and retry.
- **Alg 2.** Each task of an operator at P gets `MPC[P] / P`.
- **Alg 3.** Raise the operator whose memory drops the most for P → P+1.

### Components and data flow

```
TaskManager slot (managed memory M)
  shared LRUCache (data blocks + memtable charges via WriteBufferManager)
    └─ ShardsMrcEstimator (FRocksDB, cache-owned)
         fed by every user data-block access in BlockBasedTable
  RocksDBNativeMetricMonitor
    gauge  rocksdb.stack-distance-histogram   (v2: header + bins + infinite bin)
    gauges rocksdb.a4s.shards.*               (header fields)
        │  metric fetch
        ▼
JobManager  GET /jobs/:jobid/vertices/:vertexid/a4s-metrics
  mergeShards(subtask histograms, vertex parallelism P)
  → { scaledMrc (cumulative, unadjusted), mergedHistogram (cumulative v2) }
        │  REST, per vertex, only when a4s.enabled
        ▼
Operator  ScalingMetricCollector.windowMissRateCurves
  window = current − baseline (baseline refreshed while stabilizing)
  MRC    = fromShardsHistogram(window, SHARDS_adj)
        ▼
  ScalingMetricEvaluator   MRC + TARGET_DATA_RATE → MPC per vertex
        ▼
  ScalingExecutor          A4S.makeDecision → (P, per-task managed MB)
                           → parallelism + resource-profile overrides
        ▼
  CheckpointRescaleManager  producer pause → checkpoint → apply → restore → verify
        ▼
JobManager  PUT /jobs/:jobid/justin  → AdaptiveScheduler in-place rescale
```

### MRC estimation: SHARDS in FRocksDB

Source: `sources/libs/frocksdb/cache/shards_mrc_estimator.{h,cc}`.

**Ownership and scope**

- The estimator is owned by the `Cache` object: one per slot's shared cache,
  set through `Cache::SetShardsMrcEstimator`.
- It is fed from `block_based_table_reader.cc`, next to every block-cache trace
  point.
- It sees **user data blocks only** (`IsUserDataBlockAccess`). Index/filter
  blocks and WBM dummy entries are excluded.

**Sampling**

- Spatial sampling: a block is sampled iff
  `GetSliceNPHash64(block_key, seed) ≤ R · 2^64`.
- Only sampled accesses take the estimator's mutex.

**Distance**

- The distance of a re-access is
  `(Σ charges of distinct sampled blocks accessed since its last access) / R + own charge`.
  That is the smallest byte-capacity LRU in which the access hits, matching
  RocksDB's LRU semantics.
- It uses real cache charges; no 4 KiB block-size constant is assumed.

**Histogram**

- Bin `i` covers `(i, i+1] · bin_bytes` per-task cache.
- One infinite bin follows. It holds cold misses, re-accesses of blocks evicted
  by the bound, and distances beyond the last bin.
- The number of bins is `D_cap / bin_bytes`; the defaults give 4096.

**Memory bound (D_cap)**

- While the tracked sampled charge divided by R exceeds `D_cap`, the
  least-recently used sampled block is dropped and counted in `evicted_by_cap`.
- Counters are monotonic and never reset.

**JNI**

- `LRUCache.enableShardsMrc(R, seed, binBytes, maxDistanceBytes)` enables the
  estimator.
- `LRUCache.getShardsMrcHistogram()` returns `long[]`: a 10-field header
  (`samplingRate` bits, `hashSeed`, `binBytes`, `maxDistanceBytes`,
  `totalRefs`, `sampledRefs`, `evictedByCap`, `trackedEntries`,
  `trackedBytes`, `estimatorNanos`), then the finite bins, then the infinite
  bin.

QuickMRC is still in the same `LRUCache`. With `mrc-estimator: shards` it is
disabled in the cache, so the two estimators never both run. QuickMRC is kept
only for A/B runs.

### Runtime: wiring and aggregation

Source: `sources/flink/flink-1-18-src-from-justin/`, modules `flink-a4s`,
`flink-statebackend-rocksdb` and `flink-runtime`.

**Cache creation**

- The estimator is enabled right after the shared cache is created
  (`RocksDBMemoryControllerUtils`).
- That cache exists only with `state.backend.rocksdb.memory.managed: true`. DS2
  and Justin run with `false`, so no estimator is ever created for them.

**Index and filter blocks**

- Index/filter blocks stay out of the managed cache:
  `cacheIndexAndFilterBlocks`, `...WithHighPriority`,
  `pinL0FilterAndIndexBlocksInCache` and `pinTopLevelIndexAndFilter` are all
  `false` (`RocksDBResourceContainer`).
- The cache therefore holds only what the MRC models (data blocks) plus WBM
  charges. Index/filter live in table-reader native memory, as they do in
  unmanaged DS2/Justin runs.

**Monitor and gauges**

- One monitor reads each shared cache, so several backends in one slot do not
  each report a slice.
- `rocksdb.stack-distance-histogram` is the JSON-serialized
  `StackDistanceHistogram` (v2 carries the SHARDS header).
- The `rocksdb.a4s.shards.*` gauges expose the header fields.

**Aggregation (`A4SAggregatingVertexMetricsHandler`)**

- Subtask histograms are summed by `mergeShards(list, P)`.
- P is the **vertex parallelism**, fetched from the execution graph, not the
  number of subtasks that have reported.
- MRC point `i` is at operator-total cache `(i+1) · bin_bytes · P`, using the
  scaling assumption `MRC_op(m·P) = MRC_task(m)` (thesis §3.4.2).
- The response carries both `scaledMrc` (cumulative, unadjusted, kept for
  debugging and QuickMRC) and `mergedHistogram` (cumulative v2, SHARDS only).

### Operator: window, SHARDS_adj, MPC, decision

Source: `sources/operators/flink-kubernetes-operator-justin/flink-autoscaler/`.

**Gating.** MRC collection and MPC evaluation run only with
`job.autoscaler.a4s.enabled`. DS2 and Justin make no `/a4s-metrics` calls.

**Window (thesis "cumulative since stabilization").**
`ScalingMetricCollector.windowMissRateCurves` keeps one baseline histogram per
job and vertex.

- **While stabilizing:** every collection overwrites the baseline with the
  current cumulative histogram.
- **After stabilization:** the MRC is built from
  `current.minusShards(baseline)`.
- **Reset:** `minusShards` returns empty if a counter went down or P changed,
  meaning the caches were recreated. The baseline is then reset to the current
  histogram.
- **Rescale:** an in-place rescale advances the job's RUNNING timestamp.
  `CheckpointRescaleManager` waits for that before completing. The collector
  then clears its history and calls `cleanup`, which drops the baselines, so
  every rescale starts a new window.
- **Equivalence:** this equals HistoChron's interval query over epoch
  histograms (Sultan, PhD thesis 2024, §5.3.2).

**SHARDS_adj** (`MissRateCurve.fromShardsHistogram(h, adjust)`):

```
plain:  miss(c) = (Ns − hits(c)) / Ns
adj:    miss(c) = (Ns − hits(c)) / (R · N)        clamped to [0, 1]
```

- `N` is `totalRefs` and `Ns` is `sampledRefs` (bins plus the infinite bin).
- adj credits the sample's shortfall `R·N − Ns` as hits at the smallest
  distance, which corrects hot blocks that the spatial sample over- or
  under-represents.
- When `Ns ≈ R·N` it is a no-op.

**MPC** (`MemoryParallelismCurve.fromMissRateCurve`, called per vertex with
`T = TARGET_DATA_RATE`):

1. P ranges over `[vertex.min-parallelism, min(vertex.max-parallelism, ⌈l_max·T⌉)]`.
2. A P is skipped if `m*(P) ≤ 0` (below Pmin), or if `m*(P)` is below the
   lowest miss rate on the MRC. Such a P is unreachable within the D_cap range
   and because of cold misses.
3. Otherwise `MPC[P]` is the smallest MRC cache size with `miss ≤ min(m*(P), 1)`.
4. If no P is feasible, the vertex gets no MPC.

**Decision** (`A4S.makeDecision`):

- **Per-task managed memory:**
  ```
  task_mb(P) = MPC[P] / P / (1 − write_buffer_ratio)
  ```
  `write_buffer_ratio` is `state.backend.rocksdb.memory.write-buffer-ratio`
  (default 0.5). Flink sizes the shared cache at `(3−r)/3·M` and the WBM at
  `2r/3·M`, so data blocks get `M·(1−r)` once memtables fill the WBM.
- **Start point:** each operator starts at the first P ≥ Pmin with
  `task_mb(P) ≤ a4s.max-task-managed-memory.mb − a4s.memory-base.mb`. This is
  the thesis's "Pmin is raised" for a per-task memory limit. An operator that
  fits at no P is dropped with a warning instead of blocking the others.
- **Search loop:** `place()` checks each operator's per-task memory against
  that cap. On failure, Alg 3 raises one operator, up to `a4s.max-attempts`
  times.

**Applying the decision** (`ScalingExecutor.applyA4SDecisions`):

| Vertex | Parallelism | Managed memory per slot |
|---|---|---|
| Has a decision | decided P | `task_mb + a4s.memory-base.mb` |
| Stateless (no RocksDB metrics) | DS2's recommendation | `a4s.memory-base.mb` |
| Stateful, no decision (no MRC or no feasible P) | DS2's recommendation | carried over from the previous period. In the first period nothing is carried, so the override falls back to Justin's memory-level profile |

- **Stateful** means the vertex reports RocksDB metrics
  (`ROCKS_DB_BLOCK_CACHE_HIT_RATE` present). This is the same signal the
  collector uses.
- **Resource profile.** Managed memory becomes a per-vertex `ResourceProfile`
  through `getVertexResourceProfileOverrides`. CPU is 1.0. For TMs > 3 GiB the
  profile uses heap 363 MB and network 84 MB; otherwise heap 134 MB and network
  39 MB.
- **History output.** Decisions are recorded in the scaling-configuration
  history (`managedMemoryMB` field).
- **No MRC persistence.** MRCs are kept out of the persisted metric history
  (`@JsonIgnore`). Up to 4096 points per vertex in every entry would exceed the
  1 MB ConfigMap cap, and the cap's trimming would then empty the metric
  window.

### Realization: checkpoint-gated rescaling

A4S produces the same outputs as Justin: parallelism overrides and
resource-profile overrides. With `job.autoscaler.checkpoint-rescale.enabled`,
`CheckpointRescaleManager` applies them as a transaction:

```
WAITING_PRODUCER_PAUSE → WAITING_CHECKPOINT → CHECKPOINT_TRIGGERED → READY_TO_APPLY
→ APPLYING → RESTORING → VERIFYING → WAITING_PRODUCER_RESUME → COMPLETED | FAILED
```

The transaction is persisted in the autoscaler state store. With the switch off,
`NativeFlinkService.scale` uses the original in-place path, which restores from
the latest periodic checkpoint. A4S requires `job.autoscaler.justin.enabled`,
because it replaces Justin's `policy()` inside the Justin branch of
`ScalingExecutor`.

### Configuration

**Operator (`job.autoscaler.*`, set in the FlinkDeployment's `flinkConfiguration`)**

| Key | Default | Meaning |
|---|---|---|
| `justin.enabled` | `false` | Required for A4S |
| `a4s.enabled` | `false` | Enables MRC collection, MPC evaluation and A4S decisions |
| `a4s.hit-latency-sec` | `5e-6` | `l_hit` in Eq 3.4 |
| `a4s.miss-latency-sec` | `5e-3` | `l_miss` in Eq 3.4 |
| `a4s.max-latency-sec` | unset | `l_max`; caps P at `⌈l_max·T⌉`. Unset: only `vertex.max-parallelism` applies |
| `a4s.max-task-managed-memory.mb` | `600` | Per-slot managed-memory cap, including `memory-base.mb` |
| `a4s.memory-base.mb` | `0` | Added to every A4S vertex's managed memory |
| `a4s.max-attempts` | `20` | Alg 1 retry bound |
| `a4s.shards-adj.enabled` | `true` | SHARDS_adj on the windowed MRC |
| `a4s.managed-memory-override.enabled` / `.mb` | `false` / `0` | Debug: force a constant managed memory |
| `vertex.min-parallelism`, `vertex.max-parallelism` | upstream defaults | P range of the MPC |
| `stabilization.interval`, `metrics.window` | upstream | Window start and decision cadence |
| `checkpoint-rescale.enabled` | `false` | Checkpoint-gated realization |
| `checkpoint-rescale.producer-pause.enabled` / `.timeout` | `false` / `1m` | Pause the Kafka producer around the checkpoint |
| `checkpoint-rescale.checkpoint-timeout`, `.restore-timeout` | `5m`, `10m` | Transaction timeouts |

**Runtime (`state.backend.rocksdb.*`)**

| Key | Default | Meaning |
|---|---|---|
| `memory.managed` | Flink default `true` | Must be `true` for A4S; the shared cache and estimator exist only then |
| `memory.write-buffer-ratio` | `0.5` | Also read by the operator for `task_mb` |
| `a4s.mrc-estimator` | `shards` | `shards` or `quickmrc` |
| `a4s.shards.sampling-rate` | `0.01` | R |
| `a4s.shards.hash-seed` | `0` | Sampling hash seed; pin and record it per run |
| `a4s.shards.bin-size` | `1mb` | Per-task cache width of one bin |
| `a4s.shards.max-distance` | `4gb` | D_cap |
| `metrics.stack-distance-histogram` | `false` | Must be `true`, or the JobManager receives no histogram |
| `metrics.estimate-table-readers-mem` | `false` | Recommended: index/filter memory outside the managed cache |
| `a4s.quick-mrc.*` | see `RocksDBOptions` | QuickMRC only |

**Experiment (`experiments/1729-checkpoint-aware-rescaling/run-env.sh`)**

| Variable | Default | Meaning |
|---|---|---|
| `POLICY` | `justin` | `ds2`, `justin`, or `a4s` |
| `QUERY` | `q20` | `q4`, `q9`, `q18`, `q19`, `q20` |
| `EXPERIMENT_PROFILE` | `paper` | `paper` (c165) or `normalized` (AWS; no A4S manifests yet) |
| `RUN_ID` | required | Unique per run |
| `CHECKPOINT_RESCALE` | `true` | Renders both checkpoint-rescale flags |
| `CHECKPOINT_INTERVAL` | `24h` if gated; required otherwise | `execution.checkpointing.interval` |
| `FLINK_RUNTIME_IMAGE_TAG`, `FLINK_BENCHMARK_IMAGE_TAG`, `OPERATOR_IMAGE_TAG` | `a4s-shards-mrc` for A4S, `benchmark-checkpoint-rescale` otherwise | Image tags |

The A4S manifests (`jobs/<q>/a4s/experiment.yaml`) are Justin's manifests plus:

- `memory.managed: true`;
- the `a4s.*` keys above, with explicit `l_hit`/`l_miss`;
- `mrc-estimator: shards`, R 0.01, seed 0;
- `metrics.stack-distance-histogram: true`;
- `a4s.max-task-managed-memory.mb` set to Justin's top per-slot level on a
  3 GiB TM, `158 · 2^(max-level − 1)`: q20 632, the others 1264.

Justin's options factory is kept for identical RocksDB tuning (direct I/O,
compaction, memtables, bloom filter). Under managed memory, Flink replaces the
factory's block cache and WBM with the shared ones after the factory runs.

### Observability

| Signal | Where | Use |
|---|---|---|
| `rocksdb.a4s.shards.sampled-refs / (sampling-rate · total-refs)` | TM gauges | Sample representativeness; far from 1 means hot-block bias, which adj corrects |
| `rocksdb.a4s.shards.evicted-by-cap` | TM gauge | D_cap pressure; nonzero growth means the MRC tail is truncated |
| `rocksdb.a4s.shards.tracked-entries`, `tracked-bytes`, `estimator-ns` | TM gauges | Estimator footprint and CPU cost |
| `rocksdb.estimate-table-readers-mem` | TM gauge | Index/filter memory outside the managed cache |
| `GET /jobs/:jobid/vertices/:vertexid/a4s-metrics` | JM REST | Cumulative merged histogram and unadjusted MRC |
| `A4S: ...` log lines | Operator log | MPCs, start P per operator, decisions, no-decision warnings |
| Scaling-configuration history (`managedMemoryMB`) | Autoscaler ConfigMap | Per-period decisions |

## How to build and run

Commands run on c165 from the repository root, following
`experiments/1729-checkpoint-aware-rescaling/instruction.md`. Images are built
under the `a4s-shards-mrc` tag, so the DS2/Justin baseline images are not
replaced.

### 1. Environment

```bash
ssh -A c165
cd ~/flink-kubernetes-autoscaling
git fetch origin && git switch a4s-shards-mrc && git pull --ff-only
git submodule update --init --recursive      # Nexmark SQL benchmarks only

export QUERY=q20 POLICY=a4s RUN_ID=20261110-q20-a4s-01
unset EVENTS HELM_CHART
source experiments/1729-checkpoint-aware-rescaling/run-env.sh
# prints runtime/benchmark/operator images ending in :a4s-shards-mrc
```

### 2. Build

The order matters: the runtime image installs the FRocksDB jar that step (a)
produces.

```bash
# (a) FRocksDB JNI jar (Ubuntu 22.04 crossbuild); copies rocksdbjni-*-linux*.jar
#     into the runtime source dirs, where the runtime Dockerfile installs it
#     as org.rocksdb:frocksdbjni:6.20.3-custom
sudo bash images/frocksdb/build.sh

# (b) Flink runtime (full Flink build, includes flink-a4s), then the benchmark image
sudo bash images/flink-runtime/build.sh \
  --source-dir "${FLINK_RUNTIME_SOURCE_DIR}" --tag "${FLINK_RUNTIME_IMAGE_TAG}"
sudo bash images/flink-benchmark-runtime/build.sh \
  --runtime-image "${FLINK_RUNTIME_LOCAL}" --tag "${FLINK_BENCHMARK_IMAGE_TAG}"

# (c) Operator; build.sh passes --build-context flink-src=<runtime source> so the
#     Dockerfile can install flink-a4s first (requires BuildKit)
sudo bash images/flink-kubernetes-operator/build.sh \
  --source-dir "${OPERATOR_SOURCE_DIR}" --tag "${OPERATOR_IMAGE_TAG}"
```

Then tag and push all three images, pre-pull them, deploy the operator and
verify its digest, as in parts A4–A5 of `instruction.md`.

The operator is cluster-wide. Deploying the `a4s-shards-mrc` operator also
serves DS2/Justin jobs: its behavior is unchanged for them when `a4s.enabled`
is off, but it is a different build from the baseline operator.

### 3. Run

```bash
# Render (checks placeholders and required values), then follow Part B of instruction.md
bash experiments/1729-checkpoint-aware-rescaling/render-job.sh
kubectl apply --dry-run=server -f "${RENDERED_MANIFEST}"
kubectl apply -f "${RENDERED_MANIFEST}"
```

Without checkpoint gating, set an explicit periodic interval:

```bash
export CHECKPOINT_RESCALE=false CHECKPOINT_INTERVAL=1m
source experiments/1729-checkpoint-aware-rescaling/run-env.sh
```

### Local unit tests (no cluster)

```bash
# FRocksDB estimator (macOS clang needs DISABLE_WARNING_AS_ERROR=1)
make -C sources/libs/frocksdb DISABLE_WARNING_AS_ERROR=1 shards_mrc_estimator_test && \
  sources/libs/frocksdb/shards_mrc_estimator_test

# Runtime: install flink-parent (-N) and flink-a4s into a scratch repo, then
mvn -Dmaven.repo.local=<scratch> -pl flink-a4s install
mvn -Dmaven.repo.local=<scratch> -pl flink-runtime test -Dtest='A4S*Test'

# Operator (against the same scratch repo)
mvn -Dmaven.repo.local=<scratch> -pl flink-autoscaler test
```

Run the `mvn` commands from the runtime and operator source directories,
respectively. Use a scratch `maven.repo.local` so that locally built SNAPSHOT
artifacts do not shadow `~/.m2`.

Known status:
- `flink-a4s` 26/26 and runtime `A4S*Test` 9/9.
- Operator `flink-autoscaler` 176 tests with 2 failures:
  `MetricsCollectionAndEvaluationTest.testEndToEnd` and
  `AutoScalerOptionsTest`. Both also fail at the Justin base `ec2e63c`.

### Compare with Justin and DS2

**What is identical across the three policies**

- The operator build and the runtime build.
- The checkpoint-gated realization path and its timeouts.
- The RocksDB tuning factory.
- Index/filter placement: in table-reader memory, outside the cache.
- The query manifests, apart from the policy keys.
- `vertex.max-parallelism`, `stabilization.interval` and `metrics.window`.

**What differs by design**

| | DS2 | Justin | A4S |
|---|---|---|---|
| Parallelism | DS2 rates | DS2 rates, plus vertical-first heuristic | MPC search from Pmin |
| Memory | default slot share (`taskmanager.memory.managed.size` / slots) | discrete levels `158·2^k` MB (3 GiB TM) | continuous per-task MB from MPC |
| RocksDB memory | unmanaged; Justin factory sizes its own cache | same as DS2 | Flink managed: shared cache + WBM per slot |
| Signals | rates, busy time | plus cache-hit rate, state latency | plus SHARDS MRC |

**Rules for a fair comparison**

1. **Same ceiling.** A4S's per-slot cap equals Justin's top memory level for the
   query (already set in the manifests).
2. **Same inputs.** Keep `EVENTS`, `TPS`, `CHECKPOINT_RESCALE` and
   `CHECKPOINT_INTERVAL` identical across policies for a query.
3. **Report memory with its model.** For A4S, managed memory M yields about
   `M·(1−r)` of data-block cache. For DS2/Justin, the factory-sized cache is not
   the same quantity. Report managed MB per slot together with
   `estimate-table-readers-mem`, which is outside both budgets.
4. **Rebuild the baselines.** Existing DS2/Justin checkpoint results were
   produced with the `benchmark-checkpoint-rescale` images (operator `ec2e63c`).
   For a strict comparison, re-run DS2/Justin on the `a4s-shards-mrc` images by
   setting `POLICY=ds2` or `justin` with `FLINK_RUNTIME_IMAGE_TAG`,
   `FLINK_BENCHMARK_IMAGE_TAG` and `OPERATOR_IMAGE_TAG=a4s-shards-mrc`.
5. **Metrics.** Compare:
   - time to reach the target rate;
   - number of rescales and their interruption time (transaction
     `observedRestartDurationMillis`);
   - steady-state parallelism and managed memory per vertex;
   - catch-up lag;
   - total slots and managed memory.

## Limitations and further research directions

### Latency calibration

A4S decisions depend on `l_hit` and `l_miss` as much as on the MRC:
`m*(P) = (P/T − l_hit) / (l_miss − l_hit)` sets Pmin (`⌈l_hit·T⌉`) and the miss
rate at which every MPC point is read.

**Current state**

- Both values are fixed configuration: `a4s.hit-latency-sec = 5e-6` and
  `a4s.miss-latency-sec = 5e-3`, the thesis-style ratio of 1000.
- Neither is measured on this cluster. A wrong `l_miss` shifts every
  `m*(P)`, and therefore the memory chosen for every P.
- Every result must record the values used.

**Calibration path**

1. **Thesis method (§4).** Estimate both online:
   `l_hit = (1 − T_io − T_wait) / R_input` and
   `l_miss = l_hit + T_io / R_io`.
2. **Flink equivalent.** Take the mean per-record latency `1/TPR` (true
   processing rate) and the window's measured miss ratio `MR`, then solve
   `1/TPR = MR·l_miss + (1 − MR)·l_hit` across two windows, or across the two
   sides of a rescale.
3. **Cross-check.** Compare the result with the state-latency metrics the
   Justin operator already collects (`*_STATE_GET_MEAN_LATENCY`).

### A4S in the real environment

These limits come from how A4S is realized on this stack.

- **Placement.** `place()` checks only the per-slot memory cap. It is not the
  thesis's worker-level worst-fit bin-packing over CPU and memory. Which TM a
  slot lands on is left to Justin's resource-profile path and the adaptive
  scheduler, so Alg 3 rarely runs.
- **One stateful task per slot.** `task_mb = MPC[P] / P / (1 − r)` assumes each
  slot's managed memory serves one stateful task. This holds with Justin's
  per-vertex resource profiles.
- **Write-buffer factor.** `1/(1 − r)` assumes memtables fill the WBM. When they
  do not, the data-block cache is larger than A4S planned for.
- **Index/filter memory.** It lives in table-reader native memory outside the
  managed budget, and grows with the number of open SSTs. Watch
  `rocksdb.estimate-table-readers-mem`. If it becomes significant against the
  cache, or a TM is OOM-killed, move index/filter into the cache and subtract
  the measured size from A4S's budget.
- **D_cap.** Reuse distances beyond `a4s.shards.max-distance` (4 GiB per task)
  are truncated into the infinite bin, which caps the memory an MPC point can
  ask for; watch `rocksdb.a4s.shards.evicted-by-cap`.
- **First decision.** Before any MRC exists, a stateful vertex without a
  decision falls back to Justin's memory-level profile. The initial
  configuration comes from the manifest (thesis "cold start").
- **Operator restart.** The MRC baseline is held in operator memory. A restart
  outside stabilization loses one collection interval before the window
  resumes.
- **TM profile.** Heap and network sizes in the resource profile come from two
  fixed TM-size branches in `ScalingExecutor`, not from the TM's configuration.
- **Coupling to Justin.** A4S runs only with `justin.enabled`, because it
  replaces Justin's `policy()` and reuses its overrides and history.
