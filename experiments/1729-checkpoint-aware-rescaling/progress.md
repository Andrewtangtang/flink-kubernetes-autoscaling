# Checkpoint-Aware Rescaling Progress

- Updated: 2026-08-11
- Outer branch: `benchmark-checkpoint-rescaling`
- Base: normalized `benchmark` at `9666e588`
- Operator branch: `benchmark-checkpoint-rescaling` from benchmark `e07e163`
  plus checkpoint transaction `7aa39d3`
- Current status: independent query matrix complete locally; review and cluster validation pending

## Completed

- [x] Traced Justin and DS2 decisions from `ScalingExecutor` through
  `KubernetesScalingRealizer`, `NativeFlinkService`, and the AdaptiveScheduler.
- [x] Confirmed Flink already exposes checkpoint trigger/status REST APIs and
  restores the latest completed checkpoint when rebuilding the execution
  graph.
- [x] Verified c165, c167, and c182 share the same NFS-backed checkpoint path.
- [x] Created an isolated worktree and branch without touching the existing
  dirty experiment branch.
- [x] Brought in the allocator diagnostics and atomic-assignment safeguards.
- [x] Fixed the v1 transaction, failure, retry, and Q20 pilot design in
  `PLAN.md`.
- [x] Persisted the transaction in the autoscaler ConfigMap and blocked new
  decisions until its frozen target completes or fails.
- [x] Gated Justin P/M and DS2 P realization on a fresh completed configured
  checkpoint.
- [x] Kept checkpoint-mode native scaling fail-closed and sent a complete
  vertex-requirements map to the AdaptiveScheduler.
- [x] Added Q20 pilot configuration, retry/abort controls, live observation,
  and evidence capture without changing the replay baseline.
- [x] Added focused transaction persistence, checkpoint gate, abort,
  memory-only rescale, complete-map, and fail-closed tests.
- [x] Replaced the fixed five-minute restart assumption after the first
  successful rescale with the measured apply-to-RUNNING duration. The pilot
  keeps a one-minute bootstrap, a ten-minute observation cap, and the separate
  real Kafka-lag catch-up term.
- [x] Extended the same durable checkpoint transaction to DS2's
  parallelism-only realization path. DS2 now fails closed instead of falling
  back to a regular upgrade; every Justin/DS2 manifest enables the same
  checkpoint and restart-tracking mechanism directly.
- [x] Required an explicit `RUN_ID` and added atomic manifest rendering so
  checkpoint, savepoint, HA, job-name, manifest, and evidence provenance are
  isolated per experiment. Cleanup remains an explicit post-archive action.
- [x] Materialized independent Q4, Q9, Q18, Q19, and Q20 Justin/DS2 manifests
  from the normalized benchmark branch. Every manifest directly contains its
  checkpoint, HA, image, resource, and query-specific source settings; none
  depends on a benchmark Kustomize overlay.
- [x] Moved the shared Flink and Prometheus forwards to 18082 and 19091 after
  confirming c165 port 8081 is the long-running kube-state-metrics telemetry
  endpoint. Monitoring and evidence tools share the same overridable defaults.
- [x] Added guarded whole-run NFS cleanup for intentional `RUN_ID` reuse. It
  fails closed unless Kubernetes is reachable, no FlinkDeployment or Flink pod
  exists, and the caller repeats the exact run ID.

## Pending Validation

- [ ] Operator autoscaler and Kubernetes operator unit tests.
- [ ] Flink runtime allocator unit tests.
- [x] Shell syntax, Python compilation, YAML parsing, and image/deployment
  command dry-runs.
- [ ] Render and server-side Kubernetes dry-run for all ten manifests on c165.
- [ ] Matched Justin and DS2 continuous-input cluster pilots for all queries.
- [ ] Review complete diffs before creating new commits.

## Evidence and Blockers

- The original checkout remains on `q4-ds2-justin-reevaluation` with its
  pre-existing uncommitted experiment files.
- GitHub access was unavailable inside the local sandbox during submodule
  initialization. The feature submodule was therefore cloned from the
  existing local object store; publishing still targets the Andrewtangtang
  fork during the reviewed push stage.
- This Mac has no Maven, kubectl, or Docker. Java unit tests, Kubernetes
  server validation, and image builds must run on c165 after review.
- Local validation completed with:
  `bash -n`, `python3 -m py_compile`, `git diff --check`, Ruby YAML parsing,
  image build `--dry-run`, image push `--dry-run`, and operator deployment
  `--dry-run`.

## Next Step

Review the outer and operator-submodule diffs before creating commits. Then
publish both feature branches, pull them on c165, run the Java tests and
server-side manifest dry-runs, and start the checkpoint-aware query matrix.
