# Checkpoint-Aware Rescaling Progress

- Updated: 2026-08-09
- Outer branch: `checkpoint-aware-inplace-rescaling`
- Base: `main` at `f8ce7fbb`
- Operator branch: `checkpoint-aware-inplace-rescaling` from `70ab735`
- Current status: implementation complete locally; review and cluster validation pending

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
  back to a regular upgrade, and matched Q20 Justin/DS2 overlays share one
  checkpoint and restart-tracking patch.
- [x] Required an explicit `RUN_ID` and added atomic manifest rendering so
  checkpoint, savepoint, HA, job-name, manifest, and evidence provenance are
  isolated per experiment. Cleanup remains an explicit post-archive action.

## Pending Validation

- [ ] Operator autoscaler and Kubernetes operator unit tests.
- [ ] Flink runtime allocator unit tests.
- [x] Shell syntax, Python compilation, YAML parsing, and image/deployment
  command dry-runs.
- [ ] Kustomize render and server-side Kubernetes dry-run on c165.
- [ ] Matched Q20 Justin and DS2 continuous-input cluster pilots.
- [ ] Review complete diffs before creating new commits.

## Evidence and Blockers

- The original checkout remains on `q4-ds2-justin-reevaluation` with its
  pre-existing uncommitted experiment files.
- GitHub access was unavailable inside the local sandbox during submodule
  initialization. The feature submodule was therefore cloned from the
  existing local object store; publishing still targets the Andrewtangtang
  fork during the reviewed push stage.
- This Mac has no local JDK, Maven, kubectl, or Docker. Java unit tests,
  Kustomize rendering, and image builds must run on c165 after review.
- Local validation completed with:
  `bash -n`, `python3 -m py_compile`, `git diff --check`, Ruby YAML parsing,
  image build `--dry-run`, image push `--dry-run`, and operator deployment
  `--dry-run`.

## Next Step

Review the outer and operator-submodule diffs before creating commits. Then
publish both feature branches, pull them on c165, run the Java tests and
server-side manifest dry-run, and start the Q20 continuous-input pilot.
