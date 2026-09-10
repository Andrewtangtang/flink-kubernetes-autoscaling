# Checkpoint-Aware In-Place Rescaling

## Goal

Add opt-in Justin and DS2 rescaling paths that preserve Flink state and Kafka
offsets. A scaling decision is frozen, a fresh configured checkpoint must
complete, and only then are its parallelism and optional resource-profile
overrides sent to the AdaptiveScheduler.

```text
3m stabilization + 2m metrics
             |
             v
  freeze Justin P/M or DS2 P target
             |
             v
 wait for periodic checkpoint, if any
             |
             v
 trigger fresh configured checkpoint
             |
             v
       checkpoint completed
             |
             v
 apply P/M overrides -> in-place graph restart
             |
             v
 restore RocksDB state and Kafka offsets
             |
             v
 verify RUNNING target -> next 3m + 2m cycle
```

Kafka topics stay live throughout the transaction. The producer container is
paused before the gated checkpoint and resumed after restore verification.
This is a new production-oriented evaluation path; the original
reset-and-replay experiments remain unchanged.

## Git Isolation

- Outer branch: `producer-pause-checkpoint-rescaling`, created from normalized
  `benchmark` commit `9666e588` in the sibling worktree
  `flink-kubernetes-autoscaling-integrated`.
- Justin operator submodule: same branch name, combining benchmark commit
  `e07e163` with the checkpoint transaction from `7aa39d3`, and published
  through the Andrewtangtang fork.
- Runtime prerequisites: retain the Q9 allocator safety changes that expose
  scheduling failures and reject incomplete slot assignments.
- Use benchmark-checkpoint-specific image tags so shared benchmark and
  `justin-modified` images are never overwritten.

## Transaction

Persist one `checkpointRescaleTransaction` in the autoscaler ConfigMap. It
contains the phase, job identity, previous and target overrides, checkpoint
trigger and completed IDs, timestamps, timeouts, handled action nonces, and
the last error.

Phases:

```text
WAITING_CHECKPOINT -> CHECKPOINT_TRIGGERED -> READY_TO_APPLY
 -> APPLYING -> RESTORING -> VERIFYING -> COMPLETED
                                      \-> FAILED
```

While a transaction is active or failed, the autoscaler must not calculate a
new target. Triggering, polling, applying, and verification are idempotent so
an operator restart can resume the same transaction.

Failure is closed: checkpoint, REST, slot assignment, restore, and timeout
errors enter `FAILED`; the operator must not fall back to stateless redeploy.
An increased retry nonce retries the frozen target. Abort is allowed only
before `APPLYING`, when the previous overrides can still be restored safely.

## Runtime Configuration

The evaluation matrix uses:

- complete independent Q4, Q9, Q18, Q19, and Q20 manifests for Justin and DS2;
- the normalized benchmark consumer profile: 8 CPU, 3 GiB, eight slots,
  1264 MiB managed memory, each query's fixed source parallelism, vertex cap
  12, and pipeline maximum 360;
- periodic checkpoint interval `24h`, minimum pause `30s`;
- checkpoint timeout `5m`, maximum concurrent checkpoints `1`;
- `EXACTLY_ONCE`, retained externalized checkpoints, incremental RocksDB;
- explicit per-experiment `RUN_ID` with checkpoint, savepoint, and HA storage
  isolated under
  `/mnt/experiments/autoscaling-experiments/flink-state/runs/${RUN_ID}`;
- Kubernetes HA metadata on the same shared storage;
- `upgradeMode: last-state` for non-scaling recovery;
- checkpoint-rescale timeout `5m` and restore timeout `10m`;
- one-minute bootstrap restart estimate, followed by observed apply-to-RUNNING
  duration with a ten-minute cap and a 30-minute lag catch-up target;
- a 300-million mixed-event producer horizon for every matched policy run,
  with the observer's capacity-stability result as the primary stop condition;
- each policy's existing `3m` stabilization and `2m` metrics window.

The shared path has already been verified as the same NFS mount on c165,
c167, and c182. c153 hosts Kafka and does not read Flink checkpoint state.
Run cleanup is intentionally manual: archive evidence first, stop the
deployment, and then remove the whole run directory rather than individual
incremental-checkpoint files.

## Validation

1. Unit-test transaction persistence, checkpoint gating, reconciliation
   idempotence, retry/abort, timeout, vertical-only changes, and fail-closed
   native scaling.
2. Re-run the slot allocator tests in the Flink runtime fork.
3. Validate all ten manifests and operational scripts without changing the
   replay-based benchmark files. Confirm each rendered manifest contains one unique
   run-scoped checkpoint/savepoint/HA root and no unresolved placeholder.
4. Run matched Justin and DS2 pilots for each query with a continuous producer and verify
   that a completed checkpoint precedes every rescale, the JobManager and
   JobID survive, Kafka is never reset, state restores, and accumulated lag is
   caught up afterward.
5. Archive the transaction ConfigMap, checkpoint statistics, operator/JM
   logs, Kafka lag, and the P/M decision timeline.
6. Verify that checkpoint waiting is excluded from the recorded restart time
   and that later decisions use the observed duration without duplicating the
   real Kafka-lag catch-up term.
