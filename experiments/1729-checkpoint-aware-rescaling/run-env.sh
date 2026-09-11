#!/usr/bin/env bash

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  echo "Source this file so the checkpoint run configuration remains in the current shell:" >&2
  echo "  source experiments/1729-checkpoint-aware-rescaling/run-env.sh" >&2
  exit 1
fi

export QUERY="${QUERY:-q20}"
export POLICY="${POLICY:-justin}"
export EXPERIMENT_PROFILE="${EXPERIMENT_PROFILE:-paper}"

case "${EXPERIMENT_PROFILE}:${QUERY}" in
  paper:q4|paper:q9)
    export TPS=40000
    export SOURCE_EVENT_SHARE=0.98
    ;;
  paper:q18)
    export TPS=97827
    export SOURCE_EVENT_SHARE=0.92
    ;;
  paper:q19)
    export TPS=59783
    export SOURCE_EVENT_SHARE=0.92
    ;;
  paper:q20)
    export TPS=60000
    export SOURCE_EVENT_SHARE=0.98
    ;;
  normalized:q4|normalized:q9|normalized:q20)
    export TPS=56123
    export SOURCE_EVENT_SHARE=0.98
    ;;
  normalized:q18|normalized:q19)
    export TPS=59783
    export SOURCE_EVENT_SHARE=0.92
    ;;
  *)
    echo "EXPERIMENT_PROFILE must be paper or normalized, and QUERY must be q4, q9, q18, q19, or q20" >&2
    return 1 2>/dev/null || exit 1
    ;;
esac

if [[ "${POLICY}" != "justin" && "${POLICY}" != "ds2" ]]; then
  echo "POLICY must be justin or ds2" >&2
  return 1 2>/dev/null || exit 1
fi

if [[ -z "${RUN_ID:-}" ]]; then
  echo "RUN_ID must be set explicitly, for example 20260811-q20-justin-integrated-01" >&2
  return 1 2>/dev/null || exit 1
fi

if [[ ! "${RUN_ID}" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$ ]]; then
  echo "RUN_ID must start with an alphanumeric character and contain only" >&2
  echo "alphanumeric characters, dots, underscores, or hyphens (max 128)" >&2
  return 1 2>/dev/null || exit 1
fi

export KUBECONFIG="${KUBECONFIG:-/etc/flink-kubernetes-autoscaling/kubeconfig}"
export TARGET_HOST="${TARGET_HOST:-c153}"
export TARGET_IP="${TARGET_IP:-142.150.234.153}"
export HOST_TAG="${HOST_TAG:-c153}"
export PRODUCER_REST_PORT="${PRODUCER_REST_PORT:-18081}"

export PARALLELISM="${PARALLELISM:-4}"
export SLOTS="${SLOTS:-4}"
export TM_CORES="${TM_CORES:-16}"
export DOCKER_CPUS="${DOCKER_CPUS:-16}"
export JM_PROCESS_MEMORY="${JM_PROCESS_MEMORY:-2048m}"
export TM_PROCESS_MEMORY="${TM_PROCESS_MEMORY:-8192m}"
export EVENTS="${EVENTS:-300000000}"
export MAX_EMIT_SPEED="${MAX_EMIT_SPEED:-false}"

export RUN_ID
if [[ "${EXPERIMENT_PROFILE}" == "normalized" ]]; then
  export RUN_STORAGE_ROOT="s3://${S3_BUCKET:-__S3_BUCKET__}/flink-state/runs/${RUN_ID}"
  export EXPECTED_JOB_NAME="${QUERY}_unique-normalized-checkpoint-aware-${POLICY}-${RUN_ID}"
else
  export RUN_STORAGE_ROOT="/mnt/experiments/autoscaling-experiments/flink-state/runs/${RUN_ID}"
  export EXPECTED_JOB_NAME="${QUERY}_unique-checkpoint-aware-${POLICY}-${RUN_ID}"
fi
export RENDERED_MANIFEST="/tmp/${QUERY}-${EXPERIMENT_PROFILE}-checkpoint-aware-${RUN_ID}.yaml"
export RUN_RESULTS_DIR="experiments/1729-checkpoint-aware-rescaling/results/${RUN_ID}"

export FLINK_RUNTIME_IMAGE_TAG="benchmark-checkpoint-rescale"
export FLINK_BENCHMARK_IMAGE_TAG="benchmark-checkpoint-rescale"
export OPERATOR_IMAGE_TAG="benchmark-checkpoint-rescale"
export OPERATOR_SOURCE_DIR="sources/operators/flink-kubernetes-operator-justin"

# shellcheck source=scripts/env.sh
source scripts/env.sh

echo "Loaded checkpoint-aware ${QUERY} configuration:"
echo "  runtime=${FLINK_RUNTIME_IMAGE}"
echo "  benchmark=${FLINK_BENCHMARK_IMAGE}"
echo "  operator=${OPERATOR_IMAGE}"
echo "  policy=${POLICY} profile=${EXPERIMENT_PROFILE} run_id=${RUN_ID}"
echo "  storage=${RUN_STORAGE_ROOT}"
echo "  manifest=${RENDERED_MANIFEST}"
echo "  workload=${TPS} events/s, ${EVENTS} events, source_share=${SOURCE_EVENT_SHARE}"
