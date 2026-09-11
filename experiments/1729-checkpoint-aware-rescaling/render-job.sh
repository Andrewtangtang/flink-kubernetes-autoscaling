#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

cd "${REPO_ROOT}"
# shellcheck source=experiments/1729-checkpoint-aware-rescaling/run-env.sh
source "${SCRIPT_DIR}/run-env.sh"

if [[ "${EXPERIMENT_PROFILE}" == "normalized" ]]; then
  SOURCE_MANIFEST="${SCRIPT_DIR}/jobs-normalized/${QUERY}/${POLICY}/experiment.yaml"
  if [[ -z "${S3_BUCKET:-}" || ! "${S3_BUCKET}" =~ ^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$ ]]; then
    echo "S3_BUCKET must be a valid bucket name for the normalized profile" >&2
    exit 1
  fi
  if [[ ! "${KAFKA_BOOTSTRAP:-}" =~ ^[A-Za-z0-9.-]+:[0-9]{1,5}$ ]]; then
    echo "KAFKA_BOOTSTRAP must be a host:port value" >&2
    exit 1
  fi
  if [[ ! "${FLINK_BENCHMARK_IMAGE}" =~ \.dkr\.ecr\.[a-z0-9-]+\.amazonaws\.com/ ]]; then
    echo "FLINK_BENCHMARK_IMAGE must point to ECR for the normalized profile" >&2
    exit 1
  fi
else
  SOURCE_MANIFEST="${SCRIPT_DIR}/jobs/${QUERY}/${POLICY}/experiment.yaml"
fi
OUTPUT="${1:-${RENDERED_MANIFEST}}"

if [[ ! -f "${SOURCE_MANIFEST}" ]]; then
  echo "Missing independent manifest: ${SOURCE_MANIFEST}" >&2
  exit 1
fi

mkdir -p "$(dirname "${OUTPUT}")"
TEMP_OUTPUT="$(mktemp "${OUTPUT}.tmp.XXXXXX")"
trap 'rm -f "${TEMP_OUTPUT}"' EXIT

sed \
  -e "s|__RUN_ID__|${RUN_ID}|g" \
  -e "s|__FLINK_BENCHMARK_IMAGE__|${FLINK_BENCHMARK_IMAGE}|g" \
  -e "s|__S3_BUCKET__|${S3_BUCKET:-}|g" \
  -e "s|__KAFKA_BOOTSTRAP__|${KAFKA_BOOTSTRAP:-}|g" \
  "${SOURCE_MANIFEST}" > "${TEMP_OUTPUT}"

if grep -Eq '__[A-Z0-9_]+__' "${TEMP_OUTPUT}"; then
  echo "Rendered manifest still contains an unresolved placeholder" >&2
  exit 1
fi

for required_value in \
  "${EXPECTED_JOB_NAME}" \
  "${RUN_STORAGE_ROOT}/checkpoints" \
  "${RUN_STORAGE_ROOT}/savepoints" \
  "${RUN_STORAGE_ROOT}/ha"; do
  if ! grep -Fq "${required_value}" "${TEMP_OUTPUT}"; then
    echo "Rendered manifest is missing ${required_value}" >&2
    exit 1
  fi
done

mv "${TEMP_OUTPUT}" "${OUTPUT}"
trap - EXIT

echo "Rendered ${EXPERIMENT_PROFILE}/${QUERY}/${POLICY} manifest for ${RUN_ID}: ${OUTPUT}"
echo "Checkpoint storage: ${RUN_STORAGE_ROOT}"
