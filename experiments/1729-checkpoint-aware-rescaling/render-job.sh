#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

cd "${REPO_ROOT}"
# shellcheck source=experiments/1729-checkpoint-aware-rescaling/run-env.sh
source "${SCRIPT_DIR}/run-env.sh"

SOURCE_MANIFEST="${SCRIPT_DIR}/jobs/${QUERY}/${POLICY}/experiment.yaml"
OUTPUT="${1:-${RENDERED_MANIFEST}}"

if [[ ! -f "${SOURCE_MANIFEST}" ]]; then
  echo "Missing independent manifest: ${SOURCE_MANIFEST}" >&2
  exit 1
fi

mkdir -p "$(dirname "${OUTPUT}")"
TEMP_OUTPUT="$(mktemp "${OUTPUT}.tmp.XXXXXX")"
trap 'rm -f "${TEMP_OUTPUT}"' EXIT

sed "s|__RUN_ID__|${RUN_ID}|g" "${SOURCE_MANIFEST}" > "${TEMP_OUTPUT}"

if grep -q '__RUN_ID__' "${TEMP_OUTPUT}"; then
  echo "Rendered manifest still contains an unresolved RUN_ID placeholder" >&2
  exit 1
fi

for required_value in \
  "${QUERY}_unique-checkpoint-aware-${POLICY}-${RUN_ID}" \
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

echo "Rendered ${QUERY}/${POLICY} manifest for ${RUN_ID}: ${OUTPUT}"
echo "Checkpoint storage: ${RUN_STORAGE_ROOT}"
