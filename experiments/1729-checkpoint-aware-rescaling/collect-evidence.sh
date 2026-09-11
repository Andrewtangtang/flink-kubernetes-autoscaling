#!/usr/bin/env bash

set -euo pipefail

DEPLOYMENT="${DEPLOYMENT:-flink}"
NAMESPACE="${NAMESPACE:-default}"
FLINK_URL="${FLINK_URL:-http://localhost:${FLINK_LOCAL_PORT:-18082}}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

cd "${REPO_ROOT}"
# shellcheck source=experiments/1729-checkpoint-aware-rescaling/run-env.sh
source "${SCRIPT_DIR}/run-env.sh"

OUTPUT_DIR="${1:-${RUN_RESULTS_DIR}}"

mkdir -p "${OUTPUT_DIR}"

if [[ -f "${RENDERED_MANIFEST}" ]]; then
  cp "${RENDERED_MANIFEST}" "${OUTPUT_DIR}/rendered-manifest.yaml"
fi

{
  echo "run_id=${RUN_ID}"
  echo "query=${QUERY}"
  echo "policy=${POLICY}"
  echo "profile=${EXPERIMENT_PROFILE}"
  echo "tps=${TPS}"
  echo "events=${EVENTS}"
  echo "source_event_share=${SOURCE_EVENT_SHARE}"
  echo "storage_root=${RUN_STORAGE_ROOT}"
  echo "captured_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} > "${OUTPUT_DIR}/run-metadata.txt"

kubectl get flinkdeployment "${DEPLOYMENT}" \
  --namespace "${NAMESPACE}" \
  -o yaml > "${OUTPUT_DIR}/flinkdeployment.yaml"

if [[ "${EXPERIMENT_PROFILE}" == "normalized" ]]; then
  EXPECTED_CHECKPOINT_URI="${RUN_STORAGE_ROOT}/checkpoints"
else
  EXPECTED_CHECKPOINT_URI="file://${RUN_STORAGE_ROOT}/checkpoints"
fi
if ! grep -Fq "${EXPECTED_CHECKPOINT_URI}" "${OUTPUT_DIR}/flinkdeployment.yaml"; then
  echo "Deployment checkpoint storage does not match RUN_ID=${RUN_ID}" >&2
  echo "Expected ${EXPECTED_CHECKPOINT_URI}" >&2
  exit 1
fi

if ! grep -Fq "${EXPECTED_JOB_NAME}" "${OUTPUT_DIR}/flinkdeployment.yaml"; then
  echo "Deployment job name does not match QUERY=${QUERY}, POLICY=${POLICY}, RUN_ID=${RUN_ID}" >&2
  echo "Expected ${EXPECTED_JOB_NAME}" >&2
  exit 1
fi

kubectl get configmap "autoscaler-${DEPLOYMENT}" \
  --namespace "${NAMESPACE}" \
  -o yaml > "${OUTPUT_DIR}/autoscaler-configmap.yaml"
kubectl get pods --namespace "${NAMESPACE}" -o wide \
  > "${OUTPUT_DIR}/pods.txt"
kubectl get events --namespace "${NAMESPACE}" --sort-by=.lastTimestamp \
  > "${OUTPUT_DIR}/events.txt"

"${REPO_ROOT}/experiments/1724-kafka-q20-unique/external-kafka/run/manage-standalone-producer.sh" \
  status > "${OUTPUT_DIR}/producer-status.txt" 2>&1 || true

curl --fail --silent --show-error "${FLINK_URL}/jobs/overview" \
  > "${OUTPUT_DIR}/jobs-overview.json"
JOB_ID="$(jq -r \
  '[.jobs[] | select(.state != "FINISHED" and .state != "CANCELED")][0].jid // ""' \
  "${OUTPUT_DIR}/jobs-overview.json")"
if [[ -n "${JOB_ID}" ]]; then
  curl --fail --silent --show-error "${FLINK_URL}/jobs/${JOB_ID}" \
    > "${OUTPUT_DIR}/job-details.json"
  curl --fail --silent --show-error "${FLINK_URL}/jobs/${JOB_ID}/checkpoints" \
    > "${OUTPUT_DIR}/checkpoints.json"
fi

OPERATOR_POD="$(kubectl get pods --namespace "${NAMESPACE}" \
  -l app.kubernetes.io/name=flink-kubernetes-operator \
  -o jsonpath='{.items[0].metadata.name}')"
JM_POD="$(kubectl get pods --namespace "${NAMESPACE}" \
  -l app=flink,component=jobmanager \
  -o jsonpath='{.items[0].metadata.name}')"
kubectl logs "${OPERATOR_POD}" --namespace "${NAMESPACE}" --timestamps \
  > "${OUTPUT_DIR}/operator.log"
kubectl logs "${JM_POD}" --namespace "${NAMESPACE}" --timestamps \
  > "${OUTPUT_DIR}/jobmanager.log"

echo "Checkpoint rescale evidence written to ${OUTPUT_DIR}"
