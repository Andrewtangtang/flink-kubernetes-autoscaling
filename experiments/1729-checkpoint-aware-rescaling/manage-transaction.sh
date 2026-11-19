#!/usr/bin/env bash

set -euo pipefail

DEPLOYMENT="${DEPLOYMENT:-flink}"
NAMESPACE="${NAMESPACE:-default}"
ACTION="${1:-status}"

RETRY_ANNOTATION="autoscaling.flink.apache.org/checkpoint-rescale-retry-nonce"
ABORT_ANNOTATION="autoscaling.flink.apache.org/checkpoint-rescale-abort-nonce"
PHASE_ANNOTATION="autoscaling.flink.apache.org/checkpoint-rescale-phase"

annotation_value() {
  local annotation="$1"
  kubectl get flinkdeployment "${DEPLOYMENT}" \
    --namespace "${NAMESPACE}" \
    -o json \
    | jq -r --arg annotation "${annotation}" \
        '.metadata.annotations[$annotation] // "0"'
}

increment_annotation() {
  local annotation="$1"
  local current next
  current="$(annotation_value "${annotation}")"
  next="$((current + 1))"
  kubectl annotate flinkdeployment "${DEPLOYMENT}" \
    --namespace "${NAMESPACE}" \
    "${annotation}=${next}" \
    --overwrite
  echo "${annotation}=${next}"
}

case "${ACTION}" in
  status)
    echo "phase=$(annotation_value "${PHASE_ANNOTATION}")"
    kubectl get configmap "autoscaler-${DEPLOYMENT}" \
      --namespace "${NAMESPACE}" \
      -o json \
      | jq -r '.data.checkpointRescaleTransaction // "No transaction"'
    ;;
  retry)
    increment_annotation "${RETRY_ANNOTATION}"
    ;;
  abort)
    increment_annotation "${ABORT_ANNOTATION}"
    ;;
  *)
    echo "Usage: $0 {status|retry|abort}" >&2
    exit 2
    ;;
esac
