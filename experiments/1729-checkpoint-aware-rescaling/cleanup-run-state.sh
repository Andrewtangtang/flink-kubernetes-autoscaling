#!/usr/bin/env bash

set -euo pipefail

ACTION="${1:-status}"
RUNS_ROOT="/mnt/experiments/autoscaling-experiments/flink-state/runs"
KUBECONFIG="${KUBECONFIG:-/etc/flink-kubernetes-autoscaling/kubeconfig}"

if [[ -z "${RUN_ID:-}" ]]; then
  echo "RUN_ID must be set explicitly" >&2
  exit 2
fi

if [[ ! "${RUN_ID}" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$ ]]; then
  echo "Invalid RUN_ID: ${RUN_ID}" >&2
  exit 2
fi

TARGET="${RUNS_ROOT}/${RUN_ID}"

show_status() {
  echo "run_id=${RUN_ID}"
  echo "target=${TARGET}"
  if [[ -e "${TARGET}" ]]; then
    if [[ -L "${TARGET}" ]]; then
      echo "Refusing to inspect symlinked run directory: ${TARGET}" >&2
      exit 1
    fi
    du -sh -- "${TARGET}"
    find "${TARGET}" -mindepth 1 -maxdepth 2 -type d -print | sort
  else
    echo "state=absent"
  fi
}

assert_cluster_idle() {
  local deployments mount_type pods

  if ! mount_type="$(findmnt --noheadings --output FSTYPE --target "${TARGET}")"; then
    echo "Cannot verify the filesystem containing ${TARGET}" >&2
    exit 1
  fi
  case "${mount_type}" in
    nfs|nfs4) ;;
    *)
      echo "Refusing deletion from unexpected filesystem type: ${mount_type}" >&2
      exit 1
      ;;
  esac

  if [[ ! -r "${KUBECONFIG}" ]]; then
    echo "Cannot verify cluster state; unreadable KUBECONFIG=${KUBECONFIG}" >&2
    exit 1
  fi

  if ! deployments="$(kubectl get flinkdeployments --all-namespaces -o name)"; then
    echo "Cannot verify FlinkDeployments; refusing state deletion" >&2
    exit 1
  fi
  if [[ -n "${deployments}" ]]; then
    echo "FlinkDeployment resources still exist; stop them before cleanup:" >&2
    printf '%s\n' "${deployments}" >&2
    exit 1
  fi

  if ! pods="$(kubectl get pods --all-namespaces -l app=flink -o name)"; then
    echo "Cannot verify Flink pods; refusing state deletion" >&2
    exit 1
  fi
  if [[ -n "${pods}" ]]; then
    echo "Flink pods still exist; wait for deletion before cleanup:" >&2
    printf '%s\n' "${pods}" >&2
    exit 1
  fi
}

case "${ACTION}" in
  status)
    show_status
    ;;
  delete)
    if [[ "${2:-}" != "--confirm-run-id" || "${3:-}" != "${RUN_ID}" ]]; then
      echo "Deletion requires an exact RUN_ID confirmation:" >&2
      echo "  $0 delete --confirm-run-id \"\${RUN_ID}\"" >&2
      exit 2
    fi

    show_status
    if [[ ! -e "${TARGET}" ]]; then
      echo "Nothing to delete"
      exit 0
    fi

    assert_cluster_idle
    rm -rf --one-file-system -- "${TARGET}"

    if [[ -e "${TARGET}" ]]; then
      echo "Failed to remove ${TARGET}" >&2
      exit 1
    fi
    echo "Deleted complete run state: ${TARGET}"
    ;;
  *)
    echo "Usage: RUN_ID=<id> $0 {status|delete --confirm-run-id <id>}" >&2
    exit 2
    ;;
esac
