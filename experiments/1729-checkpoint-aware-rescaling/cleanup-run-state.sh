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

# Flink writes this tree as the uid/gid of its container user, and every
# directory is mode 755. Removing an entry needs write permission on its parent
# directory, so an interactive account can only delete the tree by first
# assuming the owning uid/gid. Both default to the current owner of TARGET and
# can be overridden when the tree predates a uid change.
resolve_state_owner() {
  if [[ -z "${STATE_UID:-}" ]]; then
    STATE_UID="$(stat -c '%u' -- "${TARGET}")"
  fi
  if [[ -z "${STATE_GID:-}" ]]; then
    STATE_GID="$(stat -c '%g' -- "${TARGET}")"
  fi

  if [[ ! "${STATE_UID}" =~ ^[0-9]+$ || ! "${STATE_GID}" =~ ^[0-9]+$ ]]; then
    echo "Invalid state owner: uid=${STATE_UID} gid=${STATE_GID}" >&2
    exit 1
  fi
}

remove_run_state() {
  if [[ "$(id -u)" == "${STATE_UID}" && "$(id -g)" == "${STATE_GID}" ]]; then
    rm -rf --one-file-system -- "${TARGET}"
    return
  fi

  # setpriv rather than sudo -u '#<uid>': the state owner has no passwd entry
  # on the cluster hosts, so sudo rejects it as an unknown user. Dropping
  # straight to the owning uid also keeps the removal unable to touch anything
  # outside the tree it owns.
  if ! command -v setpriv >/dev/null 2>&1; then
    echo "setpriv is required to delete state owned by uid ${STATE_UID}" >&2
    exit 1
  fi
  if ! sudo -n true 2>/dev/null; then
    echo "Passwordless sudo is required to assume uid ${STATE_UID}" >&2
    exit 1
  fi

  sudo -n setpriv \
    --reuid="${STATE_UID}" \
    --regid="${STATE_GID}" \
    --clear-groups \
    rm -rf --one-file-system -- "${TARGET}"
}

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
    resolve_state_owner
    echo "state_owner=${STATE_UID}:${STATE_GID}"
    remove_run_state

    if [[ -e "${TARGET}" ]]; then
      echo "Failed to remove ${TARGET}; it may be partially deleted." >&2
      echo "Do not restore from it. Re-run this command or use a new RUN_ID." >&2
      exit 1
    fi
    echo "Deleted complete run state: ${TARGET}"
    ;;
  *)
    echo "Usage: RUN_ID=<id> $0 {status|delete --confirm-run-id <id>}" >&2
    exit 2
    ;;
esac
