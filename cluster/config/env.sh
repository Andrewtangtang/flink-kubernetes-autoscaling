#!/usr/bin/env bash

# This file is a sourced environment library. Preserve the caller's shell
# options; executable callers enable their own strict mode before sourcing it.

export CLUSTER_CONFIG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export CLUSTER_ROOT="$(cd "${CLUSTER_CONFIG_DIR}/.." && pwd)"
export REPO_ROOT="$(cd "${CLUSTER_ROOT}/.." && pwd)"
_REGISTRY_OVERRIDE="${REGISTRY:-}"

# Image names and registry defaults live with the image build scripts.
# Override REGISTRY before sourcing this file if the control-plane registry changes.
source "${REPO_ROOT}/scripts/env.sh"

export CONTROL_PLANE="${CONTROL_PLANE:-c165}"
export CONTROL_PLANE_IP="${CONTROL_PLANE_IP:-142.150.234.165}"
WORKER_NODES="${WORKER_NODES:-${WORKERS:-c182 c167}}"
read -r -a WORKERS <<< "${WORKER_NODES}"
ALL_NODES=("${CONTROL_PLANE}" "${WORKERS[@]}")

export REGISTRY="${_REGISTRY_OVERRIDE:-${CONTROL_PLANE_IP}:5000}"
export FLINK_RUNTIME_IMAGE="${REGISTRY}/${FLINK_RUNTIME_LOCAL}"
export FLINK_BENCHMARK_IMAGE="${REGISTRY}/${FLINK_BENCHMARK_LOCAL}"
export OPERATOR_IMAGE="${REGISTRY}/${OPERATOR_LOCAL}"

export CRI_RUNTIME_ENDPOINT="${CRI_RUNTIME_ENDPOINT:-unix:///run/containerd/containerd.sock}"
export SHARED_KUBECONFIG="${SHARED_KUBECONFIG:-/etc/flink-kubernetes-autoscaling/kubeconfig}"
export KUBECONFIG_GROUP="${KUBECONFIG_GROUP:-users}"

export HELM_CHART="${HELM_CHART:-${OPERATOR_SOURCE}/helm/flink-kubernetes-operator}"
export OPERATOR_VALUES="${OPERATOR_VALUES:-${CLUSTER_ROOT}/operator/values.yaml}"
export MONITORING_MANIFESTS="${MONITORING_MANIFESTS:-${CLUSTER_ROOT}/monitoring}"

export EXPERIMENTS_ROOT="${EXPERIMENTS_ROOT:-/mnt/experiments/autoscaling-experiments}"
export CHECKPOINT_HOST_PATH="${CHECKPOINT_HOST_PATH:-${EXPERIMENTS_ROOT}/flink-state}"
export CHECKPOINT_MOUNT_PATH="${CHECKPOINT_MOUNT_PATH:-${EXPERIMENTS_ROOT}/flink-state}"
export CHECKPOINT_DIR="${CHECKPOINT_DIR:-file://${CHECKPOINT_MOUNT_PATH}/checkpoints}"
export SAVEPOINT_DIR="${SAVEPOINT_DIR:-file://${CHECKPOINT_MOUNT_PATH}/savepoints}"

export PROM_CHART_VERSION="${PROM_CHART_VERSION:-30.0.2}"
export LOKI_CHART_VERSION="${LOKI_CHART_VERSION:-2.10.2}"

if [[ -z "${KUBECONFIG:-}" ]]; then
  if [[ -r "${SHARED_KUBECONFIG}" ]]; then
    export KUBECONFIG="${SHARED_KUBECONFIG}"
  elif [[ -r /etc/kubernetes/admin.conf ]]; then
    export KUBECONFIG=/etc/kubernetes/admin.conf
  fi
fi
