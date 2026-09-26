#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNTIME_DIR="${PORT_FORWARD_RUNTIME_DIR:-${SCRIPT_DIR}/../../../.runtime}"
PORT_FORWARD_ADDRESS="${PORT_FORWARD_ADDRESS:-127.0.0.1}"
PORT_FORWARD_READY_TIMEOUT="${PORT_FORWARD_READY_TIMEOUT:-60}"
FLINK_NAMESPACE="${FLINK_NAMESPACE:-default}"
MONITORING_NAMESPACE="${MONITORING_NAMESPACE:-manager}"
FLINK_LOCAL_PORT="${FLINK_LOCAL_PORT:-18082}"
PROMETHEUS_LOCAL_PORT="${PROMETHEUS_LOCAL_PORT:-19091}"
GRAFANA_LOCAL_PORT="${GRAFANA_LOCAL_PORT:-3001}"
mkdir -p "${RUNTIME_DIR}"

if [[ ! "${PORT_FORWARD_READY_TIMEOUT}" =~ ^[1-9][0-9]*$ ]]; then
  echo "PORT_FORWARD_READY_TIMEOUT must be a positive integer" >&2
  exit 2
fi

owned_pid() {
  local key="$1" pid command
  [[ -f "${RUNTIME_DIR}/${key}.pid" ]] || return 1
  pid="$(<"${RUNTIME_DIR}/${key}.pid")"
  [[ "${pid}" =~ ^[0-9]+$ ]] || return 1
  command="$(ps -p "${pid}" -o args= 2>/dev/null || true)"
  [[ "${command}" == *"kubectl port-forward"*"svc/${key}"* ]]
}

stop_one() {
  local key="$1" pid
  if owned_pid "${key}"; then
    pid="$(<"${RUNTIME_DIR}/${key}.pid")"
    kill "${pid}" 2>/dev/null || true
  fi
  rm -f "${RUNTIME_DIR}/${key}.pid"
}

start_one() {
  local key="$1" namespace="$2" local_port="$3" remote_port="$4" path="$5"
  local pid deadline url
  if ! kubectl get svc -n "${namespace}" "${key}" >/dev/null 2>&1; then
    echo "${key}: service not found in ${namespace}; skipped"
    return 0
  fi
  if owned_pid "${key}"; then
    echo "${key}: already forwarded on http://127.0.0.1:${local_port}"
    return 0
  fi

  url="http://127.0.0.1:${local_port}${path}"
  deadline="$((SECONDS + PORT_FORWARD_READY_TIMEOUT))"
  while (( SECONDS < deadline )); do
    nohup kubectl port-forward --address "${PORT_FORWARD_ADDRESS}" \
      -n "${namespace}" "svc/${key}" "${local_port}:${remote_port}" \
      >"${RUNTIME_DIR}/${key}.log" 2>&1 </dev/null &
    pid="$!"
    printf '%s\n' "${pid}" >"${RUNTIME_DIR}/${key}.pid"
    while kill -0 "${pid}" 2>/dev/null && (( SECONDS < deadline )); do
      if curl --fail --silent --max-time 2 "${url}" >/dev/null 2>&1; then
        echo "${key}: http://127.0.0.1:${local_port} (pid ${pid})"
        return 0
      fi
      sleep 1
    done
    stop_one "${key}"
    wait "${pid}" 2>/dev/null || true
  done
  echo "${key}: port-forward did not become ready; see ${RUNTIME_DIR}/${key}.log" >&2
  return 1
}

case "${1:-start}" in
  start)
    start_one flink-rest "${FLINK_NAMESPACE}" "${FLINK_LOCAL_PORT}" 8081 /jobs/overview
    start_one prom-kube-prometheus-stack-prometheus "${MONITORING_NAMESPACE}" \
      "${PROMETHEUS_LOCAL_PORT}" 9090 /-/ready
    start_one prom-grafana "${MONITORING_NAMESPACE}" "${GRAFANA_LOCAL_PORT}" 80 /api/health
    ;;
  stop)
    stop_one flink-rest
    stop_one prom-kube-prometheus-stack-prometheus
    stop_one prom-grafana
    ;;
  status)
    for key in flink-rest prom-kube-prometheus-stack-prometheus prom-grafana; do
      if owned_pid "${key}"; then
        echo "${key}: pid $(<"${RUNTIME_DIR}/${key}.pid")"
      else
        echo "${key}: not running"
      fi
    done
    ;;
  *)
    echo "Usage: $(basename "$0") [start|stop|status]" >&2
    exit 2
    ;;
esac
