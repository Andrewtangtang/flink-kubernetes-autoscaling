#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../../cluster/config/env.sh"

ACTION="${1:-start}"
RUNTIME_DIR="${CLUSTER_ROOT}/.runtime"
PORT_FORWARD_ADDRESS="${PORT_FORWARD_ADDRESS:-0.0.0.0}"
GRAFANA_LOCAL_PORT="${GRAFANA_LOCAL_PORT:-3001}"
PROMETHEUS_LOCAL_PORT="${PROMETHEUS_LOCAL_PORT:-19091}"
FLINK_LOCAL_PORT="${FLINK_LOCAL_PORT:-18082}"
PORT_FORWARD_READY_TIMEOUT="${PORT_FORWARD_READY_TIMEOUT:-60}"
mkdir -p "${RUNTIME_DIR}"

if [[ ! "${PORT_FORWARD_READY_TIMEOUT}" =~ ^[1-9][0-9]*$ ]]; then
  echo "PORT_FORWARD_READY_TIMEOUT must be a positive integer" >&2
  exit 2
fi

pid_file() {
  printf '%s/%s-port-forward.pid\n' "${RUNTIME_DIR}" "$1"
}

stop_owned() {
  local name="$1"
  local file pid process
  file="$(pid_file "${name}")"

  if [[ ! -f "${file}" ]]; then
    return 0
  fi

  pid="$(cat "${file}")"
  if [[ "${pid}" =~ ^[0-9]+$ ]] && kill -0 "${pid}" 2>/dev/null; then
    process="$(ps -p "${pid}" -o args= 2>/dev/null || true)"
    if [[ "${process}" == *"kubectl port-forward"* ]]; then
      kill "${pid}" 2>/dev/null || true
    else
      echo "Ignoring stale ${name} PID ${pid}: ${process:-unknown process}" >&2
    fi
  fi

  rm -f "${file}"
}

stop_all_owned() {
  stop_owned grafana
  stop_owned prometheus
  stop_owned flink
}

start_and_wait() {
  local key="$1"
  local name="$2"
  local url="$3"
  local log_file="$4"
  local file deadline pid
  shift 4

  if curl --fail --silent --max-time 2 "${url}" >/dev/null 2>&1; then
    echo "${name}: already ready"
    return 0
  fi

  file="$(pid_file "${key}")"
  deadline="$((SECONDS + PORT_FORWARD_READY_TIMEOUT))"
  : > "${log_file}"

  while (( SECONDS < deadline )); do
    nohup "$@" </dev/null >>"${log_file}" 2>&1 &
    pid="$!"
    printf '%s\n' "${pid}" > "${file}"

    while kill -0 "${pid}" 2>/dev/null && (( SECONDS < deadline )); do
      if curl --fail --silent --max-time 2 "${url}" >/dev/null 2>&1; then
        echo "${name}: ready (pid ${pid})"
        return 0
      fi
      sleep 1
    done

    if kill -0 "${pid}" 2>/dev/null; then
      kill "${pid}" 2>/dev/null || true
    fi
    wait "${pid}" 2>/dev/null || true
    rm -f "${file}"
    sleep 1
  done

  echo "${name} port-forward did not become ready in ${PORT_FORWARD_READY_TIMEOUT}s" >&2
  tail -n 20 "${log_file}" >&2 || true
  return 1
}

show_owned() {
  local name="$1"
  local file pid process
  file="$(pid_file "${name}")"

  if [[ ! -f "${file}" ]]; then
    echo "${name}: not managed by this helper"
    return 0
  fi

  pid="$(cat "${file}")"
  process="$(ps -p "${pid}" -o args= 2>/dev/null || true)"
  if [[ -n "${process}" ]]; then
    echo "${name}: pid ${pid} ${process}"
  else
    echo "${name}: stale pid ${pid}"
  fi
}

case "${ACTION}" in
  start)
    if ! kubectl get svc flink-rest >/dev/null 2>&1; then
      echo "Flink REST service is not available; wait for the job before forwarding" >&2
      exit 1
    fi

    stop_all_owned

    if ! start_and_wait \
      grafana \
      Grafana \
      "http://127.0.0.1:${GRAFANA_LOCAL_PORT}/api/health" \
      "${RUNTIME_DIR}/grafana-port-forward.log" \
      kubectl port-forward --address "${PORT_FORWARD_ADDRESS}" \
      -n manager svc/prom-grafana "${GRAFANA_LOCAL_PORT}:80"; then
      stop_all_owned
      exit 1
    fi

    if ! start_and_wait \
      prometheus \
      Prometheus \
      "http://127.0.0.1:${PROMETHEUS_LOCAL_PORT}/-/ready" \
      "${RUNTIME_DIR}/prometheus-port-forward.log" \
      kubectl port-forward --address "${PORT_FORWARD_ADDRESS}" \
      -n manager svc/prom-kube-prometheus-stack-prometheus \
      "${PROMETHEUS_LOCAL_PORT}:9090"; then
      stop_all_owned
      exit 1
    fi

    if ! start_and_wait \
      flink \
      "Flink REST" \
      "http://127.0.0.1:${FLINK_LOCAL_PORT}/jobs/overview" \
      "${RUNTIME_DIR}/flink-port-forward.log" \
      kubectl port-forward --address "${PORT_FORWARD_ADDRESS}" \
      svc/flink-rest "${FLINK_LOCAL_PORT}:8081"; then
      stop_all_owned
      exit 1
    fi

    echo "Flink UI:   http://${CONTROL_PLANE_IP}:${FLINK_LOCAL_PORT}"
    echo "Grafana:    http://${CONTROL_PLANE_IP}:${GRAFANA_LOCAL_PORT}"
    echo "Prometheus: http://${CONTROL_PLANE_IP}:${PROMETHEUS_LOCAL_PORT}"
    ;;
  stop)
    stop_all_owned
    ;;
  status)
    echo "Flink UI:   http://${CONTROL_PLANE_IP}:${FLINK_LOCAL_PORT}"
    echo "Grafana:    http://${CONTROL_PLANE_IP}:${GRAFANA_LOCAL_PORT}"
    echo "Prometheus: http://${CONTROL_PLANE_IP}:${PROMETHEUS_LOCAL_PORT}"
    show_owned flink
    show_owned grafana
    show_owned prometheus
    ;;
  *)
    echo "Usage: $(basename "$0") [start|stop|status]" >&2
    exit 2
    ;;
esac
