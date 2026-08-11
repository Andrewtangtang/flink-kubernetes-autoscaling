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
mkdir -p "${RUNTIME_DIR}"

stop_matching() {
  pkill -f "kubectl port-forward.*$1" 2>/dev/null || true
}

case "${ACTION}" in
  start)
    stop_matching "prom-grafana"
    stop_matching "prom-kube-prometheus-stack-prometheus"
    stop_matching "flink-rest"

    kubectl port-forward --address "${PORT_FORWARD_ADDRESS}" \
      -n manager svc/prom-grafana "${GRAFANA_LOCAL_PORT}:80" \
      >"${RUNTIME_DIR}/grafana-port-forward.log" 2>&1 &
    kubectl port-forward --address "${PORT_FORWARD_ADDRESS}" \
      -n manager svc/prom-kube-prometheus-stack-prometheus \
      "${PROMETHEUS_LOCAL_PORT}:9090" \
      >"${RUNTIME_DIR}/prometheus-port-forward.log" 2>&1 &

    if kubectl get svc flink-rest >/dev/null 2>&1; then
      kubectl port-forward --address "${PORT_FORWARD_ADDRESS}" \
        svc/flink-rest "${FLINK_LOCAL_PORT}:8081" \
        >"${RUNTIME_DIR}/flink-port-forward.log" 2>&1 &
      echo "Flink UI:   http://${CONTROL_PLANE_IP}:${FLINK_LOCAL_PORT}"
    else
      echo "Flink UI:   no flink-rest service"
    fi

    echo "Grafana:    http://${CONTROL_PLANE_IP}:${GRAFANA_LOCAL_PORT}"
    echo "Prometheus: http://${CONTROL_PLANE_IP}:${PROMETHEUS_LOCAL_PORT}"
    ;;
  stop)
    stop_matching "prom-grafana"
    stop_matching "prom-kube-prometheus-stack-prometheus"
    stop_matching "flink-rest"
    ;;
  status)
    echo "Flink UI:   http://${CONTROL_PLANE_IP}:${FLINK_LOCAL_PORT}"
    echo "Grafana:    http://${CONTROL_PLANE_IP}:${GRAFANA_LOCAL_PORT}"
    echo "Prometheus: http://${CONTROL_PLANE_IP}:${PROMETHEUS_LOCAL_PORT}"
    pgrep -af "kubectl port-forward" || true
    ;;
  *)
    echo "Usage: $(basename "$0") [start|stop|status]" >&2
    exit 2
    ;;
esac
