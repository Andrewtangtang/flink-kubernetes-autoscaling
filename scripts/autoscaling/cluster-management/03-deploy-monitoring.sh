#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../../cluster/config/env.sh"

kubectl apply -f "${MONITORING_MANIFESTS}/cluster-role-binding-default.yaml"
kubectl apply -f https://github.com/jetstack/cert-manager/releases/download/v1.5.3/cert-manager.yaml
kubectl rollout status deployment/cert-manager-webhook -n cert-manager --timeout=120s

kubectl create namespace manager --dry-run=client -o yaml | kubectl apply -f -
if [[ -n "${STORAGE_CLASS_MANIFEST}" ]]; then
  kubectl apply -f "${STORAGE_CLASS_MANIFEST}"
else
  kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.21/deploy/local-path-storage.yaml
fi

helm repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null 2>&1 || true
helm repo add grafana https://grafana.github.io/helm-charts >/dev/null 2>&1 || true
helm repo update

helm upgrade --install prom prometheus-community/kube-prometheus-stack \
  --namespace manager \
  --version "${PROM_CHART_VERSION}" \
  -f "${PROM_VALUES}"

kubectl apply -f "${MONITORING_MANIFESTS}/pod-monitor.yaml"

helm upgrade --install loki grafana/loki-stack \
  --namespace manager \
  --version "${LOKI_CHART_VERSION}" \
  -f "${LOKI_VALUES}" \
  --set loki.podSecurityPolicy.enabled=false \
  --set promtail.podSecurityPolicy.enabled=false

kubectl create configmap grafana-dashboard-justin \
  --from-file=justin-dashboard.json="${MONITORING_MANIFESTS}/grafana-justin-dashboard.json" \
  -n manager --dry-run=client -o yaml | kubectl apply -f -
kubectl label configmap grafana-dashboard-justin grafana_dashboard=1 -n manager --overwrite

kubectl create configmap grafana-dashboard-autoscaling \
  --from-file=autoscaling-dashboard.json="${MONITORING_MANIFESTS}/grafana-autoscaling-dashboard.json" \
  -n manager --dry-run=client -o yaml | kubectl apply -f -
kubectl label configmap grafana-dashboard-autoscaling grafana_dashboard=1 -n manager --overwrite

kubectl rollout restart deployment/prom-grafana -n manager 2>/dev/null || true
kubectl get pods -n manager
