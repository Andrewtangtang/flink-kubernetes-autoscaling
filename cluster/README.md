# Cluster

Cluster configuration and Kubernetes manifests for the autoscaling stack.
Operational commands live under `scripts/autoscaling/`. Image definitions and
component build entrypoints live under `images/`, with aggregate image commands
under `scripts/images/`.

Contents:

- `config/env.sh`: shared machine, registry, image, kubeconfig, and path settings.
- `monitoring/`: Prometheus, Grafana, Loki, PodMonitor, and dashboard manifests.
- `aws/`: gp3 and one-node AWS monitoring values.
- `operator/`: Flink Kubernetes Operator Helm values.

Machine defaults are in `config/env.sh`. Override them with environment
variables such as `CONTROL_PLANE`, `CONTROL_PLANE_IP`, `WORKER_NODES`, or
`REGISTRY`.

Set `MONITORING_PROFILE=aws` before running `03-deploy-monitoring.sh` to use
the AWS values and gp3 StorageClass. The default remains the lab profile.
