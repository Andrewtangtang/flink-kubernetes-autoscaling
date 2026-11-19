#!/usr/bin/env bash

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  echo "Source this file so the Q9 run configuration remains in the current shell:" >&2
  echo "  source experiments/1726-kafka-q9-unique/run-env.sh" >&2
  exit 1
fi

export KUBECONFIG=/etc/flink-kubernetes-autoscaling/kubeconfig
export TARGET_HOST=c153
export TARGET_IP=142.150.234.153
export HOST_TAG=c153
export PRODUCER_REST_PORT=18081

export PARALLELISM=4
export SLOTS=4
export TM_CORES=16
export DOCKER_CPUS=16
export JM_PROCESS_MEMORY=2048m
export TM_PROCESS_MEMORY=8192m

export TPS=40000
export EVENTS=100000000
export SOURCE_EVENT_SHARE=0.98
export MAX_EMIT_SPEED=false

printf '%s\n' \
  "Loaded Q9 re-evaluation configuration:" \
  "  target=${TARGET_HOST} (${TARGET_IP}) producer_rest_port=${PRODUCER_REST_PORT}" \
  "  parallelism=${PARALLELISM} slots=${SLOTS} tm_cores=${TM_CORES} docker_cpus=${DOCKER_CPUS}" \
  "  jm_memory=${JM_PROCESS_MEMORY} tm_memory=${TM_PROCESS_MEMORY}" \
  "  tps=${TPS} events=${EVENTS} source_event_share=${SOURCE_EVENT_SHARE} max_emit_speed=${MAX_EMIT_SPEED}"
