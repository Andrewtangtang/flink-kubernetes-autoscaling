#!/usr/bin/env bash

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  echo "Source this file after setting QUERY to q4, q9, q18, q19, or q20." >&2
  exit 1
fi

case "${QUERY:-}" in
  q4|q9)
    export TPS=40000
    ;;
  q18)
    export TPS=97827
    ;;
  q19)
    export TPS=59783
    ;;
  q20)
    export TPS=60000
    ;;
  *)
    echo "QUERY must be q4, q9, q18, q19, or q20." >&2
    return 1
    ;;
esac

export PARALLELISM=4 SLOTS=4 TM_CORES=16 DOCKER_CPUS=16
export JM_PROCESS_MEMORY=2048m TM_PROCESS_MEMORY=8192m
export PRODUCER_REST_PORT=18081 EVENTS=100000000 MAX_EMIT_SPEED=false

printf 'Loaded %s: producer=%s events/s, events=%s\n' \
  "${QUERY}" "${TPS}" "${EVENTS}"
