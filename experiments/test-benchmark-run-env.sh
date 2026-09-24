#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

for case in q4:40000 q9:40000 q18:97827 q19:59783 q20:60000; do
  query="${case%%:*}"
  expected="${case#*:}"
  actual="$(QUERY="${query}" bash -c 'source experiments/benchmark-run-env.sh >/dev/null; printf "%s %s %s" "$TPS" "$EVENTS" "$PARALLELISM"')"
  [[ "${actual}" == "${expected} 100000000 4" ]] || {
    echo "Unexpected ${query} configuration: ${actual}" >&2
    exit 1
  }
done

if QUERY=invalid bash -c 'source experiments/benchmark-run-env.sh' >/dev/null 2>&1; then
  echo "Invalid query was accepted" >&2
  exit 1
fi
