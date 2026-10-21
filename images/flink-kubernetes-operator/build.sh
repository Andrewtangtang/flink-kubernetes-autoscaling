#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../scripts/env.sh"

DRY_RUN=false
NO_CACHE="${NO_CACHE:-false}"
LOCAL_IMAGE_OVERRIDE=""

usage() {
  cat <<USAGE
Usage: $(basename "$0") [--source-dir DIR] [--image-name NAME] [--tag TAG] [--local-image IMAGE] [--no-cache] [--dry-run] [--help]

Builds ${OPERATOR_LOCAL} from ${OPERATOR_SOURCE_DIR}.

  --source-dir DIR    operator source tree containing the operator Dockerfile
  --image-name NAME   local image repository name
  --tag TAG           local image tag
  --local-image IMAGE full local image reference; overrides --image-name/--tag
  --no-cache          pass Docker --no-cache
  --dry-run           print the Docker command only
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true ;;
    --no-cache) NO_CACHE=true ;;
    --source-dir) OPERATOR_SOURCE_DIR="$2"; shift ;;
    --image-name) OPERATOR_IMAGE_NAME="$2"; shift ;;
    --tag) OPERATOR_IMAGE_TAG="$2"; shift ;;
    --local-image) LOCAL_IMAGE_OVERRIDE="$2"; shift ;;
    --help|-h) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
  esac
  shift
done

if [[ -n "${LOCAL_IMAGE_OVERRIDE}" ]]; then
  OPERATOR_LOCAL="${LOCAL_IMAGE_OVERRIDE}"
else
  OPERATOR_LOCAL="${OPERATOR_IMAGE_NAME}:${OPERATOR_IMAGE_TAG}"
fi
OPERATOR_SOURCE="${REPO_ROOT}/${OPERATOR_SOURCE_DIR}"

cmd=(docker build)
if [[ "${NO_CACHE}" == "true" ]]; then
  cmd+=(--no-cache)
fi
# The A4S operator Dockerfile installs flink-a4s from the Flink runtime source first
cmd+=(--build-context "flink-src=${FLINK_RUNTIME_SOURCE}")
cmd+=("${OPERATOR_SOURCE}" -t "${OPERATOR_LOCAL}")

echo "Building ${OPERATOR_LOCAL}"
printf 'Command:'
printf ' %q' "${cmd[@]}"
printf '\n'

if [[ "${DRY_RUN}" == "false" ]]; then
  "${cmd[@]}"
fi
