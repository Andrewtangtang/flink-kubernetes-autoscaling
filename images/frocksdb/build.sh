#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../scripts/env.sh"

IMAGE_NAME="rocksdb-builder-jammy"

# This version of rocksdb must be built on Ubuntu 22.04 due to dependency on glibc 2.35
# We use a lightweight Docker image to ensure consistency
docker build -t ${IMAGE_NAME} -f ${REPO_ROOT}/images/frocksdb/Dockerfile .
docker run --rm \
    -v "${FROCKSDB_SOURCE}:/rocksdb-host:ro" \
    -v "${FROCKSDB_SOURCE}/java/target:/rocksdb-java-target" \
    -e DEBUG_LEVEL=0 \
    --entrypoint /rocksdb-host/java/crossbuild/docker-build-linux-ubuntu.sh \
    ${IMAGE_NAME}

cp ${FROCKSDB_SOURCE}/java/target/rocksdbjni-*-linux*.jar "${FLINK_RUNTIME_A4S_SOURCE}/"
cp ${FROCKSDB_SOURCE}/java/target/rocksdbjni-*-linux*.jar "${FLINK_RUNTIME_SOURCE}/"