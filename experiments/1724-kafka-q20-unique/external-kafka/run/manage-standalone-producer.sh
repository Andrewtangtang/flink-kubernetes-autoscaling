#!/bin/bash
###############################################################################
# manage-standalone-producer.sh
#
# Run the insert_kafka_unique SQL job directly on a target host in local Flink
# mode. This avoids using the Flink Kubernetes Operator for the producer.
###############################################################################
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../../../../cluster/config/env.sh"

TARGET_HOST="${TARGET_HOST:-c153}"
HOST_TAG="${HOST_TAG:-${TARGET_HOST}}"
CONTAINER_NAME="${CONTAINER_NAME:-standalone-insert-kafka-${HOST_TAG}}"
IMAGE="${FLINK_BENCHMARK_IMAGE}"
QUERY_NAME="${QUERY_NAME:-insert_kafka_unique}"
JOB_JAR_PATH="${JOB_JAR_PATH:-/opt/flink/lib/nexmark-flink-0.3-SNAPSHOT.jar}"
KAFKA_PORT="${KAFKA_PORT:-9092}"
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-kafka:${KAFKA_PORT}}"
BOOTSTRAP_HOST="${BOOTSTRAP_HOST:-${BOOTSTRAP_SERVERS%%:*}}"
KAFKA_SQL_CONNECTOR_JAR="${KAFKA_SQL_CONNECTOR_JAR:-flink-sql-connector-kafka-3.3.0.jar}"
KAFKA_CLIENTS_JAR="${KAFKA_CLIENTS_JAR:-kafka-clients-3.4.0.jar}"
PARALLELISM="${PARALLELISM:-1}"
SLOTS="${SLOTS:-${PARALLELISM}}"
TM_CORES="${TM_CORES:-4}"
DOCKER_CPUS="${DOCKER_CPUS:-${TM_CORES}}"
JM_PROCESS_MEMORY="${JM_PROCESS_MEMORY:-2048m}"
TM_PROCESS_MEMORY="${TM_PROCESS_MEMORY:-8192m}"
PRODUCER_REST_PORT="${PRODUCER_REST_PORT:-18081}"
FLINK_REST_PORT="${FLINK_REST_PORT:-${PRODUCER_REST_PORT}}"
TPS="${TPS:-50000}"
EVENTS="${EVENTS:-300000000}"
FIRST_EVENT_ID="${FIRST_EVENT_ID:-1}"
MAX_EMIT_SPEED="${MAX_EMIT_SPEED:-true}"
JOB_NAME="${JOB_NAME:-insert_kafka-unique-sql-ssd-${HOST_TAG}-rocksdb-options}"

GREEN='\033[0;32m'; RED='\033[0;31m'; NC='\033[0m'

ACTION="${1:-status}"

resolve_target_ip() {
    if [[ -n "${TARGET_IP:-}" ]]; then
        printf '%s\n' "${TARGET_IP}"
        return 0
    fi

    if command -v getent >/dev/null 2>&1; then
        local resolved_ip
        resolved_ip="$(getent ahostsv4 "${TARGET_HOST}" | awk 'NR == 1 { print $1 }')"
        if [[ -n "${resolved_ip}" ]]; then
            printf '%s\n' "${resolved_ip}"
            return 0
        fi
    fi

    if [[ -z "${TARGET_HOST}" ]]; then
        printf 'TARGET_HOST is required\n' >&2
        exit 1
    fi

    printf 'Could not resolve TARGET_IP for %s\n' "${TARGET_HOST}" >&2
    exit 1
}

TARGET_IP="$(resolve_target_ip)"

ssh_remote() {
    ssh -o BatchMode=yes "${TARGET_HOST}" "$@"
}

sync_remote_image() {
    local local_image_id remote_image_id

    docker image inspect "${IMAGE}" >/dev/null 2>&1
    local_image_id="$(docker image inspect --format '{{.Id}}' "${IMAGE}")"
    remote_image_id="$(ssh_remote "sudo -n docker image inspect --format '{{.Id}}' '${IMAGE}' 2>/dev/null || true")"

    if [[ "${remote_image_id}" == "${local_image_id}" ]]; then
        return 0
    fi

    echo "  Syncing ${IMAGE} to ${TARGET_HOST}..."
    docker save "${IMAGE}" | ssh_remote "sudo -n docker load >/dev/null"

    remote_image_id="$(ssh_remote "sudo -n docker image inspect --format '{{.Id}}' '${IMAGE}'")"
    if [[ "${remote_image_id}" != "${local_image_id}" ]]; then
        echo -e "${RED}✗ Remote image sync failed for ${IMAGE}${NC}"
        exit 1
    fi
}

ensure_remote_image() {
    local remote_image_id

    remote_image_id="$(ssh_remote "sudo -n docker image inspect --format '{{.Id}}' '${IMAGE}' 2>/dev/null || true")"
    if [[ -n "${remote_image_id}" ]]; then
        return 0
    fi

    echo "  Pulling ${IMAGE} on ${TARGET_HOST}..."
    if ssh_remote "sudo -n docker pull '${IMAGE}' >/dev/null"; then
        return 0
    fi

    echo "  Remote pull failed; falling back to a local Docker image sync..."
    sync_remote_image
}

verify_remote_image() {
    ssh_remote "sudo -n docker run --rm --entrypoint sh ${IMAGE} -lc 'test -f ${JOB_JAR_PATH} && test -f /opt/flink/lib/${KAFKA_SQL_CONNECTOR_JAR} && test -f /opt/flink/lib/${KAFKA_CLIENTS_JAR}' >/dev/null"
}

case "${ACTION}" in
    start)
        echo "── Starting Standalone insert_kafka On ${TARGET_HOST} ────────────────"
        echo "  query=${QUERY_NAME} parallelism=${PARALLELISM} slots=${SLOTS} tps=${TPS} events=${EVENTS} max_emit_speed=${MAX_EMIT_SPEED}"
        echo "  bootstrap_servers=${BOOTSTRAP_SERVERS} first_event_id=${FIRST_EVENT_ID}"
        echo "  tm_cores=${TM_CORES} tm_memory=${TM_PROCESS_MEMORY} jm_memory=${JM_PROCESS_MEMORY} rest_port=${FLINK_REST_PORT}"
        ensure_remote_image
        verify_remote_image
        ssh_remote "sudo -n docker rm -f ${CONTAINER_NAME} >/dev/null 2>&1 || true"
        ssh_remote "sudo -n docker run -d --name ${CONTAINER_NAME} --network host \
            --cpus ${DOCKER_CPUS} \
            --add-host ${BOOTSTRAP_HOST}:${TARGET_IP} \
            ${IMAGE} bash -lc '
                /opt/flink/bin/flink run \
                    -Dexecution.target=local \
                    -Dparallelism.default=${PARALLELISM} \
                    -Dtaskmanager.numberOfTaskSlots=${SLOTS} \
                    -Dtaskmanager.cpu.cores=${TM_CORES} \
                    -Djobmanager.memory.process.size=${JM_PROCESS_MEMORY} \
                    -Dtaskmanager.memory.process.size=${TM_PROCESS_MEMORY} \
                    -Drest.bind-address=0.0.0.0 \
                    -Drest.address=${TARGET_IP} \
                    -Drest.bind-port=${FLINK_REST_PORT} \
                    -Drest.port=${FLINK_REST_PORT} \
                    -Dweb.submit.enable=true \
                    -Dweb.cancel.enable=true \
                    -c com.github.nexmark.flink.sql.SqlQueryJob \
                    ${JOB_JAR_PATH} \
                    --query ${QUERY_NAME} \
                    --tps ${TPS} \
                    --events ${EVENTS} \
                    --max-emit-speed ${MAX_EMIT_SPEED} \
                    --first-event-id ${FIRST_EVENT_ID} \
                    --job-name ${JOB_NAME} \
                    --bootstrap-servers ${BOOTSTRAP_SERVERS} \
                    --wait-for-finish
            ' >/dev/null"
        sleep 3
        remote_status="$(ssh_remote "sudo -n docker inspect --format '{{.State.Status}} {{.State.ExitCode}}' ${CONTAINER_NAME}")"
        if [[ "${remote_status}" != running* ]]; then
            echo -e "${RED}✗ Standalone producer exited during startup on ${TARGET_HOST}${NC}"
            ssh_remote "sudo -n docker logs --tail 120 ${CONTAINER_NAME}" >&2 || true
            exit 1
        fi
        echo -e "${GREEN}✓${NC} Standalone producer container started on ${TARGET_HOST}"
        ;;

    sync-image)
        echo "── Syncing Standalone insert_kafka Image To ${TARGET_HOST} ───────────"
        sync_remote_image
        verify_remote_image
        echo -e "${GREEN}✓${NC} ${IMAGE} is ready on ${TARGET_HOST}"
        ;;

    stop)
        echo "── Stopping Standalone insert_kafka On ${TARGET_HOST} ────────────────"
        ssh_remote "sudo -n docker rm -f ${CONTAINER_NAME} >/dev/null 2>&1 || true"
        echo -e "${GREEN}✓${NC} Standalone producer stopped"
        ;;

    pause)
        echo "── Pausing Standalone insert_kafka On ${TARGET_HOST} ────────────────"
        remote_status="$(ssh_remote "sudo -n docker inspect --format '{{.State.Status}} {{.State.Paused}}' ${CONTAINER_NAME} 2>/dev/null || true")"
        if [[ -z "${remote_status}" ]]; then
            echo -e "${RED}✗ Standalone producer container does not exist${NC}" >&2
            exit 1
        fi
        if [[ "${remote_status}" == "running false" ]]; then
            ssh_remote "sudo -n docker pause ${CONTAINER_NAME} >/dev/null"
        elif [[ "${remote_status}" != "running true" ]]; then
            echo -e "${RED}✗ Cannot pause producer in state: ${remote_status}${NC}" >&2
            exit 1
        fi
        remote_status="$(ssh_remote "sudo -n docker inspect --format '{{.State.Status}} {{.State.Paused}}' ${CONTAINER_NAME}")"
        if [[ "${remote_status}" != "running true" ]]; then
            echo -e "${RED}✗ Producer pause verification failed: ${remote_status}${NC}" >&2
            exit 1
        fi
        echo -e "${GREEN}✓${NC} Standalone producer paused"
        ;;

    resume|unpause)
        echo "── Resuming Standalone insert_kafka On ${TARGET_HOST} ───────────────"
        remote_status="$(ssh_remote "sudo -n docker inspect --format '{{.State.Status}} {{.State.Paused}}' ${CONTAINER_NAME} 2>/dev/null || true")"
        if [[ -z "${remote_status}" ]]; then
            echo -e "${RED}✗ Standalone producer container does not exist${NC}" >&2
            exit 1
        fi
        if [[ "${remote_status}" == "running true" ]]; then
            ssh_remote "sudo -n docker unpause ${CONTAINER_NAME} >/dev/null"
        elif [[ "${remote_status}" != "running false" ]]; then
            echo -e "${RED}✗ Cannot resume producer in state: ${remote_status}${NC}" >&2
            exit 1
        fi
        remote_status="$(ssh_remote "sudo -n docker inspect --format '{{.State.Status}} {{.State.Paused}}' ${CONTAINER_NAME}")"
        if [[ "${remote_status}" != "running false" ]]; then
            echo -e "${RED}✗ Producer resume verification failed: ${remote_status}${NC}" >&2
            exit 1
        fi
        echo -e "${GREEN}✓${NC} Standalone producer resumed"
        ;;

    status)
        echo "── Standalone insert_kafka Status On ${TARGET_HOST} ──────────────────"
        ssh_remote "sudo -n docker ps -a --filter name=${CONTAINER_NAME}"
        ;;

    logs)
        echo "── Standalone insert_kafka Logs On ${TARGET_HOST} ────────────────────"
        ssh_remote "sudo -n docker logs --tail 200 ${CONTAINER_NAME}"
        ;;

    *)
        echo -e "${RED}Unknown action: ${ACTION}${NC}"
        exit 1
        ;;
esac
