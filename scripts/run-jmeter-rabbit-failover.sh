#!/usr/bin/env bash
set -euo pipefail

# Resolve repo root relative to this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

JMETER_DIR="${REPO_ROOT}/jmeter"
DOCKER_DIR="${REPO_ROOT}/docker"
COMPOSE_FILE="docker-compose.yml"

echo "[$(date +%H:%M:%S)] Starting JMeter in background..."
cd "${JMETER_DIR}"
rm -rf report results.jtl jmeter.log
jmeter -n -t "channel-leak-test-plan.jmx" -l results.jtl \
  -j jmeter.log -e -o report -f -Jduration=360 \
  > "${JMETER_DIR}/jmeter-stdout.log" 2>&1 &
JMETER_PID=$!
JMETER_START=$(date +%s)
JMETER_MAX_SECONDS=300
echo "[$(date +%H:%M:%S)] JMeter PID: ${JMETER_PID} (max runtime: ${JMETER_MAX_SECONDS}s)"

echo "[$(date +%H:%M:%S)] Waiting 90 seconds before stopping RabbitMQ nodes..."
sleep 90

cd "${DOCKER_DIR}"
for node in rabbitmq-node1 rabbitmq-node2 rabbitmq-node3; do
  echo "[$(date +%H:%M:%S)] Stopping ${node}..."
  docker compose -f "${COMPOSE_FILE}" stop "${node}"
done

echo "[$(date +%H:%M:%S)] Bringing all services back up..."
docker compose -f "${COMPOSE_FILE}" up -d

echo "[$(date +%H:%M:%S)] Waiting for JMeter (PID ${JMETER_PID}) to finish (hard cap ${JMETER_MAX_SECONDS}s)..."
while kill -0 "${JMETER_PID}" 2>/dev/null; do
  ELAPSED=$(( $(date +%s) - JMETER_START ))
  if [ "${ELAPSED}" -ge "${JMETER_MAX_SECONDS}" ]; then
    echo "[$(date +%H:%M:%S)] JMeter exceeded ${JMETER_MAX_SECONDS}s, killing process tree..."
    pkill -TERM -P "${JMETER_PID}" 2>/dev/null || true
    kill -TERM "${JMETER_PID}" 2>/dev/null || true
    sleep 5
    pkill -KILL -P "${JMETER_PID}" 2>/dev/null || true
    kill -KILL "${JMETER_PID}" 2>/dev/null || true
    break
  fi
  sleep 5
done
wait "${JMETER_PID}" 2>/dev/null || true

echo "[$(date +%H:%M:%S)] Done. Report at ${JMETER_DIR}/report"
