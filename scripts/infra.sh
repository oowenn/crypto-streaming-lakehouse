#!/usr/bin/env bash
set -euo pipefail

# --- Config (override via env) ---
COMPOSE_FILE="${COMPOSE_FILE:-infra/docker-compose.yml}"
BROKER_NAME="${BROKER_NAME:-redpanda}"     # container service name
CONSOLE_NAME="${CONSOLE_NAME:-redpanda-console}"
BROKER_INTERNAL_PORT="${BROKER_INTERNAL_PORT:-9092/tcp}"
TOPIC="${TOPIC:-crypto.trades}"
PARTITIONS="${PARTITIONS:-1}"
REPLICAS="${REPLICAS:-1}"
WAIT_SECS="${WAIT_SECS:-60}"

# --- Helpers ---
die() { echo "ERROR: $*" >&2; exit 1; }
have() { command -v "$1" >/dev/null 2>&1; }

host_port_for() {
  local container="$1" port="$2"
  docker inspect -f '{{range $p,$v := .NetworkSettings.Ports}}{{if eq $p "'"$port"'"}}{{(index $v 0).HostPort}}{{end}}{{end}}' "$container"
}

wait_broker() {
  echo "Waiting for broker in container '$BROKER_NAME' (timeout: ${WAIT_SECS}s)..."
  local start=$(date +%s)
  while :; do
    if docker exec "$BROKER_NAME" rpk cluster info >/dev/null 2>&1; then
      echo "Broker is ready."
      return 0
    fi
    sleep 1
    local now=$(date +%s)
    (( now - start > WAIT_SECS )) && die "Broker not ready after ${WAIT_SECS}s"
  done
}

create_topic() {
  echo "Ensuring topic '$TOPIC' exists..."
  docker exec "$BROKER_NAME" rpk topic create "$TOPIC" --partitions "$PARTITIONS" --replicas "$REPLICAS" >/dev/null 2>&1 || true
  docker exec "$BROKER_NAME" rpk topic list
}

cmd_up() {
  echo "Bringing up containers with $COMPOSE_FILE ..."
  docker compose -f "$COMPOSE_FILE" up -d

  wait_broker
  create_topic

  local host_port
  host_port="$(host_port_for "$BROKER_NAME" "$BROKER_INTERNAL_PORT")"
  if [[ -n "$host_port" ]]; then
    echo
    echo "✅ Infra ready."
    echo "   Console:   http://localhost:8080 (if mapped)"
    echo "   Bootstrap: localhost:${host_port}"
    echo
    echo "   Sanity test (from host):"
    echo "     docker exec $BROKER_NAME rpk topic produce $TOPIC"
    echo "     docker exec $BROKER_NAME rpk topic consume $TOPIC -n 1"
  else
    echo "⚠️  Could not determine host port for $BROKER_INTERNAL_PORT. Check 'docker ps'."
  fi
}

cmd_down() {
  echo "Stopping & removing containers + volumes..."
  docker compose -f "$COMPOSE_FILE" down -v
}

cmd_status() {
  docker compose -f "$COMPOSE_FILE" ps
  echo
  echo "Topics:"
  if docker exec "$BROKER_NAME" rpk topic list 2>/dev/null; then :; else echo "(broker not reachable)"; fi
}

cmd_logs() {
  docker logs --tail=200 -f "$BROKER_NAME"
}

usage() {
  cat <<USAGE
Usage: ${0##*/} <up|down|status|logs>

Env overrides:
  COMPOSE_FILE=$COMPOSE_FILE
  BROKER_NAME=$BROKER_NAME
  CONSOLE_NAME=$CONSOLE_NAME
  BROKER_INTERNAL_PORT=$BROKER_INTERNAL_PORT
  TOPIC=$TOPIC  PARTITIONS=$PARTITIONS  REPLICAS=$REPLICAS
  WAIT_SECS=$WAIT_SECS
USAGE
}

case "${1:-}" in
  up)     cmd_up ;;
  down)   cmd_down ;;
  status) cmd_status ;;
  logs)   cmd_logs ;;
  *)      usage ;;
esac
