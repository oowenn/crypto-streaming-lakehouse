#!/usr/bin/env bash
set -euo pipefail

# --- Config (override via env) ---
PAIR="${PAIR:-XBT/USDT}"
TOPIC="${TOPIC:-crypto.trades}"
BOOTSTRAP="${BOOTSTRAP:-localhost:19092}"

BRONZE_DIR="${BRONZE_DIR:-data/bronze/crypto_trades}"
SILVER_DIR="${SILVER_DIR:-data/silver/trades}"
GOLD_DIR="${GOLD_DIR:-data/gold/bars_1m}"
CHECKPOINT_DIR="${CHECKPOINT_DIR:-data/checkpoints}"

# --- Paths to your jobs ---
INGEST="ingestion/kraken_trades_ws.py"
BRONZE_JOB="spark/jobs/kafka_to_bronze_trades.py"
SILVER_JOB="spark/jobs/bronze_to_silver_trades.py"
GOLD_JOB="spark/jobs/silver_to_gold_bars.py"

# --- Setup dirs ---
mkdir -p .logs .pids "$BRONZE_DIR" "$SILVER_DIR" "$GOLD_DIR" "$CHECKPOINT_DIR"

pid_is_alive() { local p="$1"; [[ -n "$p" ]] && ps -p "$p" >/dev/null 2>&1; }
record_pid() { echo "$2" > ".pids/$1.pid"; }
read_pid() { [[ -f ".pids/$1.pid" ]] && cat ".pids/$1.pid" || true; }

start_ingest() {
  echo "Starting ingestion: $INGEST  ($PAIR → $TOPIC @ $BOOTSTRAP)"
  nohup python -u "$INGEST" \
      --pair "$PAIR" \
      --bootstrap "$BOOTSTRAP" \
      --topic "$TOPIC" \
      >> .logs/ingestion.log 2>&1 &
  record_pid ingestion $!
  echo " ingestion PID: $(read_pid ingestion)"
}

start_bronze() {
  echo "Starting bronze: $BRONZE_JOB"
  nohup python -u "$BRONZE_JOB" \
      --bootstrap "$BOOTSTRAP" \
      --topic "$TOPIC" \
      --out "$BRONZE_DIR" \
      --checkpoint "$CHECKPOINT_DIR/kafka_to_bronze_trades" \
      --startingOffsets earliest \
      --trigger "2 seconds" \
      >> .logs/bronze.log 2>&1 &
  record_pid bronze $!
  echo " bronze PID: $(read_pid bronze)"
}

start_silver() {
  echo "Starting silver: $SILVER_JOB"
  nohup python -u "$SILVER_JOB" \
      --bronze "$BRONZE_DIR" \
      --silver "$SILVER_DIR" \
      --checkpoint "$CHECKPOINT_DIR/bronze_to_silver_trades" \
      >> .logs/silver.log 2>&1 &
  record_pid silver $!
  echo " silver PID: $(read_pid silver)"
}

start_gold() {
  echo "Starting gold: $GOLD_JOB"
  nohup python -u "$GOLD_JOB" \
      --silver "$SILVER_DIR" \
      --gold "$GOLD_DIR" \
      --checkpoint "$CHECKPOINT_DIR/silver_to_gold_bars_1m" \
      >> .logs/gold.log 2>&1 &
  record_pid gold $!
  echo " gold PID: $(read_pid gold)"
}

stop_one() {
  local name="$1"
  local pid="$(read_pid "$name")"
  if [[ -z "$pid" ]]; then
    echo "No PID for $name"
    return 0
  fi
  if pid_is_alive "$pid"; then
    echo "Stopping $name ($pid)"
    kill "$pid" || true
  else
    echo "$name not running"
  fi
}

status() {
  for n in ingestion bronze silver gold; do
    local pid="$(read_pid "$n")"
    if [[ -n "$pid" ]] && pid_is_alive "$pid"; then
      echo "$n: running (pid $pid)"
    else
      echo "$n: not running"
    fi
  done
}

tail_logs() {
  tail -n 200 -F .logs/ingestion.log .logs/bronze.log .logs/silver.log .logs/gold.log
}

usage() {
  cat <<USAGE
Usage: ${0##*/} <command>

Commands:
  start-all         Start all four processes in background
  stop-all          Stop all four
  start [svc]       Start one (svc ∈ ingestion|bronze|silver|gold)
  stop  [svc]       Stop one
  status            Show which are running
  tail              Tail all logs

Environment overrides:
  PAIR=$PAIR
  TOPIC=$TOPIC
  BOOTSTRAP=$BOOTSTRAP
  BRONZE_DIR=$BRONZE_DIR
  SILVER_DIR=$SILVER_DIR
  GOLD_DIR=$GOLD_DIR
  CHECKPOINT_DIR=$CHECKPOINT_DIR
USAGE
}

cmd="${1:-}"; shift || true

case "$cmd" in
  start-all)
    start_ingest; start_bronze; start_silver; start_gold; status
    ;;
  stop-all)
    stop_one gold; stop_one silver; stop_one bronze; stop_one ingestion; status
    ;;
  start)
    case "${1:-}" in
      ingestion) start_ingest ;;
      bronze)    start_bronze ;;
      silver)    start_silver ;;
      gold)      start_gold ;;
      *) usage; exit 1;;
    esac
    ;;
  stop)
    case "${1:-}" in
      ingestion|bronze|silver|gold) stop_one "$1" ;;
      *) usage; exit 1;;
    esac
    ;;
  status) status ;;
  tail)   tail_logs ;;
  *) usage ;;
esac
