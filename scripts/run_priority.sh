#!/usr/bin/env bash

set -euo pipefail

# Configuration (override via env vars)
BOOTSTRAP_SERVERS=${BOOTSTRAP_SERVERS:-localhost:9092}
CONFIG_PATH=${CONFIG_PATH:-config.json}
LOG_DIR=${LOG_DIR:-logs}

# Determine python binary if not provided
if [[ -z "${PYTHON_BIN:-}" ]]; then
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN=python3
  elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN=python
  else
    echo "No python interpreter found (python3 or python). Please install Python 3."
    exit 1
  fi
fi

PRIORITY_TOPICS=(urls_priority_1 urls_priority_2 urls_priority_3 urls_priority_4 urls_priority_5)

ensure_logs_dir() {
  mkdir -p "$LOG_DIR"
}

install_deps() {
  echo "Installing Python dependencies..."
  "$PYTHON_BIN" -m pip install -r requirements.txt 1>"$LOG_DIR/pip_install.out" 2>"$LOG_DIR/pip_install.err" || {
    echo "Failed to install dependencies. See $LOG_DIR/pip_install.err"
    exit 1
  }
}

detect_kafka_topics_cmd() {
  if command -v kafka-topics >/dev/null 2>&1; then
    echo kafka-topics
    return
  fi
  if command -v kafka-topics.sh >/dev/null 2>&1; then
    echo kafka-topics.sh
    return
  fi
  echo "" # not found
}

create_topics() {
  local cmd
  cmd=$(detect_kafka_topics_cmd)
  if [[ -z "$cmd" ]]; then
    echo "Kafka topics CLI not found (kafka-topics or kafka-topics.sh). Skipping topic creation."
    return 0
  fi

  echo "Creating Kafka topics (if not exist) on $BOOTSTRAP_SERVERS..."
  for topic in "${PRIORITY_TOPICS[@]}"; do
    # Try create with --if-not-exists if supported; otherwise ignore error if exists
    if "$cmd" --bootstrap-server "$BOOTSTRAP_SERVERS" --help 2>&1 | grep -q -- "if-not-exists"; then
      "$cmd" --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1 --bootstrap-server "$BOOTSTRAP_SERVERS" || true
    else
      "$cmd" --create --topic "$topic" --partitions 3 --replication-factor 1 --bootstrap-server "$BOOTSTRAP_SERVERS" || true
    fi
  done
}

is_kafka_up() {
  # Try CLI if available
  local cmd
  cmd=$(detect_kafka_topics_cmd)
  if [[ -n "$cmd" ]]; then
    "$cmd" --bootstrap-server "$BOOTSTRAP_SERVERS" --list >/dev/null 2>&1 && return 0
  fi
  # Fallback to TCP check using bash built-in
  local host="${BOOTSTRAP_SERVERS%%:*}"
  local port="${BOOTSTRAP_SERVERS##*:}"
  (exec 3<>"/dev/tcp/${host}/${port}") >/dev/null 2>&1 && { exec 3<&-; exec 3>&-; return 0; }
  return 1
}

wait_for_kafka() {
  local retries=${1:-20}
  local delay=${2:-0.5}
  echo "Checking Kafka availability at $BOOTSTRAP_SERVERS..."
  for ((i=1; i<=retries; i++)); do
    if is_kafka_up; then
      echo "Kafka is reachable."
      return 0
    fi
    sleep "$delay"
  done
  echo "Kafka is not reachable at $BOOTSTRAP_SERVERS."
  echo "Hint: Start Kafka or set BOOTSTRAP_SERVERS. Examples:"
  echo "  brew services start kafka    # macOS (ensure zookeeper or use KRaft)"
  echo "  docker run -it --rm -p 9092:9092 redpandadata/redpanda start --overprovisioned --smp 1 --memory 1G --reserve-memory 0M --node-id 0 --check=false --advertise-kafka-addr 127.0.0.1:9092"
  return 1
}

start_workers() {
  echo "Starting 5 workers (background)..."
  for topic in "${PRIORITY_TOPICS[@]}"; do
    # Logs per worker
    nohup "$PYTHON_BIN" -m src.crawler.core.worker "$topic" --config "$CONFIG_PATH" \
      >"$LOG_DIR/worker_${topic}.out" 2>"$LOG_DIR/worker_${topic}.err" &
    echo $! >"$LOG_DIR/worker_${topic}.pid"
    echo "Worker for $topic started with PID $(cat "$LOG_DIR/worker_${topic}.pid")"
    sleep 0.2
  done
}

run_master() {
  echo "Running master dispatcher (foreground)..."
  "$PYTHON_BIN" -m src.crawler.core.master
}

print_summary() {
  cat <<EOF

Summary:
- Bootstrap servers: $BOOTSTRAP_SERVERS
- Config path: $CONFIG_PATH
- Workers logs: $LOG_DIR/worker_*.out (stderr: worker_*.err)
- Worker PIDs: $LOG_DIR/worker_*.pid

To stop workers:
  xargs kill < <(cat $LOG_DIR/worker_*.pid 2>/dev/null) || true

EOF
}

main() {
  ensure_logs_dir
  install_deps
  if ! wait_for_kafka; then
    exit 1
  fi
  create_topics
  start_workers
  print_summary
  run_master
}

main "$@"


