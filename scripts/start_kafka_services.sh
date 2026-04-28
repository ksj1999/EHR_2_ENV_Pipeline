#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$PROJECT_ROOT/logs"
KAFKA_RUNTIME_DIR="/mnt/synthea_data/kafka_runtime"
ZOOKEEPER_PORT="${ZOOKEEPER_PORT:-2182}"
KAFKA_PORT="${KAFKA_PORT:-9093}"

mkdir -p "$LOG_DIR"
mkdir -p "$KAFKA_RUNTIME_DIR/zookeeper" "$KAFKA_RUNTIME_DIR/kafka-logs"

ZOOKEEPER_CONFIG="$KAFKA_RUNTIME_DIR/zookeeper.properties"
KAFKA_CONFIG="$KAFKA_RUNTIME_DIR/server.properties"

cat > "$ZOOKEEPER_CONFIG" <<EOF
dataDir=$KAFKA_RUNTIME_DIR/zookeeper
clientPort=$ZOOKEEPER_PORT
maxClientCnxns=0
admin.enableServer=false
EOF

cat > "$KAFKA_CONFIG" <<EOF
broker.id=1
listeners=PLAINTEXT://localhost:$KAFKA_PORT
advertised.listeners=PLAINTEXT://localhost:$KAFKA_PORT
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
log.dirs=$KAFKA_RUNTIME_DIR/kafka-logs
num.partitions=3
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.retention.hours=168
log.segment.bytes=1073741824
zookeeper.connect=localhost:$ZOOKEEPER_PORT
zookeeper.connection.timeout.ms=18000
group.initial.rebalance.delay.ms=0
EOF

wait_for_port() {
  local host="$1"
  local port="$2"
  local label="$3"
  local attempts="${4:-30}"
  local delay_seconds="${5:-1}"

  for ((i = 1; i <= attempts; i++)); do
    if bash -c ">/dev/tcp/$host/$port" 2>/dev/null; then
      echo "$label is ready on $host:$port"
      return 0
    fi

    sleep "$delay_seconds"
  done

  echo "Timed out waiting for $label on $host:$port" >&2
  return 1
}

if ! pgrep -af "zookeeper-server-start.*$ZOOKEEPER_CONFIG" >/dev/null; then
  nohup "$HOME/kafka/bin/zookeeper-server-start.sh" "$ZOOKEEPER_CONFIG" \
    > "$LOG_DIR/zookeeper.log" 2>&1 &
  echo "Started ZooKeeper on port $ZOOKEEPER_PORT"
else
  echo "ZooKeeper already running"
fi

wait_for_port "localhost" "$ZOOKEEPER_PORT" "ZooKeeper"

if ! pgrep -af "kafka-server-start.*$KAFKA_CONFIG" >/dev/null; then
  nohup "$HOME/kafka/bin/kafka-server-start.sh" "$KAFKA_CONFIG" \
    > "$LOG_DIR/kafka.log" 2>&1 &
  echo "Started Kafka broker on port $KAFKA_PORT"
else
  echo "Kafka broker already running"
fi

wait_for_port "localhost" "$KAFKA_PORT" "Kafka broker"
