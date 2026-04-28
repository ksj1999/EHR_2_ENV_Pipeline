#!/usr/bin/env bash
set -euo pipefail

TOPIC="${1:-environment_raw}"
BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9093}"

"$HOME/kafka/bin/kafka-topics.sh" \
  --create \
  --if-not-exists \
  --topic "$TOPIC" \
  --bootstrap-server "$BOOTSTRAP_SERVERS" \
  --partitions 3 \
  --replication-factor 1
