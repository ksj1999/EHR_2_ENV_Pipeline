#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9093}"
TOPIC="${KAFKA_TOPIC:-environment_raw}"

python3 "$PROJECT_ROOT/producers/weather_producer.py" \
  --bootstrap-servers "$BOOTSTRAP_SERVERS" \
  --topic "$TOPIC" \
  --locations-dir /mnt/synthea_data/output/location_manifest \
  "$@"
