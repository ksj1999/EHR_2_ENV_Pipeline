#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9093}"
TOPIC="${KAFKA_TOPIC:-environment_raw}"
MODE="${WEATHER_PULL_MODE:-open-meteo}"
POLL_SECONDS="${WEATHER_PULL_INTERVAL_SECONDS:-300}"
MESSAGES_PER_BATCH="${WEATHER_PULL_MESSAGES_PER_BATCH:-10}"
MIN_PATIENT_COUNT="${WEATHER_PULL_MIN_PATIENT_COUNT:-1000}"

echo "Starting continuous weather puller"
echo "  mode: ${MODE}"
echo "  bootstrap: ${BOOTSTRAP_SERVERS}"
echo "  topic: ${TOPIC}"
echo "  interval_seconds: ${POLL_SECONDS}"
echo "  messages_per_batch: ${MESSAGES_PER_BATCH}"
echo "  min_patient_count: ${MIN_PATIENT_COUNT}"
echo "  iterations: forever"
echo
echo "Press Ctrl-C to stop."

python3 "$PROJECT_ROOT/producers/weather_producer.py" \
  --bootstrap-servers "$BOOTSTRAP_SERVERS" \
  --topic "$TOPIC" \
  --locations-dir /mnt/synthea_data/output/location_manifest \
  --mode "$MODE" \
  --messages-per-batch "$MESSAGES_PER_BATCH" \
  --interval-seconds "$POLL_SECONDS" \
  --iterations 0 \
  --min-patient-count "$MIN_PATIENT_COUNT" \
  "$@"
