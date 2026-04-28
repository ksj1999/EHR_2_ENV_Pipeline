#!/usr/bin/env bash
# One-shot pipeline launcher.
#
# Self-bootstrapping: handles a fresh EC2 session by checking prerequisites,
# building any missing artifacts (location manifest, cohort parquet, MA towns
# GeoJSON), killing stale processes, and starting all five components in a
# tmux session.
#
# Idempotent — safe to re-run; existing tmux session is killed and rebuilt.

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$PROJECT_ROOT/.env"
EBS_DEVICE="${EBS_DEVICE:-/dev/nvme1n1}"
EBS_MOUNT="${EBS_MOUNT:-/mnt/synthea_data}"
COHORT_S3="s3://synthea-full-bucket/features/patient_respiratory_features_cohort"
LOCATION_MANIFEST="$EBS_MOUNT/output/location_manifest"
GEOJSON_PATH="$EBS_MOUNT/cache/ma_towns.geojson"

step() { echo ""; echo "==> [$1] $2"; }

# ── 1. Mount EBS ────────────────────────────────────────────────────────
step "1/8" "Checking EBS mount at $EBS_MOUNT..."
if ! mountpoint -q "$EBS_MOUNT"; then
    sudo mount "$EBS_DEVICE" "$EBS_MOUNT"
    echo "    Mounted."
else
    echo "    Already mounted."
fi

# ── 2. Validate .env ────────────────────────────────────────────────────
step "2/8" "Validating .env..."
if [[ ! -f "$ENV_FILE" ]]; then
    echo "    ERROR: $ENV_FILE not found." >&2
    echo "    Create it with the Snowflake env vars (see README 'Configuration')." >&2
    exit 1
fi
# Strip CRLF if uploaded from Windows
sed -i 's/\r$//' "$ENV_FILE"
echo "    OK."

# ── 3. Location manifest ────────────────────────────────────────────────
step "3/8" "Checking location manifest..."
if [[ ! -d "$LOCATION_MANIFEST" ]] \
   || ! compgen -G "$LOCATION_MANIFEST/*.csv" >/dev/null; then
    echo "    Missing — building (~1 min)..."
    "$PROJECT_ROOT/scripts/run_build_location_manifest.sh"
else
    echo "    OK."
fi

# ── 4. Respiratory cohort parquet (input to streaming join) ─────────────
step "4/8" "Checking respiratory cohort parquet on S3..."
if ! aws s3 ls "$COHORT_S3/" >/dev/null 2>&1; then
    echo "    Missing — building patient features (~10 min)..."
    "$PROJECT_ROOT/scripts/run_build_patient_features.sh"
else
    echo "    OK."
fi

# ── 5. MA towns GeoJSON (dashboard map) ─────────────────────────────────
step "5/8" "Checking MA towns GeoJSON for dashboard..."
if [[ ! -f "$GEOJSON_PATH" ]]; then
    echo "    Missing — downloading from Census (~30 s)..."
    mkdir -p "$(dirname "$GEOJSON_PATH")"
    python3 - <<PYEOF || echo "    WARN: download failed; dashboard map will not render."
import geopandas as gpd
gdf = gpd.read_file(
    "https://www2.census.gov/geo/tiger/GENZ2022/shp/cb_2022_25_cousub_500k.zip"
)
gdf[["NAME", "geometry"]].to_file("$GEOJSON_PATH", driver="GeoJSON")
print(f"    Saved {len(gdf)} towns")
PYEOF
else
    echo "    OK."
fi

# ── 6. Stop stale processes from any previous run ───────────────────────
step "6/8" "Stopping stale processes..."
pkill -f "stream_risk_scores.py" 2>/dev/null || true
pkill -f "weather_producer.py"   2>/dev/null || true
pkill -f "streamlit run"          2>/dev/null || true
pkill -f "kafka.Kafka"            2>/dev/null || true
pkill -f "zookeeper-server-start" 2>/dev/null || true
pkill -f "airflow standalone"     2>/dev/null || true
sleep 3
echo "    OK."

# ── 7. tmux session ─────────────────────────────────────────────────────
step "7/8" "Setting up tmux session 'pipeline'..."
tmux kill-session -t pipeline 2>/dev/null || true
tmux new-session -d -s pipeline -n kafka
tmux new-window -t pipeline -n producer
tmux new-window -t pipeline -n stream
tmux new-window -t pipeline -n dashboard
tmux new-window -t pipeline -n airflow
echo "    OK."

# ── 8. Launch components ────────────────────────────────────────────────
step "8/8" "Launching components..."

echo "    [kafka] starting broker + zookeeper..."
tmux send-keys -t pipeline:kafka "cd $PROJECT_ROOT && ./scripts/start_kafka_services.sh" Enter
echo "    Waiting 15 s for Kafka to initialize..."
sleep 15

echo "    [kafka] creating topic..."
tmux send-keys -t pipeline:kafka "./scripts/create_kafka_topic.sh" Enter
sleep 3

echo "    [producer] starting weather puller..."
tmux send-keys -t pipeline:producer "cd $PROJECT_ROOT && ./scripts/run_continuous_weather_puller.sh" Enter

echo "    [stream] starting Spark Structured Streaming..."
tmux send-keys -t pipeline:stream "cd $PROJECT_ROOT && ./scripts/run_stream_risk_scores.sh" Enter
echo "    Waiting 60 s for Spark JVM to initialize..."
sleep 60

echo "    [dashboard] starting Streamlit..."
tmux send-keys -t pipeline:dashboard "cd $PROJECT_ROOT && ./scripts/run_dashboard.sh" Enter
echo "    Waiting 30 s before Airflow..."
sleep 30

echo "    [airflow] starting standalone..."
tmux send-keys -t pipeline:airflow "airflow standalone" Enter

# ── Done ────────────────────────────────────────────────────────────────
EC2_IP=$(curl -s --max-time 2 http://169.254.169.254/latest/meta-data/public-ipv4 \
         2>/dev/null || echo "EC2_IP")

cat <<EOF

──────────────────────────────────────────────────────────────────
Pipeline started. Attach with:

  tmux attach -t pipeline

Switch windows: Ctrl+B then 0=kafka 1=producer 2=stream 3=dashboard 4=airflow
Detach (leave running): Ctrl+B then d

Services:
  Dashboard: http://$EC2_IP:8501
  Airflow:   http://$EC2_IP:8080
──────────────────────────────────────────────────────────────────
EOF
