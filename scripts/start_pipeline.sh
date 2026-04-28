#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "==> Mounting EBS..."
if ! mountpoint -q /mnt/synthea_data; then
    sudo mount /dev/nvme1n1 /mnt/synthea_data
    echo "    Mounted."
else
    echo "    Already mounted."
fi

echo "==> Starting tmux session 'pipeline'..."
tmux kill-session -t pipeline 2>/dev/null || true
tmux new-session -d -s pipeline -n kafka

tmux new-window -t pipeline -n producer
tmux new-window -t pipeline -n stream
tmux new-window -t pipeline -n dashboard
tmux new-window -t pipeline -n airflow

echo "==> Starting Kafka..."
tmux send-keys -t pipeline:kafka "cd $PROJECT_ROOT && ./scripts/start_kafka_services.sh" Enter

echo "    Waiting 15s for Kafka to initialize..."
sleep 15

echo "==> Creating Kafka topic..."
tmux send-keys -t pipeline:kafka "cd $PROJECT_ROOT && ./scripts/create_kafka_topic.sh" Enter
sleep 3

echo "==> Starting weather producer..."
tmux send-keys -t pipeline:producer "cd $PROJECT_ROOT && ./scripts/run_continuous_weather_puller.sh" Enter

echo "==> Starting Spark Streaming..."
tmux send-keys -t pipeline:stream "cd $PROJECT_ROOT && ./scripts/run_stream_risk_scores.sh" Enter
echo "    Waiting 60s for Spark JVM to initialize..."
sleep 60

echo "==> Starting Streamlit dashboard..."
tmux send-keys -t pipeline:dashboard "cd $PROJECT_ROOT && ./scripts/run_dashboard.sh" Enter
echo "    Waiting 30s before starting Airflow..."
sleep 30

echo "==> Starting Airflow..."
tmux send-keys -t pipeline:airflow "airflow standalone" Enter

echo ""
echo "Pipeline started. Attach with:"
echo "  tmux attach -t pipeline"
echo ""
echo "Switch windows with Ctrl+B then:"
echo "  0=kafka  1=producer  2=stream  3=dashboard  4=airflow"
echo ""
echo "Services:"
echo "  Dashboard: http://$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || echo 'EC2_IP'):8501"
echo "  Airflow:   http://$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || echo 'EC2_IP'):8080"
