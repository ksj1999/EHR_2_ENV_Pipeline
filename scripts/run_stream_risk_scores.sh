#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9093}"
TOPIC="${KAFKA_TOPIC:-environment_raw}"
OUTPUT_DIR="${RESPIRATORY_RISK_OUTPUT_DIR:-s3a://synthea-full-bucket/processed/respiratory_patient_features}"
CHECKPOINT_DIR="${RESPIRATORY_RISK_CHECKPOINT_DIR:-/mnt/synthea_data/checkpoints/respiratory_patient_features}"

export SPARK_LOCAL_IP=127.0.0.1

# Streaming join uses the pre-filtered respiratory cohort, not the full
# feature table. The cohort is ~10x smaller and fits comfortably in
# executor memory for the broadcast join (see build_patient_features.py).
FEATURES_DIR="${FEATURES_DIR:-s3a://synthea-full-bucket/features/patient_respiratory_features_cohort}"

"$HOME/spark/bin/spark-submit" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  "$PROJECT_ROOT/spark_jobs/stream_risk_scores.py" \
  --bootstrap-servers "$BOOTSTRAP_SERVERS" \
  --topic "$TOPIC" \
  --features-dir "$FEATURES_DIR" \
  --output-dir "$OUTPUT_DIR" \
  --checkpoint-dir "$CHECKPOINT_DIR" \
  "$@"
