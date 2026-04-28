#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INPUT_DIR="${1:-s3a://synthea-full-bucket/features/patient_respiratory_features}"
OUTPUT_DIR="${2:-/mnt/synthea_data/output/location_manifest}"

export SPARK_LOCAL_IP=127.0.0.1

"$HOME/spark/bin/spark-submit" \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  "$PROJECT_ROOT/spark_jobs/build_location_manifest.py" \
  --input-dir "$INPUT_DIR" \
  --output-dir "$OUTPUT_DIR"
