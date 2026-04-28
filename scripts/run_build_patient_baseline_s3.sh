#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <s3-input-dir> <s3-output-dir>"
  echo "Example: $0 s3a://my-bucket/raw/synthea s3a://my-bucket/processed/patient_baseline"
  exit 1
fi

INPUT_DIR="$1"
OUTPUT_DIR="$2"

export SPARK_LOCAL_IP=127.0.0.1

"$HOME/spark/bin/spark-submit" \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  "$PROJECT_ROOT/spark_jobs/build_patient_baseline.py" \
  --input-dir "$INPUT_DIR" \
  --output-dir "$OUTPUT_DIR"
