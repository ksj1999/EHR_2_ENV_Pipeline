#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <s3-baseline-dir>"
  echo "Example: $0 s3a://my-bucket/processed/patient_baseline"
  exit 1
fi

INPUT_DIR="$1"

export SPARK_LOCAL_IP=127.0.0.1

"$HOME/spark/bin/spark-submit" \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  "$PROJECT_ROOT/scripts/read_patient_baseline.py" \
  --input-dir "$INPUT_DIR"
