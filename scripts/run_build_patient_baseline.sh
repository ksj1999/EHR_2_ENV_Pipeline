#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export SPARK_LOCAL_IP=127.0.0.1

"$HOME/spark/bin/spark-submit" \
  "$PROJECT_ROOT/spark_jobs/build_patient_baseline.py" \
  --input-dir "$PROJECT_ROOT/data/synthea" \
  --output-dir "$PROJECT_ROOT/output/patient_baseline"
