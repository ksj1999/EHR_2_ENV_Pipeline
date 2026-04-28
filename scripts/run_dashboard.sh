#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

python3 -m streamlit run "$PROJECT_ROOT/dashboard/streamlit_app.py" "$@"
