#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Load Snowflake (and any other) env vars from .env if present.
# The .env file is gitignored — never commit it.
if [[ -f "$PROJECT_ROOT/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "$PROJECT_ROOT/.env"
    set +a
fi

python3 -m streamlit run "$PROJECT_ROOT/dashboard/streamlit_app.py" "$@"
