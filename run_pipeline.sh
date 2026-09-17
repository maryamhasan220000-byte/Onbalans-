#!/usr/bin/env bash

set -euo pipefail

PROJECT_DIR="/home/ubuntu/Onbalans-"
cd "$PROJECT_DIR"

source venv/bin/activate

echo "=== $(date -u +%Y-%m-%dT%H:%M:%SZ) pipeline run starting ==="

python src/loader.py
python src/ingest_entsoe.py
python src/loader_entsoe.py || {
    status=$?
    if [ "$status" -eq 1 ]; then
        echo "WARNING: ENTSO-E loader had partial failures; continuing to dbt."
    else
        exit "$status"
    fi
}

cd dbt
dotenv -f ../.env run -- dbt build

echo "=== $(date -u +%Y-%m-%dT%H:%M:%SZ) pipeline run finished ==="
