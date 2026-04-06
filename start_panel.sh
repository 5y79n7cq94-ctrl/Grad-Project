#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"

if [[ -n "${MEDIACRAWLER_ROOT:-}" ]]; then
  MC_ROOT="${MEDIACRAWLER_ROOT}"
else
  MC_ROOT=""
  SEARCH_DIR="$PROJECT_ROOT"
  while [[ "$SEARCH_DIR" != "/" ]]; do
    if [[ -f "$SEARCH_DIR/MediaCrawler/api/services/project_analytics.py" ]]; then
      MC_ROOT="$SEARCH_DIR/MediaCrawler"
      break
    fi
    SEARCH_DIR="$(dirname "$SEARCH_DIR")"
  done
fi

if [[ -z "$MC_ROOT" || ! -d "$MC_ROOT" ]]; then
  echo "Could not find MediaCrawler. Set MEDIACRAWLER_ROOT first." >&2
  exit 1
fi

PYTHON_BIN="$MC_ROOT/.venv/bin/python"
if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Missing MediaCrawler virtualenv Python: $PYTHON_BIN" >&2
  exit 1
fi

export MEDIACRAWLER_ROOT="$MC_ROOT"
export MACAU_ANALYTICS_DB_PATH="${MACAU_ANALYTICS_DB_PATH:-$PROJECT_ROOT/macau_analytics.db}"

cd "$PROJECT_ROOT"
exec "$PYTHON_BIN" -m uvicorn bridge:app --reload --host 127.0.0.1 --port 9038
