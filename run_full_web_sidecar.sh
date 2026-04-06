#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${ROOT_DIR}/.venv_full_web"
HOST="${FULL_WEB_HOST:-127.0.0.1}"
PORT="${FULL_WEB_PORT:-9038}"

if [[ ! -x "${VENV_DIR}/bin/python" ]]; then
  echo "Missing ${VENV_DIR}. Run ./bootstrap_full_web.sh first."
  exit 1
fi

if [[ -z "${FULL_WEB_ANALYTICS_DB_PATH:-}" ]]; then
  DEFAULT_DB="${ROOT_DIR}/data/social_media_analytics.db"
  if [[ -f "${DEFAULT_DB}" ]]; then
    export FULL_WEB_ANALYTICS_DB_PATH="${DEFAULT_DB}"
  else
    cat <<EOF
Full-Web database not configured.

Either:
1. Put the database at:
   ${DEFAULT_DB}
or
2. Export:
   FULL_WEB_ANALYTICS_DB_PATH=/absolute/path/to/social_media_analytics.db
EOF
    exit 1
  fi
fi

echo "Starting Grad-Project with Full-Web entry on http://${HOST}:${PORT}"
echo "Using FULL_WEB_ANALYTICS_DB_PATH=${FULL_WEB_ANALYTICS_DB_PATH}"
"${VENV_DIR}/bin/python" -m uvicorn bridge:app --reload --host "${HOST}" --port "${PORT}"
