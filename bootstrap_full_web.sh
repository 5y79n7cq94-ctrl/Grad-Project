#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${ROOT_DIR}/.venv_full_web"
PYTHON_BIN="${PYTHON_BIN:-python3}"

if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
  echo "Python not found: ${PYTHON_BIN}"
  exit 1
fi

echo "Creating Full-Web virtual environment at ${VENV_DIR}"
"${PYTHON_BIN}" -m venv "${VENV_DIR}"

echo "Upgrading pip"
"${VENV_DIR}/bin/python" -m pip install --upgrade pip

echo "Installing dependencies from requirements_extra.txt"
"${VENV_DIR}/bin/python" -m pip install -r "${ROOT_DIR}/requirements_extra.txt"

cat <<EOF

Bootstrap complete.

Next:
1. Put your Full-Web database at:
   ${ROOT_DIR}/data/social_media_analytics.db
   or export FULL_WEB_ANALYTICS_DB_PATH=/absolute/path/to/social_media_analytics.db
2. Start the app with:
   ./run_full_web_sidecar.sh

EOF
