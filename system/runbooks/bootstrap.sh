#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

if [ ! -d ".venv" ]; then
  python3 -m venv .venv
fi

source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r system/bot/requirements.txt

if [ ! -f "system/bot/.env" ]; then
  cp system/bot/.env.example system/bot/.env
  echo "Created system/bot/.env from example. Fill TG_BOT_TOKEN before run."
fi

echo "Bootstrap complete."

