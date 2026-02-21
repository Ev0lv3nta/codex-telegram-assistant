#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

if [ ! -d ".venv" ]; then
  echo "Missing .venv. Run system/runbooks/bootstrap.sh first."
  exit 1
fi

source .venv/bin/activate
python -m system.bot.main

