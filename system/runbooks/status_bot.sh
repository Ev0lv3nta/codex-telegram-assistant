#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

if [ ! -f system/runbooks/bot.pid ]; then
  echo "status=stopped"
  exit 0
fi

pid="$(cat system/runbooks/bot.pid || true)"
if [ -n "${pid:-}" ] && kill -0 "$pid" 2>/dev/null; then
  echo "status=running pid=$pid"
else
  echo "status=stopped stale_pid=$pid"
fi

