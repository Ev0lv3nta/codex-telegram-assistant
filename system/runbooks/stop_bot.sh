#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

if [ ! -f system/runbooks/bot.pid ]; then
  echo "Bot is not running (no pid file)"
  exit 0
fi

pid="$(cat system/runbooks/bot.pid || true)"
if [ -z "${pid:-}" ]; then
  echo "Invalid pid file"
  rm -f system/runbooks/bot.pid
  exit 0
fi

if kill -0 "$pid" 2>/dev/null; then
  kill "$pid"
  sleep 1
  if kill -0 "$pid" 2>/dev/null; then
    kill -9 "$pid" || true
  fi
  echo "Bot stopped (pid=$pid)"
else
  echo "Bot process already dead (pid=$pid)"
fi

rm -f system/runbooks/bot.pid

