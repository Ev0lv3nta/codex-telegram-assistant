#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

mkdir -p system/logs system/runbooks

if [ -f system/runbooks/bot.pid ]; then
  old_pid="$(cat system/runbooks/bot.pid || true)"
  if [ -n "${old_pid:-}" ] && kill -0 "$old_pid" 2>/dev/null; then
    echo "Bot already running (pid=$old_pid)"
    exit 0
  fi
fi

set -a
source system/bot/.env
set +a

nohup python3 -m system.bot.main > system/logs/bot.log 2>&1 &
new_pid=$!
echo "$new_pid" > system/runbooks/bot.pid
sleep 1

if kill -0 "$new_pid" 2>/dev/null; then
  echo "Bot started in background (pid=$new_pid)"
else
  echo "Bot failed to start. Check system/logs/bot.log"
  exit 1
fi
