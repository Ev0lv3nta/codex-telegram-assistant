from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import subprocess

from .config import Settings
from .queue_store import QueueStore


class GitOps:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._repo = settings.assistant_root

    def _run(self, args: list[str]) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", "-C", str(self._repo), *args],
            capture_output=True,
            text=True,
        )

    def _ensure_identity(self) -> None:
        name = self._run(["config", "--get", "user.name"]).stdout.strip()
        email = self._run(["config", "--get", "user.email"]).stdout.strip()
        if not name:
            self._run(["config", "user.name", self._settings.git_user_name])
        if not email:
            self._run(["config", "user.email", self._settings.git_user_email])

    def commit_if_needed(self, mode: str, task_id: int) -> str:
        if not self._settings.auto_commit:
            return "Auto-commit disabled."

        status = self._run(["status", "--porcelain"])
        if status.returncode != 0:
            return f"Commit skipped (git status failed): {status.stderr.strip()}"
        if not status.stdout.strip():
            return "No file changes."

        self._ensure_identity()
        self._run(["add", "-A"])
        commit = self._run(["commit", "-m", f"bot: {mode} task #{task_id}"])
        if commit.returncode != 0:
            return f"Commit failed: {commit.stderr.strip() or commit.stdout.strip()}"
        head = self._run(["rev-parse", "--short", "HEAD"]).stdout.strip()
        return f"Committed: `{head}`"

    def push_if_due(self, store: QueueStore) -> str:
        if not self._settings.auto_push:
            return "Auto-push disabled."

        now = datetime.now(timezone.utc)
        if now.hour < self._settings.auto_push_hour_utc:
            return "Push not due yet."

        today = now.strftime("%Y-%m-%d")
        last_attempt = store.get_meta("last_push_attempt_utc", "")
        if last_attempt == today:
            return "Push already attempted today."

        has_remote = self._run(["remote"]).stdout.strip()
        if not has_remote:
            store.set_meta("last_push_attempt_utc", today)
            return "Push skipped: no git remote configured."

        pushed = self._run(["push"])
        store.set_meta("last_push_attempt_utc", today)
        if pushed.returncode != 0:
            return f"Push failed: {pushed.stderr.strip() or pushed.stdout.strip()}"
        return "Push completed."

