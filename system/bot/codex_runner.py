from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shlex
import subprocess
import tempfile

from .config import Settings


@dataclass
class CodexRunResult:
    success: bool
    message: str


class CodexRunner:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def run(self, prompt: str) -> CodexRunResult:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "last_message.txt"
            command = [
                self._settings.codex_bin,
                "exec",
                "--skip-git-repo-check",
                "--cd",
                str(self._settings.assistant_root),
                "--output-last-message",
                str(output_path),
            ]
            if self._settings.codex_search_enabled:
                command.append("--search")
            if self._settings.codex_model:
                command.extend(["-m", self._settings.codex_model])
            if self._settings.codex_extra_args:
                command.extend(shlex.split(self._settings.codex_extra_args))
            command.append(prompt)

            try:
                completed = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    timeout=self._settings.codex_timeout_sec,
                    cwd=self._settings.assistant_root,
                )
            except FileNotFoundError:
                return CodexRunResult(False, "Failed to run codex: binary not found")
            except subprocess.TimeoutExpired:
                return CodexRunResult(False, "Codex execution timed out")

            fallback = (completed.stdout or "") + "\n" + (completed.stderr or "")
            fallback = fallback.strip()
            message = (
                output_path.read_text(encoding="utf-8").strip()
                if output_path.exists()
                else fallback
            )
            if not message:
                message = "(empty Codex response)"

            if completed.returncode != 0:
                return CodexRunResult(False, message)
            return CodexRunResult(True, message)

