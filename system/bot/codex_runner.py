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

    def _build_command(self, prompt: str, output_path: Path) -> list[str]:
        command = [
            self._settings.codex_bin,
            "exec",
            "--skip-git-repo-check",
            "--cd",
            str(self._settings.assistant_root),
            "--output-last-message",
            str(output_path),
        ]
        if self._settings.codex_model:
            command.extend(["-m", self._settings.codex_model])
        if self._settings.codex_extra_args:
            command.extend(shlex.split(self._settings.codex_extra_args))
        command.append(prompt)
        return command

    def _run_once(self, command: list[str], timeout_sec: int) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            cwd=self._settings.assistant_root,
        )

    def run(self, prompt: str) -> CodexRunResult:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "last_message.txt"
            try:
                command = self._build_command(prompt, output_path)
                completed = self._run_once(command, self._settings.codex_timeout_sec)
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
