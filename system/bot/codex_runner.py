from __future__ import annotations

from dataclasses import dataclass
import json
import shlex
import subprocess

from .config import Settings


@dataclass
class CodexRunResult:
    success: bool
    message: str
    session_id: str


class CodexRunner:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def _append_common_options(self, command: list[str]) -> None:
        if self._settings.codex_model:
            command.extend(["-m", self._settings.codex_model])
        if self._settings.codex_extra_args:
            command.extend(shlex.split(self._settings.codex_extra_args))

    def _build_exec_command(self, prompt: str) -> list[str]:
        command = [
            self._settings.codex_bin,
            "exec",
            "--json",
            "--skip-git-repo-check",
            "--cd",
            str(self._settings.assistant_root),
        ]
        self._append_common_options(command)
        command.append(prompt)
        return command

    def _build_resume_command(self, session_id: str, prompt: str) -> list[str]:
        command = [
            self._settings.codex_bin,
            "exec",
            "resume",
            "--json",
            "--skip-git-repo-check",
        ]
        self._append_common_options(command)
        command.append(session_id)
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

    @staticmethod
    def _parse_json_output(stdout: str) -> tuple[str, str]:
        session_id = ""
        last_agent_message = ""
        for raw_line in stdout.splitlines():
            line = raw_line.strip()
            if not line.startswith("{"):
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("type") == "thread.started":
                session_id = str(event.get("thread_id") or session_id)
                continue
            if event.get("type") != "item.completed":
                continue
            item = event.get("item") or {}
            if item.get("type") == "agent_message":
                text = str(item.get("text") or "").strip()
                if text:
                    last_agent_message = text
        return session_id, last_agent_message

    @staticmethod
    def _fallback_text(stdout: str, stderr: str) -> str:
        merged = ((stdout or "") + "\n" + (stderr or "")).strip()
        return merged or "(empty Codex response)"

    @staticmethod
    def _non_json_lines(text: str) -> list[str]:
        result: list[str] = []
        for raw_line in (text or "").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if line.startswith("{") and line.endswith("}"):
                continue
            result.append(line)
        return result

    @classmethod
    def _failure_text(cls, stdout: str, stderr: str) -> str:
        candidates: list[str] = []
        candidates.extend(cls._non_json_lines(stderr))
        candidates.extend(cls._non_json_lines(stdout))
        if not candidates:
            return "Не удалось выполнить запрос в Codex CLI."
        details = "\n".join(candidates[:6])
        return f"Не удалось выполнить запрос в Codex CLI.\n\n{details}"

    @classmethod
    def _success_text(cls, parsed_message: str, stdout: str) -> str:
        if parsed_message:
            return parsed_message
        lines = cls._non_json_lines(stdout)
        if lines:
            return "\n".join(lines[:6])
        return "(empty Codex response)"

    def run(self, prompt: str, session_id: str = "") -> CodexRunResult:
        try:
            if session_id:
                command = self._build_resume_command(session_id=session_id, prompt=prompt)
            else:
                command = self._build_exec_command(prompt=prompt)
            completed = self._run_once(command, self._settings.codex_timeout_sec)
        except FileNotFoundError:
            return CodexRunResult(False, "Failed to run codex: binary not found", "")
        except subprocess.TimeoutExpired:
            return CodexRunResult(False, "Codex execution timed out", session_id)

        parsed_session_id, parsed_message = self._parse_json_output(completed.stdout or "")
        effective_session_id = parsed_session_id or session_id
        if completed.returncode != 0:
            return CodexRunResult(
                False,
                self._failure_text(completed.stdout or "", completed.stderr or ""),
                effective_session_id,
            )
        message = self._success_text(parsed_message, completed.stdout or "")
        return CodexRunResult(True, message, effective_session_id)
