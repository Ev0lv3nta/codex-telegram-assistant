from __future__ import annotations

from dataclasses import dataclass
import logging
import threading
import time

from .classifier import Mode
from .codex_runner import CodexRunner
from .config import Settings
from .git_ops import GitOps
from .prompts import build_prompt
from .queue_store import QueueStore, Task
from .telegram_api import TelegramAPI


def _trim(text: str, limit: int) -> str:
    clean = text.strip()
    if len(clean) <= limit:
        return clean
    return clean[: limit - 120] + "\n\n[truncated]"


class Worker(threading.Thread):
    def __init__(
        self,
        settings: Settings,
        store: QueueStore,
        api: TelegramAPI,
        runner: CodexRunner,
        git_ops: GitOps,
        stop_event: threading.Event,
    ) -> None:
        super().__init__(daemon=True)
        self._settings = settings
        self._store = store
        self._api = api
        self._runner = runner
        self._git_ops = git_ops
        self._stop_event = stop_event
        self._logger = logging.getLogger("assistant.worker")

    def run(self) -> None:
        while not self._stop_event.is_set():
            task = self._store.claim_next_task()
            if task is None:
                self._stop_event.wait(self._settings.idle_sleep_sec)
                continue
            self._process_task(task)

    def _process_task(self, task: Task) -> None:
        try:
            mode = Mode(task.mode)
        except ValueError:
            mode = Mode.INTAKE

        self._logger.info("Processing task #%s in mode=%s", task.id, mode.value)
        prompt = build_prompt(
            mode=mode,
            user_text=task.text,
            inbox_path=task.inbox_path,
            attachments=task.attachments,
        )
        try:
            self._api.send_chat_action(task.chat_id, "typing")
        except Exception:  # pragma: no cover
            pass

        result = self._runner.run(prompt)
        if result.success:
            commit_note = self._git_ops.commit_if_needed(mode.value, task.id)
            push_note = self._git_ops.push_if_due(self._store)
            final_text = (
                f"Task #{task.id} completed.\n\n"
                f"{result.message.strip()}\n\n"
                f"Git:\n- {commit_note}\n- {push_note}"
            )
            final_text = _trim(final_text, self._settings.max_result_chars)
            self._store.complete_task(task.id, final_text)
            self._safe_send(task.chat_id, final_text)
            return

        error_text = _trim(
            f"Task #{task.id} failed.\n\n{result.message}",
            self._settings.max_result_chars,
        )
        self._store.fail_task(task.id, error_text)
        self._safe_send(task.chat_id, error_text)

    def _safe_send(self, chat_id: int, text: str) -> None:
        try:
            self._api.send_message(chat_id, text)
        except Exception as exc:  # pragma: no cover
            self._logger.error("Failed to send message to chat %s: %s", chat_id, exc)

