from __future__ import annotations

import logging
import threading

from .codex_runner import CodexRunner
from .config import Settings
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
        stop_event: threading.Event,
    ) -> None:
        super().__init__(daemon=True)
        self._settings = settings
        self._store = store
        self._api = api
        self._runner = runner
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
        self._logger.info("Processing task #%s", task.id)
        chat_session_id = self._store.get_chat_session_id(task.chat_id)
        prompt = build_prompt(
            user_text=task.text,
            attachments=task.attachments,
            include_bootstrap=not bool(chat_session_id),
        )
        try:
            self._api.send_chat_action(task.chat_id, "typing")
        except Exception:  # pragma: no cover
            pass

        result = self._runner.run(prompt, session_id=chat_session_id)
        if result.session_id and result.session_id != chat_session_id:
            self._store.set_chat_session_id(task.chat_id, result.session_id)
            self._logger.info(
                "Task #%s: chat=%s session set to %s",
                task.id,
                task.chat_id,
                result.session_id,
            )
        if result.success:
            final_text = result.message.strip()
            final_text = _trim(final_text, self._settings.max_result_chars)
            self._store.complete_task(task.id, final_text)
            self._safe_send(task.chat_id, final_text)
            return

        error_text = _trim(
            f"Не удалось выполнить задачу #{task.id}.\n\n{result.message}",
            self._settings.max_result_chars,
        )
        self._store.fail_task(task.id, error_text)
        self._safe_send(task.chat_id, error_text)

    def _safe_send(self, chat_id: int, text: str) -> None:
        try:
            self._api.send_message(chat_id, text)
        except Exception as exc:  # pragma: no cover
            self._logger.error("Failed to send message to chat %s: %s", chat_id, exc)
