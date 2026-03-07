from __future__ import annotations

import asyncio
import logging
from contextlib import suppress

from aiogram import Bot
from aiogram.enums import ChatAction

from .codex_runner import CodexRunner
from .config import Settings
from .delivery import (
    ParsedAgentResponse,
    compose_task_result_text as _compose_task_result_text,
    deliver_agent_response,
    parse_agent_response as _parse_agent_response,
    resolve_file_path_for_send as _resolve_file_path_for_send,
    safe_send_text,
    trim as _trim,
)
from .prompts import build_prompt
from .queue_store import QueueStore, Task


class Worker:
    def __init__(
        self,
        settings: Settings,
        store: QueueStore,
        bot: Bot,
        runner: CodexRunner,
        stop_event: asyncio.Event,
    ) -> None:
        self._settings = settings
        self._store = store
        self._bot = bot
        self._runner = runner
        self._stop_event = stop_event
        self._logger = logging.getLogger("assistant.worker")

    async def run(self) -> None:
        while not self._stop_event.is_set():
            task = self._store.claim_next_task()
            if task is None:
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self._settings.idle_sleep_sec,
                    )
                continue
            await self._process_task(task)

    async def _acquire_user_session(self, chat_id: int) -> None:
        while not self._stop_event.is_set():
            if self._store.try_acquire_session_lease(
                chat_id=chat_id,
                owner="user",
                ttl_sec=self._settings.session_lease_sec,
            ):
                return
            await asyncio.sleep(0.5)

    async def _process_task(self, task: Task) -> None:
        self._logger.info("Processing task #%s", task.id)
        chat_session_id = self._store.get_chat_session_id(task.chat_id)
        prompt = build_prompt(
            user_text=task.text,
            attachments=task.attachments,
            include_bootstrap=not bool(chat_session_id),
        )
        await self._acquire_user_session(task.chat_id)
        try:
            await self._bot.send_chat_action(task.chat_id, ChatAction.TYPING)
        except Exception:  # pragma: no cover
            pass

        try:
            result = await asyncio.to_thread(
                self._runner.run,
                prompt,
                chat_session_id,
            )
            if result.session_id and result.session_id != chat_session_id:
                self._store.set_chat_session_id(task.chat_id, result.session_id)
                self._logger.info(
                    "Task #%s: chat=%s session set to %s",
                    task.id,
                    task.chat_id,
                    result.session_id,
                )
            if result.success:
                delivery = await deliver_agent_response(
                    bot=self._bot,
                    chat_id=task.chat_id,
                    settings=self._settings,
                    raw_message=result.message,
                    logger=self._logger,
                )
                task_result_text = _compose_task_result_text(
                    delivery.final_text,
                    delivery.sent_files,
                    delivery.send_errors,
                )
                task_result_text = _trim(task_result_text, self._settings.max_result_chars)
                self._store.complete_task(task.id, task_result_text)
                return

            error_text = _trim(
                f"Не удалось выполнить задачу #{task.id}.\n\n{result.message}",
                self._settings.max_result_chars,
            )
            self._store.fail_task(task.id, error_text)
            await safe_send_text(self._bot, task.chat_id, error_text, self._logger)
        finally:
            self._store.release_session_lease(task.chat_id, "user")
