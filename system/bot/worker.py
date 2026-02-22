from __future__ import annotations

import asyncio
from dataclasses import dataclass
import logging
from pathlib import Path
import re
from contextlib import suppress

from aiogram import Bot
from aiogram.enums import ChatAction
from aiogram.types import FSInputFile

from .codex_runner import CodexRunner
from .config import Settings
from .prompts import build_prompt
from .queue_store import QueueStore, Task


SEND_FILE_LINE_RE = re.compile(r"^\s*\[\[send-file:(.+?)\]\]\s*$", re.IGNORECASE)


@dataclass(frozen=True)
class ParsedAgentResponse:
    text: str
    file_paths: list[str]


def _trim(text: str, limit: int) -> str:
    clean = text.strip()
    if len(clean) <= limit:
        return clean
    return clean[: limit - 120] + "\n\n[truncated]"


def _parse_agent_response(message: str) -> ParsedAgentResponse:
    text_lines: list[str] = []
    file_paths: list[str] = []
    seen_paths: set[str] = set()

    for raw_line in (message or "").splitlines():
        line = raw_line.rstrip()
        match = SEND_FILE_LINE_RE.match(line)
        if not match:
            text_lines.append(raw_line)
            continue

        path = match.group(1).strip()
        if not path:
            continue
        if path in seen_paths:
            continue
        seen_paths.add(path)
        file_paths.append(path)

    return ParsedAgentResponse(text="\n".join(text_lines).strip(), file_paths=file_paths)


def _normalize_send_path(raw_path: str) -> str:
    return raw_path.strip().strip("`").strip().strip("\"'")


def _resolve_file_path_for_send(
    assistant_root: Path,
    raw_path: str,
    max_size_bytes: int,
) -> tuple[Path | None, str]:
    cleaned_path = _normalize_send_path(raw_path)
    if not cleaned_path:
        return None, "Пустой путь к файлу."

    requested = Path(cleaned_path)
    resolved = (
        (assistant_root / requested).resolve()
        if not requested.is_absolute()
        else requested.resolve()
    )

    try:
        relative_path = resolved.relative_to(assistant_root).as_posix()
    except ValueError:
        return None, f"Путь вне рабочей директории: `{cleaned_path}`"

    if not resolved.exists():
        return None, f"Файл не найден: `{relative_path}`"
    if not resolved.is_file():
        return None, f"Это не файл: `{relative_path}`"

    size = resolved.stat().st_size
    if size > max_size_bytes:
        return None, (
            f"Файл слишком большой для отправки: `{relative_path}` "
            f"({size} bytes > {max_size_bytes} bytes)"
        )
    return resolved, relative_path


def _compose_task_result_text(text: str, sent_files: list[str], send_errors: list[str]) -> str:
    chunks: list[str] = []
    if text:
        chunks.append(text)
    if sent_files:
        chunks.append("Отправленные файлы:\n" + "\n".join(f"- {item}" for item in sent_files))
    if send_errors:
        chunks.append("Ошибки отправки файлов:\n" + "\n".join(f"- {item}" for item in send_errors))
    return "\n\n".join(chunks).strip() or "(empty)"


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

    async def _process_task(self, task: Task) -> None:
        self._logger.info("Processing task #%s", task.id)
        chat_session_id = self._store.get_chat_session_id(task.chat_id)
        prompt = build_prompt(
            user_text=task.text,
            attachments=task.attachments,
            include_bootstrap=not bool(chat_session_id),
        )
        try:
            await self._bot.send_chat_action(task.chat_id, ChatAction.TYPING)
        except Exception:  # pragma: no cover
            pass

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
            parsed = _parse_agent_response(result.message)
            final_text = _trim(parsed.text, self._settings.max_result_chars) if parsed.text else ""

            if final_text:
                await self._safe_send(task.chat_id, final_text)

            sent_files: list[str] = []
            send_errors: list[str] = []
            for raw_path in parsed.file_paths:
                sent_file, error_text = await self._safe_send_file(task.chat_id, raw_path)
                if sent_file:
                    sent_files.append(sent_file)
                if error_text:
                    send_errors.append(error_text)

            if (not final_text) and sent_files:
                await self._safe_send(task.chat_id, "Файл(ы) отправлены.")
            if send_errors:
                send_errors_text = _trim(
                    "Не удалось отправить некоторые файлы:\n" + "\n".join(f"- {item}" for item in send_errors),
                    self._settings.max_result_chars,
                )
                await self._safe_send(task.chat_id, send_errors_text)

            task_result_text = _compose_task_result_text(final_text, sent_files, send_errors)
            task_result_text = _trim(task_result_text, self._settings.max_result_chars)
            self._store.complete_task(task.id, task_result_text)
            return

        error_text = _trim(
            f"Не удалось выполнить задачу #{task.id}.\n\n{result.message}",
            self._settings.max_result_chars,
        )
        self._store.fail_task(task.id, error_text)
        await self._safe_send(task.chat_id, error_text)

    async def _safe_send(self, chat_id: int, text: str) -> None:
        try:
            await self._bot.send_message(
                chat_id=chat_id,
                text=text,
                disable_web_page_preview=True,
            )
        except Exception as exc:  # pragma: no cover
            self._logger.error("Failed to send message to chat %s: %s", chat_id, exc)

    async def _safe_send_file(self, chat_id: int, raw_path: str) -> tuple[str | None, str | None]:
        resolved_path, detail = _resolve_file_path_for_send(
            assistant_root=self._settings.assistant_root,
            raw_path=raw_path,
            max_size_bytes=self._settings.max_send_file_bytes,
        )
        if resolved_path is None:
            return None, detail
        relative_path = detail

        try:
            await self._bot.send_chat_action(chat_id, ChatAction.UPLOAD_DOCUMENT)
        except Exception:  # pragma: no cover
            pass

        try:
            await self._bot.send_document(
                chat_id=chat_id,
                document=FSInputFile(str(resolved_path)),
                caption=relative_path,
            )
            return relative_path, None
        except Exception as exc:  # pragma: no cover
            self._logger.error(
                "Failed to send document to chat %s: %s (path=%s)",
                chat_id,
                exc,
                resolved_path,
            )
            return None, f"Не удалось отправить `{relative_path}`: {exc}"
