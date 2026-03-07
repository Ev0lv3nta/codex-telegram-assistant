from __future__ import annotations

from dataclasses import dataclass
import logging
from pathlib import Path
import re

from aiogram import Bot
from aiogram.enums import ChatAction
from aiogram.types import FSInputFile, ReplyKeyboardRemove

from .config import Settings


SEND_FILE_LINE_RE = re.compile(r"^\s*\[\[send-file:(.+?)\]\]\s*$", re.IGNORECASE)


@dataclass(frozen=True)
class ParsedAgentResponse:
    text: str
    file_paths: list[str]


@dataclass(frozen=True)
class DeliveryOutcome:
    final_text: str
    sent_files: list[str]
    send_errors: list[str]


def trim(text: str, limit: int) -> str:
    clean = text.strip()
    if len(clean) <= limit:
        return clean
    return clean[: limit - 120] + "\n\n[truncated]"


def parse_agent_response(message: str) -> ParsedAgentResponse:
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
        if not path or path in seen_paths:
            continue
        seen_paths.add(path)
        file_paths.append(path)

    return ParsedAgentResponse(text="\n".join(text_lines).strip(), file_paths=file_paths)


def normalize_send_path(raw_path: str) -> str:
    return raw_path.strip().strip("`").strip().strip("\"'")


def resolve_file_path_for_send(
    assistant_root: Path,
    raw_path: str,
    max_size_bytes: int,
) -> tuple[Path | None, str]:
    cleaned_path = normalize_send_path(raw_path)
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


def compose_task_result_text(text: str, sent_files: list[str], send_errors: list[str]) -> str:
    chunks: list[str] = []
    if text:
        chunks.append(text)
    if sent_files:
        chunks.append("Отправленные файлы:\n" + "\n".join(f"- {item}" for item in sent_files))
    if send_errors:
        chunks.append("Ошибки отправки файлов:\n" + "\n".join(f"- {item}" for item in send_errors))
    return "\n\n".join(chunks).strip() or "(empty)"


async def safe_send_text(bot: Bot, chat_id: int, text: str, logger: logging.Logger) -> None:
    try:
        await bot.send_message(
            chat_id=chat_id,
            text=text,
            disable_web_page_preview=True,
            reply_markup=ReplyKeyboardRemove(),
        )
    except Exception as exc:  # pragma: no cover
        logger.error("Failed to send message to chat %s: %s", chat_id, exc)


async def safe_send_file(
    bot: Bot,
    chat_id: int,
    settings: Settings,
    raw_path: str,
    logger: logging.Logger,
) -> tuple[str | None, str | None]:
    resolved_path, detail = resolve_file_path_for_send(
        assistant_root=settings.assistant_root,
        raw_path=raw_path,
        max_size_bytes=settings.max_send_file_bytes,
    )
    if resolved_path is None:
        return None, detail
    relative_path = detail

    try:
        await bot.send_chat_action(chat_id, ChatAction.UPLOAD_DOCUMENT)
    except Exception:  # pragma: no cover
        pass

    try:
        await bot.send_document(
            chat_id=chat_id,
            document=FSInputFile(str(resolved_path)),
            caption=relative_path,
        )
        return relative_path, None
    except Exception as exc:  # pragma: no cover
        logger.error(
            "Failed to send document to chat %s: %s (path=%s)",
            chat_id,
            exc,
            resolved_path,
        )
        return None, f"Не удалось отправить `{relative_path}`: {exc}"


async def deliver_agent_response(
    bot: Bot,
    chat_id: int,
    settings: Settings,
    raw_message: str,
    logger: logging.Logger,
    *,
    text_prefix: str = "",
    files_only_fallback: str = "Файл(ы) отправлены.",
) -> DeliveryOutcome:
    parsed = parse_agent_response(raw_message)
    final_text = trim(parsed.text, settings.max_result_chars) if parsed.text else ""
    if final_text and text_prefix:
        final_text = f"{text_prefix}{final_text}"

    if final_text:
        await safe_send_text(bot, chat_id, final_text, logger)

    sent_files: list[str] = []
    send_errors: list[str] = []
    for raw_path in parsed.file_paths:
        sent_file, error_text = await safe_send_file(bot, chat_id, settings, raw_path, logger)
        if sent_file:
            sent_files.append(sent_file)
        if error_text:
            send_errors.append(error_text)

    if (not final_text) and sent_files:
        await safe_send_text(bot, chat_id, files_only_fallback, logger)
    if send_errors:
        send_errors_text = trim(
            "Не удалось отправить некоторые файлы:\n" + "\n".join(f"- {item}" for item in send_errors),
            settings.max_result_chars,
        )
        await safe_send_text(bot, chat_id, send_errors_text, logger)

    return DeliveryOutcome(final_text=final_text, sent_files=sent_files, send_errors=send_errors)
