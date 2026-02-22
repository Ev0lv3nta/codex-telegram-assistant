from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message, ReplyKeyboardRemove

from .codex_runner import CodexRunner
from .config import Settings
from .ingest import download_attachments
from .queue_store import QueueStore
from .session_gc import gc_sessions
from .stt_openrouter import OpenRouterSttClient
from .worker import Worker


LOGGER = logging.getLogger("assistant.main")

HELP_TEXT = """
Ассистент подключен.

Как пользоваться:
- Пиши свободным текстом, как обычному личному ассистенту.
- Можно прикладывать файлы/фото/голосовые.
- `/status` показывает состояние очереди.
- `/reset` сбрасывает сессию чата (начать новый контекст).
- `/gc [days]` чистит старые Codex-сессии на диске (по умолчанию 7 дней).

Это прямой шлюз в Codex CLI: обычные сообщения обрабатываются как чат,
а изменения файлов/кода делаются по явной просьбе.
""".strip()


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def _is_authorized(settings: Settings, chat_id: int, user_id: int) -> bool:
    if settings.allowed_user_ids and user_id not in settings.allowed_user_ids:
        return False
    if settings.allowed_chat_ids and chat_id not in settings.allowed_chat_ids:
        return False
    return True


def _extract_text(message: Message) -> str:
    return (message.text or message.caption or "").strip()


def _pick_audio_attachment(assistant_root: Path, attachments: list[str]) -> Path | None:
    audio_suffixes = {".ogg", ".oga", ".opus", ".wav", ".mp3", ".m4a", ".flac", ".webm"}
    for rel_path in attachments:
        candidate = (assistant_root / rel_path).resolve()
        if candidate.suffix.lower() not in audio_suffixes:
            continue
        try:
            candidate.relative_to(assistant_root)
        except ValueError:
            continue
        return candidate
    return None


async def _transcribe_voice_if_needed(
    message: Message,
    settings: Settings,
    stt_client: OpenRouterSttClient,
    text: str,
    attachments: list[str],
) -> tuple[str, str, str | None]:
    if message.voice is None and message.audio is None:
        return text, "", None
    if not attachments:
        return text, "Голосовое не обработано: вложение не найдено.", None

    audio_path = _pick_audio_attachment(settings.assistant_root, attachments)
    if audio_path is None:
        return text, "Голосовое не обработано: не найден поддерживаемый аудиофайл.", None
    audio_rel_path = audio_path.relative_to(settings.assistant_root).as_posix()

    duration_sec = 0
    if message.voice is not None and getattr(message.voice, "duration", None):
        duration_sec = int(message.voice.duration)
    elif message.audio is not None and getattr(message.audio, "duration", None):
        duration_sec = int(message.audio.duration)

    result = await asyncio.to_thread(stt_client.transcribe_file, audio_path, duration_sec)
    if result.success and result.text:
        transcript = result.text.strip()
        if text:
            merged_text = f"{text}\n\n[Расшифровка голосового]\n{transcript}"
            return merged_text, "", audio_rel_path
        return transcript, "", audio_rel_path

    if text:
        return text, f"Голосовое не обработано: {result.error or 'неизвестная ошибка.'}", audio_rel_path
    return "", f"Голосовое не обработано: {result.error or 'неизвестная ошибка.'}", audio_rel_path


def _delete_attachment_file(assistant_root: Path, rel_path: str) -> None:
    file_path = (assistant_root / rel_path).resolve()
    try:
        file_path.relative_to(assistant_root)
    except ValueError:
        LOGGER.warning("Skip deleting file outside assistant root: %s", rel_path)
        return
    try:
        file_path.unlink(missing_ok=True)
    except OSError as exc:
        LOGGER.warning("Failed to delete processed attachment %s: %s", rel_path, exc)


def _render_status(store: QueueStore, chat_id: int) -> str:
    counts = store.counts()
    session_id = store.get_chat_session_id(chat_id) or "(нет)"
    return (
        "Состояние бота:\n"
        f"- session: {session_id}\n"
        f"- pending: {counts['pending']}\n"
        f"- running: {counts['running']}\n"
        f"- done: {counts['done']}\n"
        f"- failed: {counts['failed']}"
    )


def _parse_gc_days(text: str) -> int | None:
    parts = text.strip().split(maxsplit=1)
    if len(parts) == 1:
        return 7
    try:
        return int(parts[1].strip())
    except ValueError:
        return None


def _build_dispatcher(
    settings: Settings,
    store: QueueStore,
    bot: Bot,
    stt_client: OpenRouterSttClient,
) -> Dispatcher:
    dp = Dispatcher()

    async def _guard(message: Message) -> bool:
        sender = message.from_user
        if sender is None:
            return False
        chat_id = int(message.chat.id)
        user_id = int(sender.id)
        if _is_authorized(settings, chat_id, user_id):
            return True
        LOGGER.warning("Unauthorized access attempt: chat=%s user=%s", chat_id, user_id)
        return False

    @dp.message(Command("start"))
    async def on_start(message: Message) -> None:
        if not await _guard(message):
            return
        await message.answer(
            HELP_TEXT,
            reply_markup=ReplyKeyboardRemove(),
            disable_web_page_preview=True,
        )

    @dp.message(Command("status"))
    async def on_status(message: Message) -> None:
        if not await _guard(message):
            return
        await message.answer(_render_status(store, int(message.chat.id)))

    @dp.message(Command("reset"))
    async def on_reset(message: Message) -> None:
        if not await _guard(message):
            return
        chat_id = int(message.chat.id)
        store.clear_chat_session_id(chat_id)
        await message.answer("Сессия этого чата сброшена. Следующее сообщение начнет новый контекст.")

    @dp.message(Command("gc"))
    async def on_gc(message: Message) -> None:
        if not await _guard(message):
            return
        days = _parse_gc_days(message.text or "/gc")
        if days is None:
            await message.answer("Использование: /gc [days]  (пример: /gc 30)")
            return
        keep = store.list_chat_session_ids()
        sessions_dir = Path("/root/.codex/sessions")
        result = gc_sessions(
            sessions_dir=sessions_dir,
            keep_session_ids=keep,
            older_than_days=days,
        )
        await message.answer(
            "GC Codex sessions:\n"
            f"- deleted: {result.deleted_files}\n"
            f"- kept(active): {result.kept_files}\n"
            f"- skipped(recent): {result.skipped_files}\n"
            f"- errors: {result.errors}"
        )

    @dp.message()
    async def on_message(message: Message) -> None:
        if not await _guard(message):
            return

        sender = message.from_user
        if sender is None:
            return
        chat_id = int(message.chat.id)
        user_id = int(sender.id)
        username = (
            sender.username
            or " ".join(
                token for token in [sender.first_name, sender.last_name] if token
            )
            or "unknown"
        )

        text = _extract_text(message)
        attachments = await download_attachments(bot, settings.assistant_root, message)
        text, voice_error, transcribed_audio_rel_path = await _transcribe_voice_if_needed(
            message=message,
            settings=settings,
            stt_client=stt_client,
            text=text,
            attachments=attachments,
        )
        if message.voice is not None and transcribed_audio_rel_path:
            _delete_attachment_file(settings.assistant_root, transcribed_audio_rel_path)
            attachments = [item for item in attachments if item != transcribed_audio_rel_path]

        if voice_error:
            await message.answer(voice_error)
            LOGGER.warning(
                "Voice transcription failed for chat=%s user=%s: %s",
                chat_id,
                user_id,
                voice_error,
            )
            if not text:
                return
        if not text and not attachments:
            return

        task_id = store.enqueue_task(
            chat_id=chat_id,
            user_id=user_id,
            username=username,
            text=text,
            attachments=attachments,
        )
        LOGGER.info("Accepted task #%s from chat=%s", task_id, chat_id)

    return dp


async def _run_async() -> None:
    settings = Settings.from_env()
    _setup_logging(settings.log_level)
    LOGGER.info("Assistant root: %s", settings.assistant_root)

    settings.state_db_path.parent.mkdir(parents=True, exist_ok=True)
    store = QueueStore(settings.state_db_path)
    bot = Bot(token=settings.telegram_token)
    runner = CodexRunner(settings)
    stt_client = OpenRouterSttClient(settings)
    stop_event = asyncio.Event()
    worker = Worker(
        settings=settings,
        store=store,
        bot=bot,
        runner=runner,
        stop_event=stop_event,
    )
    worker_task = asyncio.create_task(worker.run())
    dispatcher = _build_dispatcher(settings, store, bot, stt_client)

    try:
        await dispatcher.start_polling(
            bot,
            polling_timeout=settings.poll_timeout_sec,
            allowed_updates=["message"],
        )
    finally:
        stop_event.set()
        await worker_task
        store.close()
        await bot.session.close()


def run() -> None:
    asyncio.run(_run_async())


if __name__ == "__main__":
    run()
