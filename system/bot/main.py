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


def _build_dispatcher(settings: Settings, store: QueueStore, bot: Bot) -> Dispatcher:
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
    stop_event = asyncio.Event()
    worker = Worker(
        settings=settings,
        store=store,
        bot=bot,
        runner=runner,
        stop_event=stop_event,
    )
    worker_task = asyncio.create_task(worker.run())
    dispatcher = _build_dispatcher(settings, store, bot)

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
