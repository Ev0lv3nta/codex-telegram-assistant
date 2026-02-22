from __future__ import annotations

import logging
import signal
import time
import traceback
import threading
from pathlib import Path

from .codex_runner import CodexRunner
from .config import Settings
from .ingest import download_attachments
from .queue_store import QueueStore
from .session_gc import gc_sessions
from .telegram_api import TelegramAPI, TelegramAPIError
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


def _extract_text(message: dict) -> str:
    return (message.get("text") or message.get("caption") or "").strip()


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


def _handle_message(
    settings: Settings,
    store: QueueStore,
    api: TelegramAPI,
    message: dict,
) -> None:
    chat = message.get("chat") or {}
    sender = message.get("from") or {}

    chat_id = int(chat.get("id", 0))
    user_id = int(sender.get("id", 0))
    username = (
        sender.get("username")
        or " ".join(
            token for token in [sender.get("first_name"), sender.get("last_name")] if token
        )
        or "unknown"
    )

    if not _is_authorized(settings, chat_id, user_id):
        LOGGER.warning("Unauthorized access attempt: chat=%s user=%s", chat_id, user_id)
        return

    text = _extract_text(message)

    if text == "/start":
        api.send_message(chat_id, HELP_TEXT, reply_markup={"remove_keyboard": True})
        return

    if text == "/status":
        api.send_message(chat_id, _render_status(store, chat_id))
        return

    if text == "/reset":
        store.clear_chat_session_id(chat_id)
        api.send_message(chat_id, "Сессия этого чата сброшена. Следующее сообщение начнет новый контекст.")
        return

    if text.strip().lower().startswith("/gc"):
        parts = text.strip().split(maxsplit=1)
        days = 7
        if len(parts) == 2:
            try:
                days = int(parts[1].strip())
            except ValueError:
                api.send_message(chat_id, "Использование: /gc [days]  (пример: /gc 30)")
                return

        keep = store.list_chat_session_ids()
        sessions_dir = Path("/root/.codex/sessions")
        result = gc_sessions(sessions_dir=sessions_dir, keep_session_ids=keep, older_than_days=days)
        api.send_message(
            chat_id,
            (
                "GC Codex sessions:\n"
                f"- deleted: {result.deleted_files}\n"
                f"- kept(active): {result.kept_files}\n"
                f"- skipped(recent): {result.skipped_files}\n"
                f"- errors: {result.errors}"
            ),
        )
        return

    attachments = download_attachments(api, settings.assistant_root, message)
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


def run() -> None:
    settings = Settings.from_env()
    _setup_logging(settings.log_level)

    LOGGER.info("Assistant root: %s", settings.assistant_root)
    settings.state_db_path.parent.mkdir(parents=True, exist_ok=True)

    store = QueueStore(settings.state_db_path)
    api = TelegramAPI(settings.telegram_token)
    runner = CodexRunner(settings)
    stop_event = threading.Event()

    worker = Worker(
        settings=settings,
        store=store,
        api=api,
        runner=runner,
        stop_event=stop_event,
    )
    worker.start()

    def _shutdown(signum: int, _frame: object) -> None:
        LOGGER.info("Shutdown signal received: %s", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    last_update_id = int(store.get_meta("last_update_id", "0") or "0")
    LOGGER.info("Starting polling from update_id=%s", last_update_id + 1)

    try:
        while not stop_event.is_set():
            try:
                updates = api.get_updates(
                    offset=last_update_id + 1,
                    timeout_sec=settings.poll_timeout_sec,
                )
            except TelegramAPIError as exc:
                LOGGER.error("Telegram API error: %s", exc)
                time.sleep(3)
                continue
            except Exception as exc:  # pragma: no cover
                LOGGER.error("Unexpected polling error: %s", exc)
                traceback.print_exc()
                time.sleep(3)
                continue

            if not updates:
                continue

            for update in updates:
                update_id = int(update.get("update_id", 0))
                if update_id > last_update_id:
                    last_update_id = update_id
                    store.set_meta("last_update_id", str(last_update_id))

                message = update.get("message")
                if not message:
                    continue

                try:
                    _handle_message(settings, store, api, message)
                except Exception as exc:  # pragma: no cover
                    LOGGER.error("Failed to handle message: %s", exc)
                    traceback.print_exc()
    finally:
        stop_event.set()
        worker.join(timeout=5)
        store.close()


if __name__ == "__main__":
    run()
