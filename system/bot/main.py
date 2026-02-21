from __future__ import annotations

from pathlib import Path
import logging
import signal
import time
import traceback
import threading

from .classifier import (
    BUTTON_LABELS,
    Mode,
    classify_text,
    keyboard_markup,
    mode_from_label,
    parse_mode_command,
)
from .codex_runner import CodexRunner
from .config import Settings
from .git_ops import GitOps
from .ingest import download_attachments, write_inbox_markdown
from .queue_store import QueueStore
from .telegram_api import TelegramAPI, TelegramAPIError
from .worker import Worker


LOGGER = logging.getLogger("assistant.main")

HELP_TEXT = """
Ассистент подключен.

Как пользоваться:
- Пиши свободным текстом: бот сам определит режим.
- Кнопки меняют режим по умолчанию для чата.
- `/mode auto|intake|research|answer|finance|maintenance` меняет режим.
- `/status` показывает состояние очереди.

Все входящие сначала сохраняются в `00_inbox`, затем обрабатываются через Codex CLI.
""".strip()


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def _safe_mode(mode_value: str) -> Mode:
    try:
        return Mode(mode_value)
    except ValueError:
        return Mode.AUTO


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
    mode = _safe_mode(store.get_chat_mode(chat_id))
    mode_label = BUTTON_LABELS[mode]
    return (
        "Состояние бота:\n"
        f"- mode: {mode.value} ({mode_label})\n"
        f"- pending: {counts['pending']}\n"
        f"- running: {counts['running']}\n"
        f"- done: {counts['done']}\n"
        f"- failed: {counts['failed']}"
    )


def _resolve_mode(store: QueueStore, chat_id: int, text: str) -> Mode:
    chat_mode = _safe_mode(store.get_chat_mode(chat_id))
    if chat_mode is not Mode.AUTO:
        return chat_mode
    return classify_text(text)


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
        api.send_message(chat_id, HELP_TEXT, reply_markup=keyboard_markup())
        return

    if text == "/status":
        api.send_message(chat_id, _render_status(store, chat_id))
        return

    command_mode = parse_mode_command(text)
    if command_mode is not None:
        store.set_chat_mode(chat_id, command_mode.value)
        api.send_message(
            chat_id,
            f"Режим по умолчанию: `{command_mode.value}` ({BUTTON_LABELS[command_mode]})",
            reply_markup=keyboard_markup(),
        )
        return

    button_mode = mode_from_label(text)
    if button_mode is not None:
        store.set_chat_mode(chat_id, button_mode.value)
        api.send_message(
            chat_id,
            f"Режим переключен: `{button_mode.value}`",
            reply_markup=keyboard_markup(),
        )
        return

    attachments = download_attachments(api, settings.assistant_root, message)
    if not text and not attachments:
        return

    effective_mode = _resolve_mode(store, chat_id, text)
    inbox_path = write_inbox_markdown(
        assistant_root=settings.assistant_root,
        message=message,
        text=text,
        mode=effective_mode.value,
        attachments=attachments,
    )

    task_id = store.enqueue_task(
        chat_id=chat_id,
        user_id=user_id,
        username=username,
        mode=effective_mode.value,
        text=text,
        inbox_path=inbox_path,
        attachments=attachments,
    )

    api.send_message(
        chat_id,
        (
            f"Задача принята: #{task_id}\n"
            f"- mode: `{effective_mode.value}`\n"
            f"- inbox: `{inbox_path}`\n"
            "Обрабатываю в очереди."
        ),
    )


def run() -> None:
    settings = Settings.from_env()
    _setup_logging(settings.log_level)

    LOGGER.info("Assistant root: %s", settings.assistant_root)
    settings.state_db_path.parent.mkdir(parents=True, exist_ok=True)

    store = QueueStore(settings.state_db_path)
    api = TelegramAPI(settings.telegram_token)
    runner = CodexRunner(settings)
    git_ops = GitOps(settings)
    stop_event = threading.Event()

    worker = Worker(
        settings=settings,
        store=store,
        api=api,
        runner=runner,
        git_ops=git_ops,
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

