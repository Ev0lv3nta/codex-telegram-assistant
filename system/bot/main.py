from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    BotCommand,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    ReplyKeyboardRemove,
)

from .autonomy_store import AutonomyStore
from .autonomy_worker import AutonomyWorker
from .autonomy_requests import ensure_autonomy_requests_scaffold
from .codex_runner import CodexRunner
from .config import Settings
from .ingest import download_attachments
from .memory_store import ensure_memory_scaffold
from .queue_store import QueueStore
from .self_restart import (
    consume_restart_notification_target,
    mark_restart_observed,
    request_service_restart,
)
from .session_gc import gc_sessions
from .stt_openrouter import OpenRouterSttClient
from .worker import Worker


LOGGER = logging.getLogger("assistant.main")
BOT_SERVICE_NAME = "personal-assistant-bot.service"
PULSE_CALLBACK_DATA = "autonomy:pulse"
PULSE_SNOOZE_CALLBACK_DATA = "autonomy:pulse:snooze:6h"
PULSE_WAKE_CALLBACK_DATA = "autonomy:pulse:wake:now"
PULSE_STOP_CALLBACK_DATA = "autonomy:pulse:stop"
PULSE_START_CALLBACK_DATA = "autonomy:pulse:start"
MSK = ZoneInfo("Europe/Moscow")

HELP_TEXT = """
Ассистент подключен.

Как пользоваться:
- Пиши свободным текстом, как обычному личному ассистенту.
- Можно прикладывать файлы/фото/голосовые.
- `/pulse` показывает короткий owner-facing пульс автономности.
- `/status` показывает состояние очереди.
- `/autonomy` показывает последние автономные задачи.
- `/reset` сбрасывает сессию чата (начать новый контекст).
- `/gc [days]` чистит старые Codex-сессии на диске (по умолчанию 7 дней).
- `/restart` перезапускает сервис бота.

Это прямой шлюз в Codex CLI: обычные сообщения обрабатываются как чат,
а изменения файлов/кода делаются по явной просьбе.
""".strip()


def _build_bot_commands() -> list[BotCommand]:
    return [
        BotCommand(command="start", description="Старт"),
        BotCommand(command="pulse", description="Пульс автономности"),
        BotCommand(command="status", description="Статус"),
        BotCommand(command="autonomy", description="Автономность"),
        BotCommand(command="restart", description="Перезапуск"),
    ]


def _allowed_update_types() -> list[str]:
    return ["message", "callback_query"]


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


def _note_chat_activity_from_message(store: QueueStore, message: Message) -> None:
    store.note_chat_activity(int(message.chat.id))


def _note_passive_owner_touch(
    store: QueueStore,
    message: Message,
) -> None:
    store.note_chat_activity(int(message.chat.id))


def _nudge_autonomy_wakeup(
    autonomy_store: AutonomyStore,
    message: Message,
    wake_event: asyncio.Event | None = None,
) -> None:
    if autonomy_store.autonomy_paused(int(message.chat.id)):
        return
    autonomy_store.clear_idle_snooze(int(message.chat.id))
    autonomy_store.schedule_next_wakeup_in(int(message.chat.id), 0)
    if wake_event is not None:
        wake_event.set()


def _schedule_autonomy_snooze(autonomy_store: AutonomyStore, chat_id: int, *, hours: int) -> str:
    until = (datetime.now(timezone.utc) + timedelta(hours=max(1, hours))).isoformat()
    autonomy_store.mark_idle_snooze_until(chat_id, until)
    autonomy_store.set_next_wakeup(chat_id, until)
    autonomy_store.set_mode(chat_id, "sleeping_idle")
    return until


def _wake_autonomy_now(
    autonomy_store: AutonomyStore,
    chat_id: int,
    wake_event: asyncio.Event | None = None,
) -> None:
    autonomy_store.set_autonomy_paused(chat_id, False)
    autonomy_store.clear_idle_snooze(chat_id)
    autonomy_store.schedule_next_wakeup_in(chat_id, 0)
    autonomy_store.set_mode(chat_id, "idle")
    if wake_event is not None:
        wake_event.set()


def _stop_autonomy_now(autonomy_store: AutonomyStore, chat_id: int) -> None:
    autonomy_store.set_autonomy_paused(chat_id, True)
    autonomy_store.clear_idle_snooze(chat_id)
    autonomy_store.clear_next_wakeup(chat_id)
    autonomy_store.set_mode(chat_id, "stopped")


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


def _render_status(store: QueueStore, autonomy_store: AutonomyStore, chat_id: int) -> str:
    counts = store.counts()
    autonomy_counts = autonomy_store.counts()
    session_id = store.get_chat_session_id(chat_id) or "(нет)"
    session_owner = store.get_session_owner(chat_id) or "(свободна)"
    heartbeat = autonomy_store.get_heartbeat("loop") or "(не было)"
    return (
        "Состояние бота:\n"
        f"- session: {session_id}\n"
        f"- session owner: {session_owner}\n"
        f"- pending: {counts['pending']}\n"
        f"- running: {counts['running']}\n"
        f"- done: {counts['done']}\n"
        f"- failed: {counts['failed']}\n"
        f"- autonomy pending: {autonomy_counts['pending']}\n"
        f"- autonomy running: {autonomy_counts['running']}\n"
        f"- autonomy waiting_user: {autonomy_counts['waiting_user']}\n"
        f"- autonomy done: {autonomy_counts['done']}\n"
        f"- autonomy failed: {autonomy_counts['failed']}\n"
        f"- autonomy heartbeat: {heartbeat}"
    )


def _parse_iso_dt(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _format_eta_from_heartbeat(last_at: str, heartbeat_sec: int) -> str:
    if not last_at:
        return "(ожидание первого heartbeat)"
    last_dt = _parse_iso_dt(last_at)
    if last_dt is None:
        return "(неизвестно)"
    next_dt = last_dt + timedelta(seconds=heartbeat_sec)
    remaining = next_dt - datetime.now(next_dt.tzinfo)
    if remaining.total_seconds() <= 0:
        return "due now"
    total_seconds = int(remaining.total_seconds())
    minutes, seconds = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"in {hours}h {minutes}m"
    if minutes > 0:
        return f"in {minutes}m {seconds}s"
    return f"in {seconds}s"


def _format_owner_moment(value: str) -> str:
    parsed = _parse_iso_dt(value)
    if parsed is None:
        return value
    if parsed.tzinfo is None:
        return value
    msk_dt = parsed.astimezone(MSK)
    return msk_dt.strftime("%d.%m %H:%M MSK")


def _render_autonomy_status(
    autonomy_store: AutonomyStore,
    chat_id: int,
    heartbeat_sec: int,
) -> str:
    counts = autonomy_store.counts_for_chat(chat_id)
    tasks = autonomy_store.list_tasks(chat_id=chat_id, limit=5, order_by="recent")
    heartbeat = autonomy_store.get_heartbeat("loop") or "(не было)"
    heartbeat_kind = autonomy_store.get_last_heartbeat_kind() or "(не было)"
    heartbeat_at = autonomy_store.get_last_heartbeat_at()
    mode = autonomy_store.get_mode(chat_id) or "(не задан)"
    next_wakeup = autonomy_store.get_next_wakeup(chat_id) or "(не задан)"
    mission = autonomy_store.get_active_mission(chat_id)
    notify_last_sent = autonomy_store.get_notify_last_sent(chat_id) or "(не было)"
    lines = [
        "Автономность:",
        f"- pending: {counts['pending']}",
        f"- running: {counts['running']}",
        f"- waiting_user: {counts['waiting_user']}",
        f"- done: {counts['done']}",
        f"- failed: {counts['failed']}",
        f"- mode: {mode}",
        f"- next wakeup: {next_wakeup}",
        f"- heartbeat: {heartbeat}",
        f"- last heartbeat status: {heartbeat_kind}",
        f"- next heartbeat: {_format_eta_from_heartbeat(heartbeat_at, heartbeat_sec)}",
        f"- last notify: {notify_last_sent}",
    ]
    if mission is not None and mission.title.strip():
        lines.append(f"- active mission: {mission.title} ({mission.kind})")
    if not tasks:
        lines.append("- recent: (нет задач)")
        return "\n".join(lines)

    lines.append("- recent:")
    for task in tasks:
        title = task.title.strip() or "(без названия)"
        parent = f", parent={task.parent_task_id}" if task.parent_task_id is not None else ""
        lines.append(
            f"  - #{task.id} [{task.status}] {title} "
            f"({task.kind}, src={task.source}, p={task.priority}{parent})"
        )
    return "\n".join(lines)


def _render_autonomy_pulse(autonomy_store: AutonomyStore, chat_id: int) -> str:
    counts = autonomy_store.counts_for_chat(chat_id)
    mode = autonomy_store.get_mode(chat_id) or "(не задан)"
    stopped = autonomy_store.autonomy_paused(chat_id)
    next_wakeup_raw = autonomy_store.get_next_wakeup(chat_id) or "(не задан)"
    idle_snooze_raw = autonomy_store.get_idle_snooze_until(chat_id) or ""
    mission = autonomy_store.get_active_mission(chat_id)
    next_pending = autonomy_store.get_next_pending_task(chat_id)
    next_wakeup = (
        _format_owner_moment(next_wakeup_raw)
        if next_wakeup_raw != "(не задан)"
        else next_wakeup_raw
    )
    idle_snooze = _format_owner_moment(idle_snooze_raw) if idle_snooze_raw else ""

    lines = [
        "Пульс автономности:",
        f"- режим: {'stopped' if stopped else mode}",
        f"- следующий wake-up: {'(остановлен)' if stopped else next_wakeup}",
    ]
    if mission is not None and mission.title.strip():
        if mission.phase == "scheduled" and mission.scheduled_for:
            lines.append(f"- следующий шаг: {mission.title}")
        else:
            lines.append(f"- текущая линия: {mission.title}")
    elif next_pending is not None and next_pending.title.strip():
        lines.append(f"- следующий шаг: {next_pending.title}")
    else:
        lines.append("- текущая линия: (нет активной миссии)")

    if stopped:
        lines.append("- статус: автономный контур остановлен")
    elif mode == "sleeping_idle" and idle_snooze:
        lines.append(f"- статус: притушен до {idle_snooze}")
    elif mission is not None and mission.phase == "waiting_user":
        lines.append("- статус: ждёт ответа владельца")
    elif counts["waiting_user"] > 0:
        lines.append("- статус: ждёт ответа владельца")
    elif counts["running"] > 0:
        lines.append("- статус: сейчас выполняет автономный шаг")
    elif counts["pending"] > 0:
        lines.append("- статус: есть запланированное продолжение")
    else:
        lines.append("- статус: явного автономного хвоста сейчас нет")
    return "\n".join(lines)
def _build_pulse_keyboard(*, stopped: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text="Обновить pulse",
                callback_data=PULSE_CALLBACK_DATA,
            )
        ],
    ]
    if stopped:
        rows.append(
            [
                InlineKeyboardButton(
                    text="Запустить автономность",
                    callback_data=PULSE_START_CALLBACK_DATA,
                )
            ]
        )
        return InlineKeyboardMarkup(inline_keyboard=rows)

    rows.extend(
        [
            [
                InlineKeyboardButton(
                    text="Пауза 6ч",
                    callback_data=PULSE_SNOOZE_CALLBACK_DATA,
                )
            ],
            [
                InlineKeyboardButton(
                    text="Разбудить сейчас",
                    callback_data=PULSE_WAKE_CALLBACK_DATA,
                )
            ],
            [
                InlineKeyboardButton(
                    text="Остановить автономность",
                    callback_data=PULSE_STOP_CALLBACK_DATA,
                )
            ],
        ]
    )
    return InlineKeyboardMarkup(
        inline_keyboard=rows
    )


def _parse_gc_days(text: str) -> int | None:
    parts = text.strip().split(maxsplit=1)
    if len(parts) == 1:
        return 7
    try:
        return int(parts[1].strip())
    except ValueError:
        return None


def _enqueue_restart_success_task(store: QueueStore, chat_id: int, service_name: str) -> int:
    text = (
        f"Системное событие: self-restart `{service_name}` уже успешно завершён. "
        "Коротко сообщи владельцу в чат, что рестарт прошёл успешно."
    )
    return store.enqueue_task(
        chat_id=chat_id,
        user_id=0,
        username="system",
        text=text,
        attachments=[],
    )


def _build_dispatcher(
    settings: Settings,
    store: QueueStore,
    autonomy_store: AutonomyStore,
    bot: Bot,
    stt_client: OpenRouterSttClient,
    autonomy_wake_event: asyncio.Event | None = None,
) -> Dispatcher:
    dp = Dispatcher()
    media_group_messages: dict[str, list[Message]] = {}
    media_group_flush_tasks: dict[str, asyncio.Task[None]] = {}
    media_group_lock = asyncio.Lock()
    media_group_flush_delay_sec = 1.2

    def _dedupe_paths(paths: list[str]) -> list[str]:
        seen: set[str] = set()
        result: list[str] = []
        for path in paths:
            if path in seen:
                continue
            seen.add(path)
            result.append(path)
        return result

    def _group_text(messages: list[Message]) -> str:
        for item in messages:
            text = _extract_text(item)
            if text:
                return text
        return ""

    async def _enqueue_grouped_messages(group_id: str) -> None:
        async with media_group_lock:
            messages = media_group_messages.pop(group_id, [])
            media_group_flush_tasks.pop(group_id, None)

        if not messages:
            return

        messages.sort(key=lambda m: int(m.message_id or 0))
        first = messages[0]
        sender = first.from_user
        if sender is None:
            return

        chat_id = int(first.chat.id)
        user_id = int(sender.id)
        username = (
            sender.username
            or " ".join(token for token in [sender.first_name, sender.last_name] if token)
            or "unknown"
        )
        text = _group_text(messages)

        attachments: list[str] = []
        for item in messages:
            attachments.extend(await download_attachments(bot, settings.assistant_root, item))
        attachments = _dedupe_paths(attachments)

        if not text and not attachments:
            return

        task_id = store.enqueue_task(
            chat_id=chat_id,
            user_id=user_id,
            username=username,
            text=text,
            attachments=attachments,
        )
        LOGGER.info(
            "Accepted grouped task #%s from chat=%s media_group=%s items=%s attachments=%s",
            task_id,
            chat_id,
            group_id,
            len(messages),
            len(attachments),
        )

    async def _schedule_media_group_message(message: Message) -> None:
        group_id = str(message.media_group_id)
        async with media_group_lock:
            media_group_messages.setdefault(group_id, []).append(message)
            previous = media_group_flush_tasks.get(group_id)
            if previous is not None:
                previous.cancel()

            async def _flush_later() -> None:
                await asyncio.sleep(media_group_flush_delay_sec)
                await _enqueue_grouped_messages(group_id)

            media_group_flush_tasks[group_id] = asyncio.create_task(_flush_later())

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
        _note_chat_activity_from_message(store, message)
        _nudge_autonomy_wakeup(autonomy_store, message, autonomy_wake_event)
        await message.answer(
            HELP_TEXT,
            reply_markup=ReplyKeyboardRemove(),
            disable_web_page_preview=True,
        )

    @dp.message(Command("restart"))
    async def on_restart(message: Message) -> None:
        if not await _guard(message):
            return
        _note_chat_activity_from_message(store, message)
        _nudge_autonomy_wakeup(autonomy_store, message, autonomy_wake_event)
        await message.answer("Выполнен перезапуск сервиса.")
        ok, detail = await asyncio.to_thread(
            request_service_restart,
            settings.state_db_path,
            BOT_SERVICE_NAME,
        )
        if ok:
            LOGGER.info("Restart scheduled via unit: %s", detail)
            return

        LOGGER.error("Failed to schedule bot restart: %s", detail)
        await message.answer(f"Не удалось запланировать перезапуск: {detail}")

    @dp.message(Command("status"))
    async def on_status(message: Message) -> None:
        if not await _guard(message):
            return
        _note_chat_activity_from_message(store, message)
        _nudge_autonomy_wakeup(autonomy_store, message, autonomy_wake_event)
        await message.answer(_render_status(store, autonomy_store, int(message.chat.id)))

    @dp.message(Command("pulse"))
    async def on_pulse(message: Message) -> None:
        if not await _guard(message):
            return
        _note_chat_activity_from_message(store, message)
        _nudge_autonomy_wakeup(autonomy_store, message, autonomy_wake_event)
        await message.answer(
            _render_autonomy_pulse(autonomy_store, int(message.chat.id)),
            reply_markup=_build_pulse_keyboard(
                stopped=autonomy_store.autonomy_paused(int(message.chat.id))
            ),
        )

    @dp.callback_query(F.data == PULSE_CALLBACK_DATA)
    async def on_pulse_callback(callback: CallbackQuery) -> None:
        sender = callback.from_user
        message = callback.message
        if sender is None or message is None:
            await callback.answer()
            return
        chat_id = int(message.chat.id)
        user_id = int(sender.id)
        if not _is_authorized(settings, chat_id, user_id):
            await callback.answer("Нет доступа.", show_alert=True)
            return

        _note_passive_owner_touch(store, message)
        text = _render_autonomy_pulse(autonomy_store, chat_id)
        current_text = (message.text or message.caption or "").strip()
        if current_text != text:
            await message.edit_text(
                text,
                reply_markup=_build_pulse_keyboard(
                    stopped=autonomy_store.autonomy_paused(chat_id)
                ),
            )
        await callback.answer("Пульс обновлён")

    @dp.callback_query(F.data == PULSE_SNOOZE_CALLBACK_DATA)
    async def on_pulse_snooze_callback(callback: CallbackQuery) -> None:
        sender = callback.from_user
        message = callback.message
        if sender is None or message is None:
            await callback.answer()
            return
        chat_id = int(message.chat.id)
        user_id = int(sender.id)
        if not _is_authorized(settings, chat_id, user_id):
            await callback.answer("Нет доступа.", show_alert=True)
            return

        _note_chat_activity_from_message(store, message)
        _schedule_autonomy_snooze(autonomy_store, chat_id, hours=6)
        text = _render_autonomy_pulse(autonomy_store, chat_id)
        current_text = (message.text or message.caption or "").strip()
        if current_text != text:
            await message.edit_text(
                text,
                reply_markup=_build_pulse_keyboard(
                    stopped=autonomy_store.autonomy_paused(chat_id)
                ),
            )
        await callback.answer("Автономность притушена на 6 часов")

    @dp.callback_query(F.data == PULSE_WAKE_CALLBACK_DATA)
    async def on_pulse_wake_callback(callback: CallbackQuery) -> None:
        sender = callback.from_user
        message = callback.message
        if sender is None or message is None:
            await callback.answer()
            return
        chat_id = int(message.chat.id)
        user_id = int(sender.id)
        if not _is_authorized(settings, chat_id, user_id):
            await callback.answer("Нет доступа.", show_alert=True)
            return

        _note_chat_activity_from_message(store, message)
        _wake_autonomy_now(autonomy_store, chat_id, autonomy_wake_event)
        text = _render_autonomy_pulse(autonomy_store, chat_id)
        current_text = (message.text or message.caption or "").strip()
        if current_text != text:
            await message.edit_text(
                text,
                reply_markup=_build_pulse_keyboard(
                    stopped=autonomy_store.autonomy_paused(chat_id)
                ),
            )
        await callback.answer("Автономность разбужена")

    @dp.callback_query(F.data == PULSE_STOP_CALLBACK_DATA)
    async def on_pulse_stop_callback(callback: CallbackQuery) -> None:
        sender = callback.from_user
        message = callback.message
        if sender is None or message is None:
            await callback.answer()
            return
        chat_id = int(message.chat.id)
        user_id = int(sender.id)
        if not _is_authorized(settings, chat_id, user_id):
            await callback.answer("Нет доступа.", show_alert=True)
            return

        _note_passive_owner_touch(store, message)
        _stop_autonomy_now(autonomy_store, chat_id)
        text = _render_autonomy_pulse(autonomy_store, chat_id)
        current_text = (message.text or message.caption or "").strip()
        if current_text != text:
            await message.edit_text(
                text,
                reply_markup=_build_pulse_keyboard(stopped=True),
            )
        await callback.answer("Автономность остановлена")

    @dp.callback_query(F.data == PULSE_START_CALLBACK_DATA)
    async def on_pulse_start_callback(callback: CallbackQuery) -> None:
        sender = callback.from_user
        message = callback.message
        if sender is None or message is None:
            await callback.answer()
            return
        chat_id = int(message.chat.id)
        user_id = int(sender.id)
        if not _is_authorized(settings, chat_id, user_id):
            await callback.answer("Нет доступа.", show_alert=True)
            return

        _note_passive_owner_touch(store, message)
        _wake_autonomy_now(autonomy_store, chat_id, autonomy_wake_event)
        text = _render_autonomy_pulse(autonomy_store, chat_id)
        current_text = (message.text or message.caption or "").strip()
        if current_text != text:
            await message.edit_text(
                text,
                reply_markup=_build_pulse_keyboard(stopped=False),
            )
        await callback.answer("Автономность запущена")

    @dp.message(Command("autonomy"))
    async def on_autonomy(message: Message) -> None:
        if not await _guard(message):
            return
        _note_chat_activity_from_message(store, message)
        _nudge_autonomy_wakeup(autonomy_store, message, autonomy_wake_event)
        await message.answer(
            _render_autonomy_status(
                autonomy_store,
                int(message.chat.id),
                settings.autonomy_heartbeat_sec,
            )
        )

    @dp.message(Command("reset"))
    async def on_reset(message: Message) -> None:
        if not await _guard(message):
            return
        _note_chat_activity_from_message(store, message)
        _nudge_autonomy_wakeup(autonomy_store, message, autonomy_wake_event)
        chat_id = int(message.chat.id)
        store.clear_chat_session_id(chat_id)
        await message.answer("Сессия этого чата сброшена. Следующее сообщение начнет новый контекст.")

    @dp.message(Command("gc"))
    async def on_gc(message: Message) -> None:
        if not await _guard(message):
            return
        _note_chat_activity_from_message(store, message)
        _nudge_autonomy_wakeup(autonomy_store, message, autonomy_wake_event)
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

        if message.media_group_id is not None:
            await _schedule_media_group_message(message)
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
        if not autonomy_store.autonomy_paused(chat_id):
            autonomy_store.clear_idle_snooze(chat_id)
            autonomy_store.schedule_next_wakeup_in(chat_id, 0)
            if autonomy_wake_event is not None:
                autonomy_wake_event.set()
        LOGGER.info("Accepted task #%s from chat=%s", task_id, chat_id)

    return dp


async def _run_async() -> None:
    settings = Settings.from_env()
    _setup_logging(settings.log_level)
    LOGGER.info("Assistant root: %s", settings.assistant_root)

    created_memory_files = ensure_memory_scaffold(settings.assistant_root)
    if created_memory_files:
        LOGGER.info(
            "Initialized memory scaffold: %s",
            ", ".join(path.relative_to(settings.assistant_root).as_posix() for path in created_memory_files),
        )
    created_requests_file = ensure_autonomy_requests_scaffold(settings.assistant_root)
    if created_requests_file is not None:
        LOGGER.info(
            "Initialized autonomy requests scaffold: %s",
            created_requests_file.relative_to(settings.assistant_root).as_posix(),
        )

    settings.state_db_path.parent.mkdir(parents=True, exist_ok=True)
    observed_restart = mark_restart_observed(settings.state_db_path, BOT_SERVICE_NAME)
    restart_notify_chat_id = consume_restart_notification_target(settings.state_db_path, BOT_SERVICE_NAME)
    store = QueueStore(settings.state_db_path)
    autonomy_store = AutonomyStore(settings.state_db_path)
    if restart_notify_chat_id is not None:
        restart_task_id = _enqueue_restart_success_task(store, restart_notify_chat_id, BOT_SERVICE_NAME)
        LOGGER.info(
            "Enqueued restart confirmation task #%s for chat=%s",
            restart_task_id,
            restart_notify_chat_id,
        )
    bot = Bot(token=settings.telegram_token)
    runner = CodexRunner(settings)
    stt_client = OpenRouterSttClient(settings)
    stop_event = asyncio.Event()
    if observed_restart:
        LOGGER.info("Observed completed restart for %s", BOT_SERVICE_NAME)
    autonomy_wake_event = asyncio.Event()
    worker = Worker(
        settings=settings,
        store=store,
        bot=bot,
        runner=runner,
        stop_event=stop_event,
    )
    autonomy_worker = AutonomyWorker(
        settings=settings,
        queue_store=store,
        autonomy_store=autonomy_store,
        bot=bot,
        runner=runner,
        stop_event=stop_event,
        wake_event=autonomy_wake_event,
    )
    worker_task = asyncio.create_task(worker.run())
    autonomy_worker_task = asyncio.create_task(autonomy_worker.run())
    try:
        await bot.set_my_commands(_build_bot_commands())
    except Exception as exc:  # pragma: no cover
        LOGGER.warning("Failed to configure bot commands menu: %s", exc)
    dispatcher = _build_dispatcher(
        settings,
        store,
        autonomy_store,
        bot,
        stt_client,
        autonomy_wake_event,
    )

    try:
        await dispatcher.start_polling(
            bot,
            polling_timeout=settings.poll_timeout_sec,
            allowed_updates=_allowed_update_types(),
        )
    finally:
        stop_event.set()
        await worker_task
        await autonomy_worker_task
        store.close()
        autonomy_store.close()
        await bot.session.close()


def run() -> None:
    asyncio.run(_run_async())


if __name__ == "__main__":
    run()
