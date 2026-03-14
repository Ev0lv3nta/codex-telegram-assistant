from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
import tomllib
from zoneinfo import ZoneInfo
from typing import Any

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

from .autonomy_guard import (
    GUARD_APPROVE_CALLBACK_DATA,
    GUARD_STOP_CALLBACK_DATA,
    build_guard_keyboard,
)
from .autonomy_store import AutonomySchedule, AutonomyStore
from .autonomy_worker import AutonomyWorker
from .autonomy_requests import ensure_autonomy_requests_scaffold
from .codex_runner import CodexRunner
from .config import Settings
from .ingest import download_attachments
from .memory_store import ensure_memory_scaffold
from .queue_store import QueueStore
from .session_gc import _extract_session_id
from .schedule_parser import (
    DEFAULT_TIMEZONE,
    ScheduleIntent,
    build_schedule_intent_prompt,
    compute_next_run_at,
    describe_recurrence,
    parse_schedule_intent_response,
)
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
CODEX_HOME = Path("/root/.codex")
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
- `/codexstatus` показывает модель и usage-лимиты Codex CLI.
- `/schedules` показывает активные и поставленные на паузу расписания.
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
        BotCommand(command="codexstatus", description="Лимиты Codex CLI"),
        BotCommand(command="schedules", description="Расписания"),
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
    autonomy_store.clear_guard_block(chat_id)
    autonomy_store.set_autonomy_paused(chat_id, False)
    autonomy_store.clear_idle_snooze(chat_id)
    autonomy_store.clear_guard_session(chat_id)
    autonomy_store.schedule_next_wakeup_in(chat_id, 0)
    autonomy_store.set_mode(chat_id, "idle")
    if wake_event is not None:
        wake_event.set()


def _stop_autonomy_now(autonomy_store: AutonomyStore, chat_id: int) -> None:
    autonomy_store.clear_guard_block(chat_id)
    autonomy_store.clear_guard_session(chat_id)
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


def _format_reset_epoch(epoch_value: object) -> str:
    try:
        epoch = int(epoch_value)
    except (TypeError, ValueError):
        return "(неизвестно)"
    dt = datetime.fromtimestamp(epoch, tz=timezone.utc).astimezone(MSK)
    return dt.strftime("%d.%m %H:%M MSK")


def _format_context_left_percent(turn_tokens: object, model_context_window: object) -> str:
    try:
        used = int(turn_tokens)
        window = int(model_context_window)
    except (TypeError, ValueError):
        return "(неизвестно)"
    if window <= 0:
        return "(неизвестно)"
    left = max(0.0, 100.0 - (used / window * 100.0))
    return f"{left:.0f}% left"


def _find_codex_session_file(
    codex_home: Path,
    *,
    session_id: str = "",
) -> Path | None:
    sessions_root = codex_home / "sessions"
    if not sessions_root.exists():
        return None
    candidates = sorted(
        sessions_root.rglob("*.jsonl"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if session_id.strip():
        wanted = session_id.strip().lower()
        for candidate in candidates:
            if _extract_session_id(candidate) == wanted:
                return candidate
    return candidates[0] if candidates else None


def _read_codex_session_snapshot(session_file: Path) -> dict[str, Any]:
    latest_rate_limits: dict[str, object] | None = None
    latest_turn_context: dict[str, object] | None = None
    latest_task_started: dict[str, object] | None = None
    latest_session_meta: dict[str, object] | None = None
    try:
        lines = session_file.read_text(encoding="utf-8", errors="ignore").splitlines()
    except OSError:
        return {}
    for raw_line in lines:
        try:
            event = json.loads(raw_line)
        except json.JSONDecodeError:
            continue
        event_type = event.get("type")
        payload = event.get("payload")
        if event_type == "session_meta" and isinstance(payload, dict):
            latest_session_meta = payload
        elif event_type == "turn_context" and isinstance(payload, dict):
            latest_turn_context = payload
        if isinstance(payload, dict):
            payload_type = payload.get("type")
            if event_type == "event_msg" or payload_type in {"task_started", "token_count"}:
                if payload_type == "task_started":
                    latest_task_started = payload
                elif payload_type == "token_count":
                    rate_limits = payload.get("rate_limits")
                    if isinstance(rate_limits, dict):
                        latest_rate_limits = rate_limits
                    info = payload.get("info")
                    if isinstance(info, dict):
                        latest_turn_context = {**(latest_turn_context or {}), "_token_info": info}
    return {
        "rate_limits": latest_rate_limits or {},
        "turn_context": latest_turn_context or {},
        "task_started": latest_task_started or {},
        "session_meta": latest_session_meta or {},
    }


def _render_codex_cli_status(
    codex_home: Path = CODEX_HOME,
    *,
    chat_session_id: str = "",
) -> str:
    version = "(неизвестно)"
    version_path = codex_home / "version.json"
    if version_path.exists():
        try:
            version = str(json.loads(version_path.read_text(encoding="utf-8")).get("version") or version)
        except (OSError, json.JSONDecodeError):
            pass

    model = "(не задана)"
    reasoning = "(не задан)"
    config_path = codex_home / "config.toml"
    if config_path.exists():
        try:
            config = tomllib.loads(config_path.read_text(encoding="utf-8"))
            model = str(config.get("model") or model)
            reasoning = str(config.get("model_reasoning_effort") or reasoning)
        except (OSError, tomllib.TOMLDecodeError):
            pass

    requested_session = chat_session_id.strip()
    session_file = (
        _find_codex_session_file(codex_home, session_id=requested_session)
        if requested_session
        else None
    )
    snapshot = _read_codex_session_snapshot(session_file) if session_file is not None else {}
    latest_rate_limits = snapshot.get("rate_limits") if isinstance(snapshot, dict) else {}
    turn_context = snapshot.get("turn_context") if isinstance(snapshot, dict) else {}
    task_started = snapshot.get("task_started") if isinstance(snapshot, dict) else {}
    session_meta = snapshot.get("session_meta") if isinstance(snapshot, dict) else {}
    if not latest_rate_limits:
        latest_session_file = _find_codex_session_file(codex_home)
        latest_snapshot = (
            _read_codex_session_snapshot(latest_session_file)
            if latest_session_file is not None
            else {}
        )
        fallback_limits = latest_snapshot.get("rate_limits") if isinstance(latest_snapshot, dict) else {}
        if isinstance(fallback_limits, dict):
            latest_rate_limits = fallback_limits

    session_model = ""
    session_reasoning = ""
    if isinstance(turn_context, dict):
        session_model = str(turn_context.get("model") or "")
        session_reasoning = str(turn_context.get("effort") or "")
        token_info = turn_context.get("_token_info")
    else:
        token_info = None
    if not session_model and isinstance(session_meta, dict):
        session_model = str(session_meta.get("model") or "")
    if session_model:
        model = session_model
    if session_reasoning:
        reasoning = session_reasoning

    model_context_window = ""
    if isinstance(task_started, dict):
        model_context_window = str(task_started.get("model_context_window") or "")
    turn_tokens = ""
    if isinstance(token_info, dict):
        last_usage = token_info.get("last_token_usage")
        if isinstance(last_usage, dict):
            turn_tokens = str(last_usage.get("total_tokens") or "")

    lines = [
        f"Codex CLI status:",
        f"- version: {version}",
        f"- model: {model}",
        f"- reasoning: {reasoning}",
    ]
    if requested_session:
        lines.append(f"- chat session: {requested_session}")
    else:
        lines.append("- chat session: (нет активной сессии у этого чата)")
    if session_file is not None:
        lines.append(f"- session file: {session_file.stem}")
    if turn_tokens and model_context_window:
        lines.append(
            f"- context est.: {_format_context_left_percent(turn_tokens, model_context_window)} (last turn)"
        )

    if isinstance(latest_rate_limits, dict):
        primary = latest_rate_limits.get("primary")
        secondary = latest_rate_limits.get("secondary")
        if isinstance(primary, dict):
            used = float(primary.get("used_percent") or 0.0)
            lines.append(
                f"- 5h limit: {max(0.0, 100.0 - used):.0f}% left "
                f"(resets {_format_reset_epoch(primary.get('resets_at'))})"
            )
        if isinstance(secondary, dict):
            used = float(secondary.get("used_percent") or 0.0)
            lines.append(
                f"- weekly limit: {max(0.0, 100.0 - used):.0f}% left "
                f"(resets {_format_reset_epoch(secondary.get('resets_at'))})"
            )
        plan_type = latest_rate_limits.get("plan_type")
        if plan_type:
            lines.append(f"- plan: {plan_type}")
    else:
        lines.append("- limits: локальный usage-state пока не найден")

    return "\n".join(lines)


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


def _is_schedule_confirmation(text: str) -> bool:
    clean = (text or "").strip().lower()
    return clean in {"да", "ага", "ок", "окей", "подтверждаю", "confirm", "yes", "y"}


def _is_schedule_cancel(text: str) -> bool:
    clean = (text or "").strip().lower()
    return clean in {"нет", "отмена", "cancel", "no", "n"}


def _normalize_schedule_prompt(prompt_text: str, delivery_hint: str) -> str:
    base = prompt_text.strip()
    if delivery_hint == "html":
        suffix = (
            "Итог оформи как HTML в `html_responses/last-response.html`, "
            "перезапиши файл и отправь его через `[[send-file:html_responses/last-response.html]]`."
        )
        return f"{base}\n\n{suffix}".strip()
    if delivery_hint == "md":
        suffix = (
            "Итог оформи как Markdown в `html_responses/last-response.md`, "
            "перезапиши файл и отправь его через `[[send-file:html_responses/last-response.md]]`."
        )
        return f"{base}\n\n{suffix}".strip()
    return base


def _schedule_preview_text(payload: dict[str, object]) -> str:
    recurrence_kind = str(payload.get("recurrence_kind") or "")
    recurrence_json = payload.get("recurrence_json")
    if not isinstance(recurrence_json, dict):
        recurrence_json = {}
    timezone_name = str(payload.get("timezone") or DEFAULT_TIMEZONE)
    next_run_at = str(payload.get("next_run_at") or "")
    delivery_hint = str(payload.get("delivery_hint") or "plain")
    title = str(payload.get("title") or "(без названия)")
    prompt_text = str(payload.get("prompt_text") or "")
    action = str(payload.get("action") or "create")
    action_label = "обновление" if action == "update" else "создание"
    return (
        f"Предпросмотр расписания ({action_label}):\n"
        f"- title: {title}\n"
        f"- recurrence: {describe_recurrence(recurrence_kind, recurrence_json, timezone_name=timezone_name)}\n"
        f"- next run: {_format_owner_moment(next_run_at)}\n"
        f"- delivery: {delivery_hint}\n"
        f"- prompt: {prompt_text}\n\n"
        "Ответь `да` чтобы сохранить, или `отмена` чтобы отменить."
    )


def _render_schedules_list(autonomy_store: AutonomyStore, chat_id: int) -> str:
    schedules = autonomy_store.list_schedules(chat_id=chat_id, include_inactive=True)
    if not schedules:
        return "Расписаний пока нет."
    lines = ["Расписания:"]
    for schedule in schedules:
        status = "active" if schedule.active else "paused"
        if schedule.last_status:
            status = f"{status}, {schedule.last_status}"
        lines.append(
            f"- #{schedule.id} {schedule.title}\n"
            f"  {describe_recurrence(schedule.recurrence_kind, schedule.recurrence_json, timezone_name=schedule.timezone)}\n"
            f"  next: {_format_owner_moment(schedule.next_run_at) if schedule.next_run_at else '(не задан)'}\n"
            f"  status: {status}"
        )
    return "\n".join(lines)


def _parse_schedule_command_id(text: str) -> int | None:
    parts = (text or "").strip().split(maxsplit=1)
    if len(parts) != 2:
        return None
    try:
        return int(parts[1].strip())
    except ValueError:
        return None


def _maybe_align_wakeup_with_schedules(
    autonomy_store: AutonomyStore,
    chat_id: int,
    wake_event: asyncio.Event | None = None,
) -> None:
    schedule_next = autonomy_store.get_next_schedule_run(chat_id)
    current_next = autonomy_store.get_next_wakeup(chat_id)
    if schedule_next:
        target = schedule_next
        if current_next:
            current_dt = _parse_iso_dt(current_next)
            schedule_dt = _parse_iso_dt(schedule_next)
            if current_dt is not None and schedule_dt is not None and current_dt <= schedule_dt:
                target = current_next
        autonomy_store.set_next_wakeup(chat_id, target)
    if wake_event is not None:
        wake_event.set()


def _arm_autonomy_for_schedule(
    autonomy_store: AutonomyStore,
    chat_id: int,
    wake_event: asyncio.Event | None = None,
) -> None:
    autonomy_store.set_autonomy_paused(chat_id, False)
    autonomy_store.clear_idle_snooze(chat_id)
    autonomy_store.set_mode(chat_id, "idle")
    _maybe_align_wakeup_with_schedules(autonomy_store, chat_id, wake_event)


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
    active_phase = autonomy_store.get_active_mission(chat_id)
    mission = autonomy_store.get_live_mission(chat_id)
    notify_last_sent = autonomy_store.get_notify_last_sent(chat_id) or "(не было)"
    guard_waiting = autonomy_store.guard_waiting_approval(chat_id)
    guard_reason = autonomy_store.get_guard_block_reason(chat_id)
    guard_started = _parse_iso_dt(autonomy_store.get_guard_session_started_at(chat_id))
    guard_runtime_min = 0
    if guard_started is not None:
        guard_runtime_min = max(
            0,
            int((datetime.now(timezone.utc) - guard_started).total_seconds() // 60),
        )
    guard_calls = len(autonomy_store.get_guard_recent_call_timestamps(chat_id))
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
    if guard_waiting:
        lines.append("- guard: waiting_owner_approval")
        lines.append(f"- guard reason: {guard_reason or '(неизвестно)'}")
        lines.append(f"- guard runtime: {guard_runtime_min} min")
        lines.append(f"- guard calls/hour: {guard_calls}")
    if mode == "sleeping_completed" and next_wakeup != "(не задан)":
        lines.append("- sleep reason: миссия завершена, контур спит до следующей проверки")
    elif mode == "sleeping_empty_idle" and next_wakeup != "(не задан)":
        lines.append("- sleep reason: активных задач нет, контур спит")
    elif mode == "sleeping_user_declined" and next_wakeup != "(не задан)":
        lines.append("- sleep reason: владелец отказался от idle-инициативы, контур спит")
    if mission is not None:
        lines.append(f"- root mission: {mission.root_objective}")
        lines.append(f"- mission status: {mission.status}")
        if mission.plan_state == "staged":
            lines.append("- mission plan: staged")
            stages = mission.plan_json or []
            current_stage = None
            next_stage = None
            if 0 <= mission.current_stage_index < len(stages):
                current_stage = stages[mission.current_stage_index]
            if 0 <= mission.current_stage_index + 1 < len(stages):
                next_stage = stages[mission.current_stage_index + 1]
            if current_stage and str(current_stage.get("title", "")).strip():
                lines.append(f"- current stage: {str(current_stage.get('title', '')).strip()}")
            if next_stage and str(next_stage.get("title", "")).strip():
                lines.append(f"- next stage: {str(next_stage.get('title', '')).strip()}")
        if mission.current_focus.strip():
            lines.append(f"- mission focus: {mission.current_focus}")
        if active_phase is not None and active_phase.phase.strip():
            lines.append(f"- mission phase: {active_phase.phase}")
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
    guard_waiting = autonomy_store.guard_waiting_approval(chat_id)
    next_wakeup_raw = autonomy_store.get_next_wakeup(chat_id) or "(не задан)"
    idle_snooze_raw = autonomy_store.get_idle_snooze_until(chat_id) or ""
    active_phase = autonomy_store.get_active_mission(chat_id)
    mission = autonomy_store.get_live_mission(chat_id)
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
    if mission is not None:
        lines.append(f"- корневая миссия: {mission.root_objective}")
        if mission.plan_state == "staged":
            stages = mission.plan_json or []
            current_stage = None
            next_stage = None
            if 0 <= mission.current_stage_index < len(stages):
                current_stage = stages[mission.current_stage_index]
            if 0 <= mission.current_stage_index + 1 < len(stages):
                next_stage = stages[mission.current_stage_index + 1]
            if current_stage and str(current_stage.get("title", "")).strip():
                lines.append(f"- текущий этап: {str(current_stage.get('title', '')).strip()}")
            if next_stage and str(next_stage.get("title", "")).strip():
                lines.append(f"- следующий этап: {str(next_stage.get('title', '')).strip()}")
        focus = ""
        if active_phase is not None and active_phase.title.strip():
            focus = active_phase.title
        elif mission.current_focus.strip():
            focus = mission.current_focus
        if focus:
            lines.append(f"- текущий фокус: {focus}")
            lines.append(f"- текущая линия: {focus}")
    elif active_phase is not None and active_phase.title.strip():
        if active_phase.phase == "scheduled" and active_phase.scheduled_for:
            lines.append(f"- следующий шаг: {active_phase.title}")
        else:
            lines.append(f"- текущая линия: {active_phase.title}")
    elif next_pending is not None and next_pending.title.strip():
        lines.append(f"- следующий шаг: {next_pending.title}")
    else:
        lines.append("- текущая линия: (нет активной миссии)")

    if guard_waiting:
        guard_reason = autonomy_store.get_guard_block_reason(chat_id) or "неизвестно"
        session_started = _parse_iso_dt(autonomy_store.get_guard_session_started_at(chat_id))
        runtime_min = 0
        if session_started is not None:
            runtime_min = max(
                0,
                int((datetime.now(timezone.utc) - session_started).total_seconds() // 60),
            )
        lines.append(f"- статус: guard остановил автономность, ждёт подтверждения владельца")
        lines.append(f"- причина guard: {guard_reason}")
        lines.append(
            f"- guard: {len(autonomy_store.get_guard_recent_call_timestamps(chat_id))} автономных вызова за окно, {runtime_min} мин непрерывной работы"
        )
    elif stopped:
        lines.append("- статус: автономный контур остановлен")
    elif mode == "sleeping_completed" and next_wakeup != "(не задан)":
        lines.append(f"- статус: миссия завершена, спит до {next_wakeup}")
    elif mode == "sleeping_empty_idle" and next_wakeup != "(не задан)":
        lines.append(f"- статус: активных задач нет, спит до {next_wakeup}")
    elif mode == "sleeping_user_declined" and next_wakeup != "(не задан)":
        lines.append(f"- статус: владелец отказался от idle-инициативы, спит до {next_wakeup}")
    elif mode == "sleeping_idle" and idle_snooze:
        lines.append(f"- статус: притушен до {idle_snooze}")
    elif active_phase is not None and active_phase.phase == "waiting_user":
        lines.append("- статус: ждёт ответа владельца")
    elif mission is not None and mission.status == "blocked_user":
        lines.append("- статус: миссия ждёт ответа владельца")
    elif counts["waiting_user"] > 0:
        lines.append("- статус: ждёт ответа владельца")
    elif counts["running"] > 0:
        lines.append("- статус: сейчас выполняет автономный шаг")
    elif counts["pending"] > 0:
        lines.append("- статус: есть запланированное продолжение")
    elif mission is not None and mission.status == "completed":
        lines.append("- статус: последняя миссия уже закрыта")
    else:
        lines.append("- статус: явного автономного хвоста сейчас нет")
    if guard_waiting:
        lines.append("- причина следующего wake-up: ждёт твоего разрешения продолжить")
    elif active_phase is not None and active_phase.phase == "scheduled":
        lines.append("- причина следующего wake-up: запланированное продолжение текущей линии")
    elif mission is not None and mission.status == "blocked_user":
        lines.append("- причина следующего wake-up: ожидание ответа владельца")
    elif mode == "sleeping_completed":
        lines.append("- причина следующего wake-up: контрольная проверка после завершения миссии")
    elif mode == "sleeping_empty_idle":
        lines.append("- причина следующего wake-up: редкая idle-проверка без активных задач")
    elif mode == "sleeping_user_declined":
        lines.append("- причина следующего wake-up: длинная пауза после отказа владельца")
    elif counts["running"] > 0:
        lines.append("- причина следующего wake-up: текущий этап ещё выполняется")
    elif counts["pending"] > 0:
        lines.append("- причина следующего wake-up: переход к следующему этапу или checkpoint")
    elif not stopped:
        lines.append("- причина следующего wake-up: обычный idle heartbeat")
    return "\n".join(lines)


def _build_pulse_keyboard(*, stopped: bool = False, guard_waiting: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text="Обновить pulse",
                callback_data=PULSE_CALLBACK_DATA,
            )
        ],
    ]
    if guard_waiting:
        rows.extend(build_guard_keyboard().inline_keyboard)
        return InlineKeyboardMarkup(inline_keyboard=rows)
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
    runner: CodexRunner,
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

    def _save_schedule_payload(chat_id: int, payload: dict[str, object]) -> tuple[int | None, str]:
        recurrence_json = payload.get("recurrence_json")
        if not isinstance(recurrence_json, dict):
            recurrence_json = {}
        normalized_prompt = _normalize_schedule_prompt(
            str(payload.get("prompt_text") or ""),
            str(payload.get("delivery_hint") or "plain"),
        )
        action = str(payload.get("action") or "create")
        if action == "update":
            schedule_id = int(payload.get("schedule_id") or 0)
            if schedule_id <= 0:
                return None, "Не удалось обновить расписание: отсутствует id."
            schedule = autonomy_store.get_schedule(schedule_id, chat_id=chat_id)
            if schedule is None:
                return None, f"Расписание #{schedule_id} не найдено."
            autonomy_store.update_schedule(
                schedule_id,
                title=str(payload.get("title") or schedule.title),
                prompt_text=normalized_prompt,
                timezone=str(payload.get("timezone") or schedule.timezone),
                recurrence_kind=str(payload.get("recurrence_kind") or schedule.recurrence_kind),
                recurrence_json=recurrence_json or schedule.recurrence_json,
                next_run_at=str(payload.get("next_run_at") or schedule.next_run_at),
                delivery_hint=str(payload.get("delivery_hint") or schedule.delivery_hint),
                active=True,
                last_status="scheduled",
            )
            return schedule_id, f"Расписание #{schedule_id} обновлено."
        schedule_id = autonomy_store.create_schedule(
            chat_id=chat_id,
            title=str(payload.get("title") or "Scheduled job"),
            prompt_text=normalized_prompt,
            timezone=str(payload.get("timezone") or DEFAULT_TIMEZONE),
            recurrence_kind=str(payload.get("recurrence_kind") or ""),
            recurrence_json=recurrence_json,
            next_run_at=str(payload.get("next_run_at") or ""),
            delivery_hint=str(payload.get("delivery_hint") or "plain"),
            active=True,
        )
        return schedule_id, f"Расписание #{schedule_id} сохранено."

    async def _classify_schedule_intent(text: str) -> ScheduleIntent:
        prompt = build_schedule_intent_prompt(text)
        result = await asyncio.to_thread(runner.run, prompt, "")
        if not result.success:
            return ScheduleIntent(action="no_schedule_intent")
        return parse_schedule_intent_response(result.message)

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

    @dp.message(Command("codexstatus"))
    async def on_codex_status(message: Message) -> None:
        if not await _guard(message):
            return
        _note_chat_activity_from_message(store, message)
        await message.answer(
            _render_codex_cli_status(
                chat_session_id=store.get_chat_session_id(int(message.chat.id)) or "",
            )
        )

    @dp.message(Command("schedules"))
    async def on_schedules(message: Message) -> None:
        if not await _guard(message):
            return
        _note_chat_activity_from_message(store, message)
        await message.answer(_render_schedules_list(autonomy_store, int(message.chat.id)))

    @dp.message(Command("schedule_pause"))
    async def on_schedule_pause(message: Message) -> None:
        if not await _guard(message):
            return
        _note_chat_activity_from_message(store, message)
        chat_id = int(message.chat.id)
        schedule_id = _parse_schedule_command_id(message.text or "")
        if schedule_id is None:
            await message.answer("Использование: /schedule_pause <id>")
            return
        if not autonomy_store.pause_schedule(schedule_id, chat_id=chat_id):
            await message.answer(f"Расписание #{schedule_id} не найдено.")
            return
        _maybe_align_wakeup_with_schedules(autonomy_store, chat_id, autonomy_wake_event)
        await message.answer(f"Расписание #{schedule_id} поставлено на паузу.")

    @dp.message(Command("schedule_resume"))
    async def on_schedule_resume(message: Message) -> None:
        if not await _guard(message):
            return
        _note_chat_activity_from_message(store, message)
        chat_id = int(message.chat.id)
        schedule_id = _parse_schedule_command_id(message.text or "")
        if schedule_id is None:
            await message.answer("Использование: /schedule_resume <id>")
            return
        schedule = autonomy_store.get_schedule(schedule_id, chat_id=chat_id)
        if schedule is None:
            await message.answer(f"Расписание #{schedule_id} не найдено.")
            return
        next_run_at = compute_next_run_at(
            schedule.recurrence_kind,
            schedule.recurrence_json,
            schedule.timezone,
        )
        autonomy_store.resume_schedule(schedule_id, chat_id=chat_id, next_run_at=next_run_at)
        _arm_autonomy_for_schedule(autonomy_store, chat_id, autonomy_wake_event)
        await message.answer(
            f"Расписание #{schedule_id} возобновлено. Следующий запуск: {_format_owner_moment(next_run_at)}."
        )

    @dp.message(Command("schedule_delete"))
    async def on_schedule_delete(message: Message) -> None:
        if not await _guard(message):
            return
        _note_chat_activity_from_message(store, message)
        chat_id = int(message.chat.id)
        schedule_id = _parse_schedule_command_id(message.text or "")
        if schedule_id is None:
            await message.answer("Использование: /schedule_delete <id>")
            return
        if not autonomy_store.delete_schedule(schedule_id, chat_id=chat_id):
            await message.answer(f"Расписание #{schedule_id} не найдено.")
            return
        _maybe_align_wakeup_with_schedules(autonomy_store, chat_id, autonomy_wake_event)
        await message.answer(f"Расписание #{schedule_id} удалено.")

    @dp.message(Command("pulse"))
    async def on_pulse(message: Message) -> None:
        if not await _guard(message):
            return
        _note_chat_activity_from_message(store, message)
        _nudge_autonomy_wakeup(autonomy_store, message, autonomy_wake_event)
        await message.answer(
            _render_autonomy_pulse(autonomy_store, int(message.chat.id)),
            reply_markup=_build_pulse_keyboard(
                stopped=autonomy_store.autonomy_paused(int(message.chat.id)),
                guard_waiting=autonomy_store.guard_waiting_approval(int(message.chat.id)),
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
                    stopped=autonomy_store.autonomy_paused(chat_id),
                    guard_waiting=autonomy_store.guard_waiting_approval(chat_id),
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
                    stopped=autonomy_store.autonomy_paused(chat_id),
                    guard_waiting=autonomy_store.guard_waiting_approval(chat_id),
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
                    stopped=autonomy_store.autonomy_paused(chat_id),
                    guard_waiting=autonomy_store.guard_waiting_approval(chat_id),
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
                reply_markup=_build_pulse_keyboard(
                    stopped=True,
                    guard_waiting=autonomy_store.guard_waiting_approval(chat_id),
                ),
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
                reply_markup=_build_pulse_keyboard(
                    stopped=False,
                    guard_waiting=autonomy_store.guard_waiting_approval(chat_id),
                ),
            )
        await callback.answer("Автономность запущена")

    @dp.callback_query(F.data == GUARD_APPROVE_CALLBACK_DATA)
    async def on_guard_approve_callback(callback: CallbackQuery) -> None:
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
        autonomy_store.set_guard_waiting_approval(chat_id, False)
        autonomy_store.set_guard_approved_once(chat_id, True)
        autonomy_store.set_guard_alert_message_id(chat_id, None)
        autonomy_store.set_mode(chat_id, "idle")
        autonomy_store.schedule_next_wakeup_in(chat_id, 0)
        if autonomy_wake_event is not None:
            autonomy_wake_event.set()
        text = _render_autonomy_pulse(autonomy_store, chat_id)
        current_text = (message.text or message.caption or "").strip()
        if current_text != text:
            await message.edit_text(
                text,
                reply_markup=_build_pulse_keyboard(
                    stopped=autonomy_store.autonomy_paused(chat_id),
                    guard_waiting=autonomy_store.guard_waiting_approval(chat_id),
                ),
            )
        await callback.answer("Разрешён один автономный сеанс")

    @dp.callback_query(F.data == GUARD_STOP_CALLBACK_DATA)
    async def on_guard_stop_callback(callback: CallbackQuery) -> None:
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
                reply_markup=_build_pulse_keyboard(stopped=True, guard_waiting=False),
            )
        await callback.answer("Автономность остановлена")

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

        if text and not attachments:
            pending_schedule = autonomy_store.get_pending_schedule_confirmation(chat_id)
            if pending_schedule is not None:
                if _is_schedule_confirmation(text):
                    autonomy_store.clear_pending_schedule_confirmation(chat_id)
                    schedule_id, reply = _save_schedule_payload(chat_id, pending_schedule)
                    if schedule_id is not None:
                        _arm_autonomy_for_schedule(autonomy_store, chat_id, autonomy_wake_event)
                    if schedule_id is not None:
                        await message.answer(reply)
                    else:
                        await message.answer(reply)
                    return
                if _is_schedule_cancel(text):
                    autonomy_store.clear_pending_schedule_confirmation(chat_id)
                    await message.answer("Создание расписания отменено.")
                    return
                await message.answer(
                    "Есть неподтверждённое расписание. Ответь `да` чтобы сохранить его или `отмена` чтобы сбросить.",
                )
                return

            intent = await _classify_schedule_intent(text)
            if intent.action == "list":
                await message.answer(_render_schedules_list(autonomy_store, chat_id))
                return
            if intent.action in {"pause", "resume", "delete"} and intent.schedule_id is not None:
                if intent.action == "pause":
                    if autonomy_store.pause_schedule(intent.schedule_id, chat_id=chat_id):
                        _maybe_align_wakeup_with_schedules(autonomy_store, chat_id, autonomy_wake_event)
                        await message.answer(f"Расписание #{intent.schedule_id} поставлено на паузу.")
                    else:
                        await message.answer(f"Расписание #{intent.schedule_id} не найдено.")
                    return
                if intent.action == "resume":
                    schedule = autonomy_store.get_schedule(intent.schedule_id, chat_id=chat_id)
                    if schedule is None:
                        await message.answer(f"Расписание #{intent.schedule_id} не найдено.")
                        return
                    next_run_at = compute_next_run_at(
                        schedule.recurrence_kind,
                        schedule.recurrence_json,
                        schedule.timezone,
                    )
                    autonomy_store.resume_schedule(intent.schedule_id, chat_id=chat_id, next_run_at=next_run_at)
                    _arm_autonomy_for_schedule(autonomy_store, chat_id, autonomy_wake_event)
                    await message.answer(
                        f"Расписание #{intent.schedule_id} возобновлено. Следующий запуск: {_format_owner_moment(next_run_at)}."
                    )
                    return
                if intent.action == "delete":
                    if autonomy_store.delete_schedule(intent.schedule_id, chat_id=chat_id):
                        _maybe_align_wakeup_with_schedules(autonomy_store, chat_id, autonomy_wake_event)
                        await message.answer(f"Расписание #{intent.schedule_id} удалено.")
                    else:
                        await message.answer(f"Расписание #{intent.schedule_id} не найдено.")
                    return

            if intent.action in {"create", "update"}:
                try:
                    next_run_at = compute_next_run_at(
                        intent.recurrence_kind,
                        intent.recurrence_json or {},
                        intent.timezone,
                    )
                except Exception:
                    await message.answer("Не удалось разобрать расписание. Проверь дату/время и попробуй ещё раз.")
                    return
                preview_payload = {
                    "action": intent.action,
                    "schedule_id": intent.schedule_id,
                    "title": intent.title,
                    "prompt_text": intent.prompt_text,
                    "recurrence_kind": intent.recurrence_kind,
                    "recurrence_json": intent.recurrence_json or {},
                    "timezone": intent.timezone,
                    "delivery_hint": intent.delivery_hint,
                    "next_run_at": next_run_at,
                }
                if intent.action == "update":
                    existing = autonomy_store.get_schedule(int(intent.schedule_id or 0), chat_id=chat_id)
                    if existing is None:
                        await message.answer(f"Расписание #{intent.schedule_id} не найдено.")
                        return
                autonomy_store.set_pending_schedule_confirmation(chat_id, preview_payload)
                await message.answer(_schedule_preview_text(preview_payload))
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
    last_active_chat_id = store.get_last_active_chat_id()
    if last_active_chat_id is not None:
        _maybe_align_wakeup_with_schedules(
            autonomy_store,
            last_active_chat_id,
            autonomy_wake_event,
        )
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
        runner,
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
