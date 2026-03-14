from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from zoneinfo import ZoneInfo


DEFAULT_TIMEZONE = "Europe/Moscow"


@dataclass(frozen=True)
class ScheduleIntent:
    action: str
    title: str = ""
    prompt_text: str = ""
    recurrence_kind: str = ""
    recurrence_json: dict[str, object] | None = None
    timezone: str = DEFAULT_TIMEZONE
    delivery_hint: str = "plain"
    schedule_id: int | None = None


def build_schedule_intent_prompt(user_text: str) -> str:
    return (
        "Ты разбираешь сообщение владельца бота и должен понять, хочет ли он создать, обновить, "
        "остановить, возобновить, удалить или посмотреть расписание автономной задачи.\n\n"
        "Верни ТОЛЬКО один JSON-объект без пояснений и markdown.\n"
        "Допустимые action: create, update, pause, resume, delete, list, no_schedule_intent.\n"
        "Поля JSON: action, schedule_id, title, prompt_text, recurrence_kind, recurrence_json, timezone, delivery_hint.\n"
        "recurrence_kind: once | daily | weekly | ''.\n"
        "Для once recurrence_json должен содержать date=YYYY-MM-DD и time=HH:MM.\n"
        "Для daily recurrence_json должен содержать time=HH:MM.\n"
        "Для weekly recurrence_json должен содержать weekday=0..6 и time=HH:MM, где 0=понедельник.\n"
        "timezone по умолчанию Europe/Moscow, если пользователь не указал другой.\n"
        "delivery_hint: plain | md | html | auto.\n"
        "Если пользователь не просит именно расписание/отложенную задачу, верни action=no_schedule_intent.\n"
        "Если не хватает данных для корректного расписания, всё равно верни create/update только если намерение очевидно; "
        "иначе no_schedule_intent.\n\n"
        f"Сообщение пользователя:\n{user_text.strip()}"
    )


def parse_schedule_intent_response(
    response_text: str,
    *,
    default_timezone: str = DEFAULT_TIMEZONE,
) -> ScheduleIntent:
    raw = (response_text or "").strip()
    if not raw:
        return ScheduleIntent(action="no_schedule_intent")

    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end < start:
        return ScheduleIntent(action="no_schedule_intent")

    try:
        payload = json.loads(raw[start : end + 1])
    except json.JSONDecodeError:
        return ScheduleIntent(action="no_schedule_intent")
    if not isinstance(payload, dict):
        return ScheduleIntent(action="no_schedule_intent")

    action = str(payload.get("action") or "no_schedule_intent").strip().lower()
    if action not in {"create", "update", "pause", "resume", "delete", "list", "no_schedule_intent"}:
        action = "no_schedule_intent"

    recurrence_kind = str(payload.get("recurrence_kind") or "").strip().lower()
    if recurrence_kind not in {"once", "daily", "weekly", ""}:
        recurrence_kind = ""

    recurrence_json = payload.get("recurrence_json")
    if not isinstance(recurrence_json, dict):
        recurrence_json = {}

    delivery_hint = str(payload.get("delivery_hint") or "plain").strip().lower()
    if delivery_hint not in {"plain", "md", "html", "auto"}:
        delivery_hint = "plain"

    schedule_id = payload.get("schedule_id")
    try:
        schedule_id_value = int(schedule_id) if schedule_id is not None else None
    except (TypeError, ValueError):
        schedule_id_value = None

    return ScheduleIntent(
        action=action,
        title=str(payload.get("title") or "").strip(),
        prompt_text=str(payload.get("prompt_text") or "").strip(),
        recurrence_kind=recurrence_kind,
        recurrence_json=recurrence_json,
        timezone=str(payload.get("timezone") or default_timezone).strip() or default_timezone,
        delivery_hint=delivery_hint,
        schedule_id=schedule_id_value,
    )


def compute_next_run_at(
    recurrence_kind: str,
    recurrence_json: dict[str, object],
    timezone_name: str = DEFAULT_TIMEZONE,
    *,
    now: datetime | None = None,
) -> str:
    tz = ZoneInfo(timezone_name or DEFAULT_TIMEZONE)
    current = now.astimezone(tz) if now is not None else datetime.now(tz)

    if recurrence_kind == "once":
        date_value = str(recurrence_json.get("date") or "")
        time_value = str(recurrence_json.get("time") or "")
        if not date_value or not time_value:
            raise ValueError("once schedule requires date and time")
        local_dt = datetime.fromisoformat(f"{date_value}T{time_value}:00").replace(tzinfo=tz)
        return local_dt.astimezone(timezone.utc).isoformat()

    if recurrence_kind == "daily":
        time_value = str(recurrence_json.get("time") or "")
        hour_str, minute_str = time_value.split(":", maxsplit=1)
        candidate = current.replace(
            hour=int(hour_str),
            minute=int(minute_str),
            second=0,
            microsecond=0,
        )
        if candidate <= current:
            candidate = candidate + timedelta(days=1)
        return candidate.astimezone(timezone.utc).isoformat()

    if recurrence_kind == "weekly":
        time_value = str(recurrence_json.get("time") or "")
        weekday = int(recurrence_json.get("weekday"))
        hour_str, minute_str = time_value.split(":", maxsplit=1)
        candidate = current.replace(
            hour=int(hour_str),
            minute=int(minute_str),
            second=0,
            microsecond=0,
        )
        delta_days = (weekday - candidate.weekday()) % 7
        candidate = candidate + timedelta(days=delta_days)
        if candidate <= current:
            candidate = candidate + timedelta(days=7)
        return candidate.astimezone(timezone.utc).isoformat()

    raise ValueError(f"Unsupported recurrence kind: {recurrence_kind}")


def describe_recurrence(
    recurrence_kind: str,
    recurrence_json: dict[str, object],
    *,
    timezone_name: str = DEFAULT_TIMEZONE,
) -> str:
    if recurrence_kind == "daily":
        return f"каждый день в {recurrence_json.get('time')} ({timezone_name})"
    if recurrence_kind == "weekly":
        weekday = int(recurrence_json.get("weekday", 0))
        names = {
            0: "каждый понедельник",
            1: "каждый вторник",
            2: "каждую среду",
            3: "каждый четверг",
            4: "каждую пятницу",
            5: "каждую субботу",
            6: "каждое воскресенье",
        }
        return f"{names.get(weekday, 'каждую неделю')} в {recurrence_json.get('time')} ({timezone_name})"
    if recurrence_kind == "once":
        return f"один раз {recurrence_json.get('date')} в {recurrence_json.get('time')} ({timezone_name})"
    return recurrence_kind
