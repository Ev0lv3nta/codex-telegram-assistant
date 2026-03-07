from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


MSK = ZoneInfo("Europe/Moscow")
AUTONOMY_JOURNAL_DIR = Path("system/tasks/autonomy_journal")


@dataclass(frozen=True)
class AutonomyJournalEntry:
    status: str
    title: str
    summary: str
    task_id: int | None = None


def autonomy_journal_dir(assistant_root: Path) -> Path:
    return assistant_root / AUTONOMY_JOURNAL_DIR


def journal_rel_path_for_day(day: datetime | None = None) -> str:
    current = (day or datetime.now(MSK)).astimezone(MSK)
    return f"system/tasks/autonomy_journal/{current:%Y-%m-%d}.md"


def _day_header(now: datetime) -> str:
    return f"# Автономность за {now:%Y-%m-%d}\n\n"


def _normalize_summary(text: str, limit: int = 280) -> str:
    compact = " ".join((text or "").strip().split())
    if not compact:
        return "Краткий результат не зафиксирован."
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3].rstrip() + "..."


def append_autonomy_journal_entry(
    assistant_root: Path,
    entry: AutonomyJournalEntry,
    *,
    now: datetime | None = None,
) -> Path:
    timestamp = (now or datetime.now(MSK)).astimezone(MSK)
    target = assistant_root / journal_rel_path_for_day(timestamp)
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        target.write_text(_day_header(timestamp), encoding="utf-8")

    task_part = f"#{entry.task_id} " if entry.task_id is not None else ""
    block = (
        f"## {timestamp:%H:%M} · {entry.status}\n"
        f"- Задача: {task_part}{entry.title.strip() or '(без названия)'}\n"
        f"- Итог: {_normalize_summary(entry.summary)}\n\n"
    )
    with target.open("a", encoding="utf-8") as handle:
        handle.write(block)
    return target


def read_recent_autonomy_journal_entries(
    assistant_root: Path,
    *,
    limit: int = 3,
    day: datetime | None = None,
) -> list[str]:
    safe_limit = max(1, limit)
    target = assistant_root / journal_rel_path_for_day(day)
    if not target.exists():
        return []

    text = target.read_text(encoding="utf-8").strip()
    if not text:
        return []

    chunks = [chunk.strip() for chunk in text.split("\n## ") if chunk.strip()]
    entries: list[str] = []
    for chunk in reversed(chunks):
        if chunk.startswith("# Автономность за "):
            continue
        normalized = chunk if chunk.startswith("## ") else f"## {chunk}"
        entries.append(" ".join(normalized.split()))
        if len(entries) >= safe_limit:
            break
    return entries
