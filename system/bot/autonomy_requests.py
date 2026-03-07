from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


AUTONOMY_REQUESTS_PATH = Path("system/tasks/autonomy_requests.md")
AUTONOMY_REQUESTS_PATH_NOTE = "system/tasks/autonomy_requests.md"
AUTONOMY_REQUESTS_TEMPLATE = """# Автономные поручения

Этот файл хранит явные поручения владельца для будущих автономных запусков.

Правила:
- активные поручения живут только в разделе `## Активные`;
- если поручение полностью выполнено, его нужно убрать из `## Активные`;
- если поручение больше неактуально, его тоже нужно вывести из активного списка.

## Активные

<!--
Шаблон поручения:

### Название поручения
- due: 2026-03-07 20:00 MSK
- deliverable: краткая сводка / методичка / note / research
- details: что именно нужно сделать
- notes: необязательно
-->
"""


@dataclass(frozen=True)
class AutonomyRequestSummary:
    title: str
    summary: str


def autonomy_requests_path(assistant_root: Path) -> Path:
    return assistant_root / AUTONOMY_REQUESTS_PATH


def ensure_autonomy_requests_scaffold(assistant_root: Path) -> Path | None:
    target = autonomy_requests_path(assistant_root)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        return None
    target.write_text(AUTONOMY_REQUESTS_TEMPLATE, encoding="utf-8")
    return target


def _extract_active_section(lines: list[str]) -> list[str]:
    capture = False
    in_comment = False
    collected: list[str] = []
    for raw_line in lines:
        stripped = raw_line.strip()
        if stripped.startswith("## "):
            heading = stripped[3:].strip().lower()
            if heading in {"активные", "active", "pending"}:
                capture = True
                continue
            if capture:
                break
        if capture:
            if "<!--" in stripped:
                in_comment = True
            if not in_comment:
                collected.append(raw_line.rstrip("\n"))
            if "-->" in stripped:
                in_comment = False
    return collected


def _summarize_block(block_lines: list[str]) -> str:
    parts: list[str] = []
    for raw_line in block_lines:
        line = raw_line.strip()
        if not line or line.startswith("<!--") or line.startswith("-->"):
            continue
        if line.startswith("### "):
            continue
        if line.startswith("- "):
            line = line[2:].strip()
        parts.append(line)
        if len(parts) >= 3:
            break
    compact = " ".join(parts).strip()
    return compact[:180].rstrip()


def read_active_autonomy_request_summaries(
    assistant_root: Path,
    *,
    limit: int = 5,
) -> list[str]:
    target = autonomy_requests_path(assistant_root)
    if not target.exists():
        return []

    text = target.read_text(encoding="utf-8")
    section_lines = _extract_active_section(text.splitlines())
    if not section_lines:
        return []

    summaries: list[str] = []
    current_title: str | None = None
    current_block: list[str] = []

    def flush() -> None:
        nonlocal current_title, current_block
        if not current_title:
            return
        summary = _summarize_block(current_block)
        line = current_title
        if summary:
            line = f"{line} — {summary}"
        summaries.append(line)
        current_title = None
        current_block = []

    for raw_line in section_lines:
        stripped = raw_line.strip()
        if stripped.startswith("### "):
            flush()
            current_title = stripped[4:].strip()
            continue
        if current_title is not None:
            current_block.append(raw_line)
    flush()

    return summaries[: max(1, int(limit))]
