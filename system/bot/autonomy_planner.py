from __future__ import annotations

from dataclasses import dataclass

AUTONOMY_NEXT_START = "[[autonomy-next]]"
AUTONOMY_NEXT_END = "[[/autonomy-next]]"


@dataclass(frozen=True)
class WakeupDecision:
    action: str
    title: str
    kind: str
    priority: int
    details: str
    result_text: str


@dataclass(frozen=True)
class AutonomyContinuation:
    action: str
    title: str
    kind: str
    priority: int
    delay_sec: int
    details: str


def _parse_multisection_fields(lines: list[str]) -> tuple[dict[str, str], list[str], list[str]]:
    fields: dict[str, str] = {}
    details_lines: list[str] = []
    result_lines: list[str] = []
    section: str | None = None

    for raw_line in lines:
        line = raw_line.rstrip()
        if section == "DETAILS":
            if ":" in line:
                key, value = line.split(":", 1)
                normalized_key = key.strip().upper()
                if normalized_key == "RESULT":
                    section = "RESULT"
                    if value.strip():
                        result_lines.append(value.strip())
                    continue
            details_lines.append(raw_line)
            continue
        if section == "RESULT":
            result_lines.append(raw_line)
            continue
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        normalized_key = key.strip().upper()
        normalized_value = value.strip()
        if normalized_key == "DETAILS":
            section = "DETAILS"
            if normalized_value:
                details_lines.append(normalized_value)
            continue
        if normalized_key == "RESULT":
            section = "RESULT"
            if normalized_value:
                result_lines.append(normalized_value)
            continue
        fields[normalized_key] = normalized_value

    return fields, details_lines, result_lines


def parse_wakeup_decision(text: str) -> WakeupDecision:
    fields, details_lines, result_lines = _parse_multisection_fields((text or "").splitlines())

    action = fields.get("ACTION", "NOOP").strip().upper() or "NOOP"
    if action not in {"STEP", "COMPLETE"}:
        return WakeupDecision("NOOP", "", "general", 100, "", "")

    title = fields.get("TITLE", "").strip()
    kind = fields.get("KIND", "general").strip().lower() or "general"
    priority_raw = fields.get("PRIORITY", "100").strip()
    try:
        priority = int(priority_raw)
    except ValueError:
        priority = 100
    priority = min(500, max(1, priority))

    details = "\n".join(details_lines).strip()
    result_text = "\n".join(result_lines).strip()
    return WakeupDecision(action, title, kind, priority, details, result_text)


def extract_autonomy_continuation(text: str) -> tuple[str, AutonomyContinuation | None]:
    lines = (text or "").splitlines()
    start_index: int | None = None
    end_index: int | None = None

    for index, raw_line in enumerate(lines):
        if raw_line.strip().lower() == AUTONOMY_NEXT_START:
            start_index = index

    if start_index is None:
        return (text or "").strip(), None

    for index in range(start_index + 1, len(lines)):
        if lines[index].strip().lower() == AUTONOMY_NEXT_END:
            end_index = index
            break

    if end_index is None:
        return (text or "").strip(), None

    clean_lines = lines[:start_index] + lines[end_index + 1 :]
    clean_text = "\n".join(clean_lines).strip()
    block_lines = lines[start_index + 1 : end_index]
    fields, details_lines, _ = _parse_multisection_fields(block_lines)

    action = fields.get("ACTION", "NOOP").strip().upper() or "NOOP"
    if action != "ENQUEUE":
        return clean_text, None

    title = fields.get("TITLE", "").strip()
    if not title:
        return clean_text, None

    kind = fields.get("KIND", "general").strip().lower() or "general"
    priority_raw = fields.get("PRIORITY", "100").strip()
    delay_raw = fields.get("DELAY_SEC", "0").strip()

    try:
        priority = int(priority_raw)
    except ValueError:
        priority = 100
    priority = min(500, max(1, priority))

    try:
        delay_sec = int(delay_raw)
    except ValueError:
        delay_sec = 0
    delay_sec = min(7 * 24 * 3600, max(0, delay_sec))

    details = "\n".join(details_lines).strip()
    return (
        clean_text,
        AutonomyContinuation(
            action="ENQUEUE",
            title=title,
            kind=kind,
            priority=priority,
            delay_sec=delay_sec,
            details=details,
        ),
    )
