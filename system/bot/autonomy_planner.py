from __future__ import annotations

from dataclasses import dataclass

AUTONOMY_NEXT_START = "[[autonomy-next]]"
AUTONOMY_NEXT_END = "[[/autonomy-next]]"
MISSION_PLAN_START = "[[mission-plan]]"
MISSION_PLAN_END = "[[/mission-plan]]"
SELF_REVIEW_START = "[[self-review]]"
SELF_REVIEW_END = "[[/self-review]]"


@dataclass(frozen=True)
class WakeupDecision:
    action: str
    plan_mode: str
    root_objective: str
    success_criteria: str
    current_stage: str
    next_stage: str
    title: str
    kind: str
    priority: int
    details: str
    result_text: str
    mission_status: str
    stage_status: str
    checkpoint_summary: str
    why_not_done_now: str
    blocker_type: str
    goal_check: str
    progress_delta: str
    drift_risk: str
    why_not_finished_now: str
    next_step_justification: str


@dataclass(frozen=True)
class AutonomyContinuation:
    action: str
    title: str
    kind: str
    priority: int
    delay_sec: int
    details: str


@dataclass(frozen=True)
class AutonomySelfReview:
    change: str
    why: str
    risk: str
    check: str


@dataclass(frozen=True)
class AutonomyControlDecision:
    verdict: str
    reason: str


@dataclass(frozen=True)
class MissionStage:
    title: str
    goal: str
    done_when: str
    status: str
    completion_summary: str


@dataclass(frozen=True)
class MissionPlan:
    stages: list[MissionStage]


def _extract_block_lines(text: str, start_marker: str, end_marker: str) -> tuple[str, list[str] | None]:
    lines = (text or "").splitlines()
    start_index: int | None = None
    end_index: int | None = None

    for index, raw_line in enumerate(lines):
        if raw_line.strip().lower() == start_marker:
            start_index = index

    if start_index is None:
        return (text or "").strip(), None

    for index in range(start_index + 1, len(lines)):
        if lines[index].strip().lower() == end_marker:
            end_index = index
            break

    if end_index is None:
        return (text or "").strip(), None

    clean_lines = lines[:start_index] + lines[end_index + 1 :]
    clean_text = "\n".join(clean_lines).strip()
    return clean_text, lines[start_index + 1 : end_index]


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
    raw_lines = (text or "").splitlines()
    fields, details_lines, result_lines = _parse_multisection_fields(raw_lines)
    trailing_fields: dict[str, str] = {}
    for raw_line in raw_lines:
        line = raw_line.strip()
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        normalized_key = key.strip().upper()
        if normalized_key in {
            "PLAN_MODE",
            "ROOT_OBJECTIVE",
            "SUCCESS_CRITERIA",
            "CURRENT_STAGE",
            "NEXT_STAGE",
            "MISSION_STATUS",
            "STAGE_STATUS",
            "CHECKPOINT_SUMMARY",
            "WHY_NOT_DONE_NOW",
            "BLOCKER_TYPE",
            "GOAL_CHECK",
            "PROGRESS_DELTA",
            "DRIFT_RISK",
            "WHY_NOT_FINISHED_NOW",
            "NEXT_STEP_JUSTIFICATION",
        }:
            trailing_fields[normalized_key] = value.strip()

    action = fields.get("ACTION", "NOOP").strip().upper() or "NOOP"
    if action not in {"STEP", "COMPLETE"}:
        return WakeupDecision(
            "NOOP",
            "",
            "",
            "",
            "",
            "",
            "",
            "general",
            100,
            "",
            "",
            "",
            "",
            "",
            "",
            "none",
            "",
            "",
            "",
            "",
            "",
        )

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
    plan_mode = trailing_fields.get("PLAN_MODE", fields.get("PLAN_MODE", "")).strip().lower()
    if plan_mode not in {"single_pass", "staged"}:
        plan_mode = ""
    mission_status = trailing_fields.get("MISSION_STATUS", fields.get("MISSION_STATUS", "")).strip().lower()
    if mission_status not in {"continue_now", "follow_up_later", "complete", "blocked_user"}:
        mission_status = ""
    stage_status = trailing_fields.get("STAGE_STATUS", fields.get("STAGE_STATUS", "")).strip().lower()
    if stage_status not in {"continue_stage", "stage_done", "blocked_user", "complete_mission"}:
        stage_status = ""
    blocker_type = trailing_fields.get("BLOCKER_TYPE", fields.get("BLOCKER_TYPE", "none")).strip().lower() or "none"
    if blocker_type not in {"none", "user", "external", "timebox", "context_missing"}:
        blocker_type = "none"
    return WakeupDecision(
        action,
        plan_mode,
        trailing_fields.get("ROOT_OBJECTIVE", fields.get("ROOT_OBJECTIVE", "")).strip(),
        trailing_fields.get("SUCCESS_CRITERIA", fields.get("SUCCESS_CRITERIA", "")).strip(),
        trailing_fields.get("CURRENT_STAGE", fields.get("CURRENT_STAGE", "")).strip(),
        trailing_fields.get("NEXT_STAGE", fields.get("NEXT_STAGE", "")).strip(),
        title,
        kind,
        priority,
        details,
        result_text,
        mission_status,
        stage_status,
        trailing_fields.get("CHECKPOINT_SUMMARY", fields.get("CHECKPOINT_SUMMARY", "")).strip(),
        trailing_fields.get("WHY_NOT_DONE_NOW", fields.get("WHY_NOT_DONE_NOW", "")).strip(),
        blocker_type,
        trailing_fields.get("GOAL_CHECK", fields.get("GOAL_CHECK", "")).strip(),
        trailing_fields.get("PROGRESS_DELTA", fields.get("PROGRESS_DELTA", "")).strip(),
        trailing_fields.get("DRIFT_RISK", fields.get("DRIFT_RISK", "")).strip(),
        trailing_fields.get("WHY_NOT_FINISHED_NOW", fields.get("WHY_NOT_FINISHED_NOW", "")).strip(),
        trailing_fields.get(
            "NEXT_STEP_JUSTIFICATION",
            fields.get("NEXT_STEP_JUSTIFICATION", ""),
        ).strip(),
    )


def extract_autonomy_continuation(text: str) -> tuple[str, AutonomyContinuation | None]:
    clean_text, block_lines = _extract_block_lines(
        text,
        AUTONOMY_NEXT_START,
        AUTONOMY_NEXT_END,
    )
    if block_lines is None:
        return clean_text, None
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


def extract_self_review(text: str) -> tuple[str, AutonomySelfReview | None]:
    clean_text, block_lines = _extract_block_lines(
        text,
        SELF_REVIEW_START,
        SELF_REVIEW_END,
    )
    if block_lines is None:
        return clean_text, None

    fields: dict[str, str] = {}
    for raw_line in block_lines:
        line = raw_line.strip()
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        fields[key.strip().upper()] = value.strip()

    review = AutonomySelfReview(
        change=fields.get("CHANGE", ""),
        why=fields.get("WHY", ""),
        risk=fields.get("RISK", ""),
        check=fields.get("CHECK", ""),
    )
    if not any([review.change, review.why, review.risk, review.check]):
        return clean_text, None
    return clean_text, review


def extract_mission_plan(text: str) -> tuple[str, MissionPlan | None]:
    clean_text, block_lines = _extract_block_lines(
        text,
        MISSION_PLAN_START,
        MISSION_PLAN_END,
    )
    if block_lines is None:
        return clean_text, None

    stages: list[MissionStage] = []
    current: dict[str, str] | None = None
    for raw_line in block_lines:
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("### "):
            if current is not None and current.get("title"):
                stages.append(
                    MissionStage(
                        title=current.get("title", ""),
                        goal=current.get("goal", ""),
                        done_when=current.get("done_when", ""),
                        status=current.get("status", "pending") or "pending",
                        completion_summary=current.get("completion_summary", ""),
                    )
                )
            current = {
                "title": line[4:].strip(),
                "goal": "",
                "done_when": "",
                "status": "pending",
                "completion_summary": "",
            }
            continue
        if current is None or ":" not in line:
            continue
        key, value = line.split(":", 1)
        normalized_key = key.strip().lower()
        normalized_value = value.strip()
        if normalized_key in {"goal", "done_when", "status", "completion_summary"}:
            current[normalized_key] = normalized_value
    if current is not None and current.get("title"):
        stages.append(
            MissionStage(
                title=current.get("title", ""),
                goal=current.get("goal", ""),
                done_when=current.get("done_when", ""),
                status=current.get("status", "pending") or "pending",
                completion_summary=current.get("completion_summary", ""),
            )
        )
    if not stages:
        return clean_text, None
    return clean_text, MissionPlan(stages=stages[:6])


def parse_control_decision(text: str) -> AutonomyControlDecision:
    fields: dict[str, str] = {}
    for raw_line in (text or "").splitlines():
        line = raw_line.strip()
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        fields[key.strip().upper()] = value.strip()
    verdict = fields.get("VERDICT", "").strip().upper()
    if verdict not in {
        "APPROVE_CONTINUE_NOW",
        "APPROVE_FOLLOWUP",
        "FORCE_STAGE_DONE",
        "FORCE_COMPLETE",
        "FORCE_BLOCKED_USER",
        "REJECT_AS_MICROSTEP",
    }:
        verdict = "APPROVE_FOLLOWUP"
    return AutonomyControlDecision(verdict=verdict, reason=fields.get("REASON", "").strip())
