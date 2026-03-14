from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
import sqlite3
import threading
from datetime import datetime, timedelta, timezone


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_dt(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


@dataclass
class AutonomyTask:
    id: int
    chat_id: int
    mission_id: int | None
    kind: str
    title: str
    details: str
    priority: int
    status: str
    created_at: str
    scheduled_for: str
    parent_task_id: int | None
    source: str
    started_at: str | None
    finished_at: str | None
    blocked_user_signal: int | None
    result_text: str
    error_text: str
    continuation_count: int = 0
    schedule_id: int | None = None


@dataclass
class AutonomyMission:
    id: int
    chat_id: int
    source: str
    root_objective: str
    success_criteria: str
    plan_state: str
    plan_json: list[dict[str, object]]
    current_stage_index: int
    status: str
    started_at: str
    updated_at: str
    completed_at: str | None
    blocked_reason: str
    current_focus: str
    plan_updated_at: str | None
    last_checkpoint_summary: str
    last_self_check_summary: str


@dataclass
class AutonomySchedule:
    id: int
    chat_id: int
    title: str
    prompt_text: str
    timezone: str
    recurrence_kind: str
    recurrence_json: dict[str, object]
    next_run_at: str
    last_enqueued_at: str | None
    last_started_at: str | None
    last_finished_at: str | None
    last_status: str
    delivery_hint: str
    active: bool
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class ActiveMission:
    task_id: int | None
    title: str
    details: str
    kind: str
    source: str
    phase: str
    scheduled_for: str


class AutonomyStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(
            str(self._db_path), check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        with self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS autonomy_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL DEFAULT 0,
                    mission_id INTEGER,
                    schedule_id INTEGER,
                    kind TEXT NOT NULL,
                    title TEXT NOT NULL,
                    details TEXT NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 100,
                    status TEXT NOT NULL,
                    parent_task_id INTEGER,
                    source TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    scheduled_for TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    blocked_user_signal INTEGER,
                    continuation_count INTEGER NOT NULL DEFAULT 0,
                    result_text TEXT NOT NULL DEFAULT '',
                    error_text TEXT NOT NULL DEFAULT ''
                )
                """
            )
            columns = {
                str(row["name"])
                for row in self._conn.execute("PRAGMA table_info(autonomy_tasks)").fetchall()
            }
            if "chat_id" not in columns:
                self._conn.execute(
                    "ALTER TABLE autonomy_tasks ADD COLUMN chat_id INTEGER NOT NULL DEFAULT 0"
                )
            if "mission_id" not in columns:
                self._conn.execute(
                    "ALTER TABLE autonomy_tasks ADD COLUMN mission_id INTEGER"
                )
            if "schedule_id" not in columns:
                self._conn.execute(
                    "ALTER TABLE autonomy_tasks ADD COLUMN schedule_id INTEGER"
                )
            if "parent_task_id" not in columns:
                self._conn.execute(
                    "ALTER TABLE autonomy_tasks ADD COLUMN parent_task_id INTEGER"
                )
            if "blocked_user_signal" not in columns:
                self._conn.execute(
                    "ALTER TABLE autonomy_tasks ADD COLUMN blocked_user_signal INTEGER"
                )
            if "continuation_count" not in columns:
                self._conn.execute(
                    "ALTER TABLE autonomy_tasks ADD COLUMN continuation_count INTEGER NOT NULL DEFAULT 0"
                )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS autonomy_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS autonomy_missions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL DEFAULT 0,
                    source TEXT NOT NULL,
                    root_objective TEXT NOT NULL,
                    success_criteria TEXT NOT NULL,
                    plan_state TEXT NOT NULL DEFAULT 'single_pass',
                    plan_json TEXT NOT NULL DEFAULT '[]',
                    current_stage_index INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    completed_at TEXT,
                    blocked_reason TEXT NOT NULL DEFAULT '',
                    current_focus TEXT NOT NULL DEFAULT '',
                    plan_updated_at TEXT,
                    last_checkpoint_summary TEXT NOT NULL DEFAULT '',
                    last_self_check_summary TEXT NOT NULL DEFAULT ''
                )
                """
            )
            mission_columns = {
                str(row["name"])
                for row in self._conn.execute("PRAGMA table_info(autonomy_missions)").fetchall()
            }
            if "plan_state" not in mission_columns:
                self._conn.execute(
                    "ALTER TABLE autonomy_missions ADD COLUMN plan_state TEXT NOT NULL DEFAULT 'single_pass'"
                )
            if "plan_json" not in mission_columns:
                self._conn.execute(
                    "ALTER TABLE autonomy_missions ADD COLUMN plan_json TEXT NOT NULL DEFAULT '[]'"
                )
            if "current_stage_index" not in mission_columns:
                self._conn.execute(
                    "ALTER TABLE autonomy_missions ADD COLUMN current_stage_index INTEGER NOT NULL DEFAULT 0"
                )
            if "plan_updated_at" not in mission_columns:
                self._conn.execute(
                    "ALTER TABLE autonomy_missions ADD COLUMN plan_updated_at TEXT"
                )
            if "last_checkpoint_summary" not in mission_columns:
                self._conn.execute(
                    "ALTER TABLE autonomy_missions ADD COLUMN last_checkpoint_summary TEXT NOT NULL DEFAULT ''"
                )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_autonomy_missions_chat_status
                ON autonomy_missions(chat_id, status, updated_at DESC)
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_autonomy_tasks_chat_status
                ON autonomy_tasks(chat_id, status, scheduled_for, priority, id)
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_autonomy_tasks_mission
                ON autonomy_tasks(mission_id, id DESC)
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_autonomy_tasks_schedule
                ON autonomy_tasks(schedule_id, status, scheduled_for, id)
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS autonomy_schedules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL DEFAULT 0,
                    title TEXT NOT NULL,
                    prompt_text TEXT NOT NULL,
                    timezone TEXT NOT NULL DEFAULT 'Europe/Moscow',
                    recurrence_kind TEXT NOT NULL,
                    recurrence_json TEXT NOT NULL DEFAULT '{}',
                    next_run_at TEXT,
                    last_enqueued_at TEXT,
                    last_started_at TEXT,
                    last_finished_at TEXT,
                    last_status TEXT NOT NULL DEFAULT '',
                    delivery_hint TEXT NOT NULL DEFAULT 'plain',
                    active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            schedule_columns = {
                str(row["name"])
                for row in self._conn.execute("PRAGMA table_info(autonomy_schedules)").fetchall()
            }
            if "delivery_hint" not in schedule_columns:
                self._conn.execute(
                    "ALTER TABLE autonomy_schedules ADD COLUMN delivery_hint TEXT NOT NULL DEFAULT 'plain'"
                )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_autonomy_schedules_chat_active_next
                ON autonomy_schedules(chat_id, active, next_run_at, id)
                """
            )

    def enqueue_task(
        self,
        title: str,
        details: str = "",
        *,
        chat_id: int = 0,
        mission_id: int | None = None,
        schedule_id: int | None = None,
        kind: str = "general",
        priority: int = 100,
        scheduled_for: str | None = None,
        parent_task_id: int | None = None,
        source: str = "assistant",
    ) -> int:
        when = scheduled_for or _utc_now()
        with self._lock, self._conn:
            cursor = self._conn.execute(
                """
                INSERT INTO autonomy_tasks (
                    chat_id,
                    mission_id,
                    schedule_id,
                    kind,
                    title,
                    details,
                    priority,
                    status,
                    parent_task_id,
                    source,
                    created_at,
                    scheduled_for
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    chat_id,
                    mission_id,
                    schedule_id,
                    kind,
                    title,
                    details,
                    priority,
                    "pending",
                    parent_task_id,
                    source,
                    _utc_now(),
                    when,
                ),
            )
            return int(cursor.lastrowid)

    def claim_next_ready_task(
        self,
        *,
        chat_id: int | None = None,
        now: str | None = None,
    ) -> AutonomyTask | None:
        effective_now = now or _utc_now()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            if chat_id is None:
                row = self._conn.execute(
                    """
                    SELECT * FROM autonomy_tasks
                    WHERE status = 'pending'
                      AND scheduled_for <= ?
                    ORDER BY priority ASC, id ASC
                    LIMIT 1
                    """,
                    (effective_now,),
                ).fetchone()
            else:
                row = self._conn.execute(
                    """
                    SELECT * FROM autonomy_tasks
                    WHERE status = 'pending'
                      AND scheduled_for <= ?
                      AND chat_id = ?
                    ORDER BY priority ASC, id ASC
                    LIMIT 1
                    """,
                    (effective_now, chat_id),
                ).fetchone()
            if row is None:
                self._conn.execute("COMMIT")
                return None
            started_at = _utc_now()
            self._conn.execute(
                """
                UPDATE autonomy_tasks
                SET status = 'running', started_at = ?, error_text = ''
                WHERE id = ?
                """,
                (started_at, row["id"]),
            )
            self._conn.execute("COMMIT")
            return self._row_to_task(
                {
                    **dict(row),
                    "status": "running",
                    "started_at": started_at,
                    "error_text": "",
                }
            )

    def complete_task(self, task_id: int, result_text: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE autonomy_tasks
                SET status = 'done',
                    finished_at = ?,
                    blocked_user_signal = NULL,
                    result_text = ?,
                    error_text = ''
                WHERE id = ?
                """,
                (_utc_now(), result_text, task_id),
            )

    def wait_for_user(self, task_id: int, result_text: str, *, user_signal: int) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE autonomy_tasks
                SET status = 'waiting_user',
                    finished_at = ?,
                    blocked_user_signal = ?,
                    result_text = ?,
                    error_text = ''
                WHERE id = ?
                """,
                (_utc_now(), user_signal, result_text, task_id),
            )

    def fail_task(self, task_id: int, error_text: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE autonomy_tasks
                SET status = 'failed',
                    finished_at = ?,
                    error_text = ?
                WHERE id = ?
                """,
                (_utc_now(), error_text, task_id),
            )

    def requeue_task(
        self,
        task_id: int,
        *,
        scheduled_for: str | None = None,
        priority: int | None = None,
    ) -> None:
        when = scheduled_for or _utc_now()
        with self._lock, self._conn:
            if priority is None:
                self._conn.execute(
                    """
                    UPDATE autonomy_tasks
                    SET status = 'pending',
                    scheduled_for = ?,
                    started_at = NULL,
                    finished_at = NULL,
                    blocked_user_signal = NULL,
                    result_text = '',
                    error_text = ''
                    WHERE id = ?
                    """,
                    (when, task_id),
                )
                return
            self._conn.execute(
                """
                UPDATE autonomy_tasks
                SET status = 'pending',
                    scheduled_for = ?,
                    priority = ?,
                    started_at = NULL,
                    finished_at = NULL,
                    blocked_user_signal = NULL,
                    result_text = '',
                    error_text = ''
                WHERE id = ?
                """,
                (when, priority, task_id),
            )

    def continue_task(
        self,
        task_id: int,
        *,
        mission_id: int | None = None,
        title: str,
        details: str,
        kind: str,
        priority: int,
        scheduled_for: str,
        progress_text: str = "",
    ) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE autonomy_tasks
                SET title = ?,
                    details = ?,
                    kind = ?,
                    priority = ?,
                    mission_id = COALESCE(?, mission_id),
                    status = 'pending',
                    scheduled_for = ?,
                    started_at = NULL,
                    finished_at = NULL,
                    blocked_user_signal = NULL,
                    continuation_count = continuation_count + 1,
                    result_text = ?,
                    error_text = ''
                WHERE id = ?
                """,
                (
                    title,
                    details,
                    kind,
                    priority,
                    mission_id,
                    scheduled_for,
                    progress_text,
                    task_id,
                ),
            )

    def set_task_mission(self, task_id: int, mission_id: int) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE autonomy_tasks
                SET mission_id = ?
                WHERE id = ?
                """,
                (mission_id, task_id),
            )

    def create_mission(
        self,
        *,
        chat_id: int,
        source: str,
        root_objective: str,
        success_criteria: str,
        plan_state: str = "single_pass",
        plan_json: list[dict[str, object]] | None = None,
        current_stage_index: int = 0,
        current_focus: str = "",
        status: str = "active",
        plan_updated_at: str | None = None,
        last_checkpoint_summary: str = "",
        last_self_check_summary: str = "",
    ) -> int:
        now = _utc_now()
        with self._lock, self._conn:
            cursor = self._conn.execute(
                """
                INSERT INTO autonomy_missions (
                    chat_id,
                    source,
                    root_objective,
                    success_criteria,
                    plan_state,
                    plan_json,
                    current_stage_index,
                    status,
                    started_at,
                    updated_at,
                    current_focus,
                    plan_updated_at,
                    last_checkpoint_summary,
                    last_self_check_summary
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    chat_id,
                    source.strip() or "initiative",
                    root_objective.strip(),
                    success_criteria.strip(),
                    (plan_state.strip() or "single_pass"),
                    json.dumps(plan_json or [], ensure_ascii=False),
                    max(0, int(current_stage_index)),
                    status.strip() or "active",
                    now,
                    now,
                    current_focus.strip(),
                    plan_updated_at,
                    last_checkpoint_summary.strip(),
                    last_self_check_summary.strip(),
                ),
            )
            return int(cursor.lastrowid)

    def get_mission(self, mission_id: int) -> AutonomyMission | None:
        row = self._conn.execute(
            """
            SELECT *
            FROM autonomy_missions
            WHERE id = ?
            """,
            (mission_id,),
        ).fetchone()
        if row is None:
            return None
        return self._row_to_mission(row)

    def get_live_mission(
        self,
        chat_id: int,
        *,
        source: str | None = None,
    ) -> AutonomyMission | None:
        params: list[object] = [chat_id]
        source_sql = ""
        if source:
            source_sql = "AND source = ?"
            params.append(source)
        row = self._conn.execute(
            f"""
            SELECT *
            FROM autonomy_missions
            WHERE chat_id = ?
              AND status IN ('active', 'blocked_user')
              {source_sql}
            ORDER BY
              CASE status WHEN 'active' THEN 0 ELSE 1 END,
              updated_at DESC,
              id DESC
            LIMIT 1
            """,
            params,
        ).fetchone()
        if row is None:
            return None
        return self._row_to_mission(row)

    def update_mission(
        self,
        mission_id: int,
        *,
        status: str | None = None,
        blocked_reason: str | None = None,
        current_focus: str | None = None,
        plan_state: str | None = None,
        plan_json: list[dict[str, object]] | None = None,
        current_stage_index: int | None = None,
        plan_updated_at: str | None = None,
        last_checkpoint_summary: str | None = None,
        last_self_check_summary: str | None = None,
        root_objective: str | None = None,
        success_criteria: str | None = None,
    ) -> None:
        assignments = ["updated_at = ?"]
        params: list[object] = [_utc_now()]
        if status is not None:
            assignments.append("status = ?")
            params.append(status)
            if status == "completed":
                assignments.append("completed_at = ?")
                params.append(_utc_now())
            elif status in {"active", "blocked_user", "abandoned"}:
                assignments.append("completed_at = NULL")
        if blocked_reason is not None:
            assignments.append("blocked_reason = ?")
            params.append(blocked_reason)
        if current_focus is not None:
            assignments.append("current_focus = ?")
            params.append(current_focus)
        if plan_state is not None:
            assignments.append("plan_state = ?")
            params.append(plan_state)
        if plan_json is not None:
            assignments.append("plan_json = ?")
            params.append(json.dumps(plan_json, ensure_ascii=False))
        if current_stage_index is not None:
            assignments.append("current_stage_index = ?")
            params.append(max(0, int(current_stage_index)))
        if plan_updated_at is not None:
            assignments.append("plan_updated_at = ?")
            params.append(plan_updated_at)
        if last_checkpoint_summary is not None:
            assignments.append("last_checkpoint_summary = ?")
            params.append(last_checkpoint_summary)
        if last_self_check_summary is not None:
            assignments.append("last_self_check_summary = ?")
            params.append(last_self_check_summary)
        if root_objective is not None:
            assignments.append("root_objective = ?")
            params.append(root_objective)
        if success_criteria is not None:
            assignments.append("success_criteria = ?")
            params.append(success_criteria)
        params.append(mission_id)
        with self._lock, self._conn:
            self._conn.execute(
                f"""
                UPDATE autonomy_missions
                SET {", ".join(assignments)}
                WHERE id = ?
                """,
                params,
            )

    def complete_mission(
        self,
        mission_id: int,
        *,
        current_focus: str = "",
        last_self_check_summary: str = "",
    ) -> None:
        self.update_mission(
            mission_id,
            status="completed",
            blocked_reason="",
            current_focus=current_focus,
            plan_state="single_pass",
            plan_json=[],
            current_stage_index=0,
            plan_updated_at="",
            last_checkpoint_summary="",
            last_self_check_summary=last_self_check_summary,
        )

    def block_mission(
        self,
        mission_id: int,
        *,
        reason: str,
        current_focus: str = "",
        last_checkpoint_summary: str = "",
        last_self_check_summary: str = "",
    ) -> None:
        self.update_mission(
            mission_id,
            status="blocked_user",
            blocked_reason=reason,
            current_focus=current_focus,
            last_checkpoint_summary=last_checkpoint_summary,
            last_self_check_summary=last_self_check_summary,
        )

    def abandon_mission(
        self,
        mission_id: int,
        *,
        reason: str = "",
        current_focus: str = "",
        last_self_check_summary: str = "",
    ) -> None:
        self.update_mission(
            mission_id,
            status="abandoned",
            blocked_reason=reason,
            current_focus=current_focus,
            last_self_check_summary=last_self_check_summary,
        )

    def list_mission_tasks(
        self,
        mission_id: int,
        *,
        limit: int = 10,
    ) -> list[AutonomyTask]:
        rows = self._conn.execute(
            """
            SELECT *
            FROM autonomy_tasks
            WHERE mission_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (mission_id, max(1, int(limit))),
        ).fetchall()
        return [self._row_to_task(row) for row in rows]

    def resume_waiting_tasks(self, chat_id: int, *, user_signal: int, now: str | None = None) -> int:
        effective_now = now or _utc_now()
        with self._lock, self._conn:
            cursor = self._conn.execute(
                """
                UPDATE autonomy_tasks
                SET status = 'pending',
                    scheduled_for = ?,
                    started_at = NULL,
                    finished_at = NULL,
                    blocked_user_signal = NULL,
                    error_text = ''
                WHERE chat_id = ?
                  AND status = 'waiting_user'
                  AND blocked_user_signal IS NOT NULL
                  AND blocked_user_signal < ?
                """,
                (effective_now, chat_id, user_signal),
            )
            return int(cursor.rowcount or 0)

    def counts(self) -> dict[str, int]:
        rows = self._conn.execute(
            """
            SELECT status, COUNT(*) AS cnt
            FROM autonomy_tasks
            GROUP BY status
            """
        ).fetchall()
        result = {"pending": 0, "running": 0, "waiting_user": 0, "done": 0, "failed": 0}
        for row in rows:
            result[str(row["status"])] = int(row["cnt"])
        return result

    def has_active_task_for_schedule(self, chat_id: int, schedule_id: int) -> bool:
        row = self._conn.execute(
            """
            SELECT 1
            FROM autonomy_tasks
            WHERE chat_id = ?
              AND schedule_id = ?
              AND status IN ('pending', 'running', 'waiting_user')
            LIMIT 1
            """,
            (chat_id, schedule_id),
        ).fetchone()
        return row is not None

    def create_schedule(
        self,
        *,
        chat_id: int,
        title: str,
        prompt_text: str,
        timezone: str,
        recurrence_kind: str,
        recurrence_json: dict[str, object],
        next_run_at: str,
        delivery_hint: str = "plain",
        active: bool = True,
    ) -> int:
        now = _utc_now()
        with self._lock, self._conn:
            cursor = self._conn.execute(
                """
                INSERT INTO autonomy_schedules (
                    chat_id,
                    title,
                    prompt_text,
                    timezone,
                    recurrence_kind,
                    recurrence_json,
                    next_run_at,
                    last_status,
                    delivery_hint,
                    active,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    chat_id,
                    title.strip(),
                    prompt_text.strip(),
                    timezone.strip() or "Europe/Moscow",
                    recurrence_kind.strip(),
                    json.dumps(recurrence_json, ensure_ascii=False),
                    next_run_at.strip(),
                    "scheduled",
                    delivery_hint.strip() or "plain",
                    1 if active else 0,
                    now,
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def get_schedule(self, schedule_id: int, *, chat_id: int | None = None) -> AutonomySchedule | None:
        params: list[object] = [schedule_id]
        chat_sql = ""
        if chat_id is not None:
            chat_sql = "AND chat_id = ?"
            params.append(chat_id)
        row = self._conn.execute(
            f"""
            SELECT *
            FROM autonomy_schedules
            WHERE id = ?
              {chat_sql}
            LIMIT 1
            """,
            params,
        ).fetchone()
        if row is None:
            return None
        return self._row_to_schedule(row)

    def list_schedules(
        self,
        *,
        chat_id: int,
        include_inactive: bool = True,
        limit: int = 100,
    ) -> list[AutonomySchedule]:
        where = "WHERE chat_id = ?"
        params: list[object] = [chat_id]
        if not include_inactive:
            where += " AND active = 1"
        params.append(max(1, int(limit)))
        rows = self._conn.execute(
            f"""
            SELECT *
            FROM autonomy_schedules
            {where}
            ORDER BY active DESC, next_run_at IS NULL, next_run_at ASC, id ASC
            LIMIT ?
            """,
            params,
        ).fetchall()
        return [self._row_to_schedule(row) for row in rows]

    def list_due_schedules(
        self,
        *,
        chat_id: int,
        now: str | None = None,
    ) -> list[AutonomySchedule]:
        effective_now = now or _utc_now()
        rows = self._conn.execute(
            """
            SELECT *
            FROM autonomy_schedules
            WHERE chat_id = ?
              AND active = 1
              AND next_run_at IS NOT NULL
              AND next_run_at <= ?
            ORDER BY next_run_at ASC, id ASC
            """,
            (chat_id, effective_now),
        ).fetchall()
        return [self._row_to_schedule(row) for row in rows]

    def get_next_schedule_run(self, chat_id: int) -> str:
        row = self._conn.execute(
            """
            SELECT next_run_at
            FROM autonomy_schedules
            WHERE chat_id = ?
              AND active = 1
              AND next_run_at IS NOT NULL
            ORDER BY next_run_at ASC, id ASC
            LIMIT 1
            """,
            (chat_id,),
        ).fetchone()
        if row is None:
            return ""
        return str(row["next_run_at"] or "")

    def update_schedule(
        self,
        schedule_id: int,
        *,
        title: str | None = None,
        prompt_text: str | None = None,
        timezone: str | None = None,
        recurrence_kind: str | None = None,
        recurrence_json: dict[str, object] | None = None,
        next_run_at: str | None = None,
        last_enqueued_at: str | None = None,
        last_started_at: str | None = None,
        last_finished_at: str | None = None,
        last_status: str | None = None,
        delivery_hint: str | None = None,
        active: bool | None = None,
    ) -> None:
        assignments = ["updated_at = ?"]
        params: list[object] = [_utc_now()]
        if title is not None:
            assignments.append("title = ?")
            params.append(title.strip())
        if prompt_text is not None:
            assignments.append("prompt_text = ?")
            params.append(prompt_text.strip())
        if timezone is not None:
            assignments.append("timezone = ?")
            params.append(timezone.strip() or "Europe/Moscow")
        if recurrence_kind is not None:
            assignments.append("recurrence_kind = ?")
            params.append(recurrence_kind.strip())
        if recurrence_json is not None:
            assignments.append("recurrence_json = ?")
            params.append(json.dumps(recurrence_json, ensure_ascii=False))
        if next_run_at is not None:
            assignments.append("next_run_at = ?")
            params.append(next_run_at.strip())
        if last_enqueued_at is not None:
            assignments.append("last_enqueued_at = ?")
            params.append(last_enqueued_at)
        if last_started_at is not None:
            assignments.append("last_started_at = ?")
            params.append(last_started_at)
        if last_finished_at is not None:
            assignments.append("last_finished_at = ?")
            params.append(last_finished_at)
        if last_status is not None:
            assignments.append("last_status = ?")
            params.append(last_status.strip())
        if delivery_hint is not None:
            assignments.append("delivery_hint = ?")
            params.append(delivery_hint.strip() or "plain")
        if active is not None:
            assignments.append("active = ?")
            params.append(1 if active else 0)
        params.append(schedule_id)
        with self._lock, self._conn:
            self._conn.execute(
                f"""
                UPDATE autonomy_schedules
                SET {", ".join(assignments)}
                WHERE id = ?
                """,
                params,
            )

    def pause_schedule(self, schedule_id: int, *, chat_id: int) -> bool:
        with self._lock, self._conn:
            cursor = self._conn.execute(
                """
                UPDATE autonomy_schedules
                SET active = 0,
                    updated_at = ?,
                    last_status = 'paused'
                WHERE id = ?
                  AND chat_id = ?
                """,
                (_utc_now(), schedule_id, chat_id),
            )
            return int(cursor.rowcount or 0) > 0

    def resume_schedule(self, schedule_id: int, *, chat_id: int, next_run_at: str) -> bool:
        with self._lock, self._conn:
            cursor = self._conn.execute(
                """
                UPDATE autonomy_schedules
                SET active = 1,
                    next_run_at = ?,
                    updated_at = ?,
                    last_status = 'scheduled'
                WHERE id = ?
                  AND chat_id = ?
                """,
                (next_run_at, _utc_now(), schedule_id, chat_id),
            )
            return int(cursor.rowcount or 0) > 0

    def delete_schedule(self, schedule_id: int, *, chat_id: int) -> bool:
        with self._lock, self._conn:
            cursor = self._conn.execute(
                """
                DELETE FROM autonomy_schedules
                WHERE id = ?
                  AND chat_id = ?
                """,
                (schedule_id, chat_id),
            )
            return int(cursor.rowcount or 0) > 0

    def set_pending_schedule_confirmation(self, chat_id: int, payload: dict[str, object]) -> None:
        self.set_meta(
            f"schedule:pending_confirmation:{chat_id}",
            json.dumps(payload, ensure_ascii=False),
        )

    def get_pending_schedule_confirmation(self, chat_id: int) -> dict[str, object] | None:
        raw = self.get_meta(f"schedule:pending_confirmation:{chat_id}", "").strip()
        if not raw:
            return None
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return None
        if not isinstance(payload, dict):
            return None
        return payload

    def clear_pending_schedule_confirmation(self, chat_id: int) -> None:
        self.set_meta(f"schedule:pending_confirmation:{chat_id}", "")

    def counts_for_chat(self, chat_id: int) -> dict[str, int]:
        rows = self._conn.execute(
            """
            SELECT status, COUNT(*) AS cnt
            FROM autonomy_tasks
            WHERE chat_id = ?
            GROUP BY status
            """,
            (chat_id,),
        ).fetchall()
        result = {"pending": 0, "running": 0, "waiting_user": 0, "done": 0, "failed": 0}
        for row in rows:
            result[str(row["status"])] = int(row["cnt"])
        return result

    def active_task_count(self, chat_id: int) -> int:
        row = self._conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM autonomy_tasks
            WHERE chat_id = ?
              AND status IN ('pending', 'running')
            """,
            (chat_id,),
        ).fetchone()
        if row is None:
            return 0
        return int(row["cnt"])

    def get_next_pending_scheduled_for(self, chat_id: int) -> str:
        row = self._conn.execute(
            """
            SELECT scheduled_for
            FROM autonomy_tasks
            WHERE chat_id = ?
              AND status = 'pending'
            ORDER BY scheduled_for ASC, priority ASC, id ASC
            LIMIT 1
            """,
            (chat_id,),
        ).fetchone()
        if row is None:
            return ""
        return str(row["scheduled_for"] or "")

    def get_next_pending_task(self, chat_id: int) -> AutonomyTask | None:
        row = self._conn.execute(
            """
            SELECT *
            FROM autonomy_tasks
            WHERE chat_id = ?
              AND status = 'pending'
            ORDER BY scheduled_for ASC, priority ASC, id ASC
            LIMIT 1
            """,
            (chat_id,),
        ).fetchone()
        if row is None:
            return None
        return self._row_to_task(row)

    def list_tasks(
        self,
        *,
        chat_id: int | None = None,
        statuses: set[str] | None = None,
        limit: int = 20,
        order_by: str = "recent",
    ) -> list[AutonomyTask]:
        safe_limit = max(1, int(limit))
        params: list[object] = []
        clauses: list[str] = []
        if chat_id is not None:
            clauses.append("chat_id = ?")
            params.append(chat_id)
        if statuses:
            placeholders = ", ".join("?" for _ in statuses)
            clauses.append(f"status IN ({placeholders})")
            params.extend(sorted(statuses))

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        if order_by == "priority":
            order_sql = "priority ASC, id ASC"
        else:
            order_sql = "id DESC"
        params.append(safe_limit)
        rows = self._conn.execute(
            f"""
            SELECT * FROM autonomy_tasks
            {where_sql}
            ORDER BY {order_sql}
            LIMIT ?
            """,
            params,
        ).fetchall()
        return [self._row_to_task(row) for row in rows]

    def set_meta(self, key: str, value: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO autonomy_meta(key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )

    def get_meta(self, key: str, default: str = "") -> str:
        row = self._conn.execute(
            "SELECT value FROM autonomy_meta WHERE key = ?",
            (key,),
        ).fetchone()
        if row is None:
            return default
        return str(row["value"])

    def mark_heartbeat(self, heartbeat_kind: str, at: str | None = None) -> str:
        effective_at = at or _utc_now()
        self.set_meta(f"heartbeat:{heartbeat_kind}", effective_at)
        self.set_meta("heartbeat:last_kind", heartbeat_kind)
        self.set_meta("heartbeat:last_at", effective_at)
        return effective_at

    def get_heartbeat(self, heartbeat_kind: str) -> str:
        return self.get_meta(f"heartbeat:{heartbeat_kind}", "")

    def get_last_heartbeat_kind(self) -> str:
        return self.get_meta("heartbeat:last_kind", "")

    def get_last_heartbeat_at(self) -> str:
        return self.get_meta("heartbeat:last_at", "")

    def set_mode(self, chat_id: int, mode: str) -> None:
        self.set_meta(f"mode:{chat_id}", mode.strip())

    def get_mode(self, chat_id: int) -> str:
        return self.get_meta(f"mode:{chat_id}", "")

    def set_next_wakeup(self, chat_id: int, at: str) -> None:
        self.set_meta(f"next_wakeup:{chat_id}", at.strip())

    def get_next_wakeup(self, chat_id: int) -> str:
        return self.get_meta(f"next_wakeup:{chat_id}", "")

    def clear_next_wakeup(self, chat_id: int) -> None:
        self.set_meta(f"next_wakeup:{chat_id}", "")

    def schedule_next_wakeup_in(
        self,
        chat_id: int,
        delay_sec: int,
        *,
        now: str | None = None,
    ) -> str:
        effective_now = _parse_dt(now or _utc_now()) or datetime.now(timezone.utc)
        wake_dt = effective_now + timedelta(seconds=max(0, delay_sec))
        wake_at = wake_dt.isoformat()
        self.set_next_wakeup(chat_id, wake_at)
        return wake_at

    def wakeup_due(self, chat_id: int, *, now: str | None = None) -> bool:
        wake_at = _parse_dt(self.get_next_wakeup(chat_id))
        if wake_at is None:
            return True
        effective_now = _parse_dt(now or _utc_now()) or datetime.now(timezone.utc)
        return effective_now >= wake_at

    def seconds_until_next_wakeup(self, chat_id: int, *, now: str | None = None) -> float | None:
        wake_at = _parse_dt(self.get_next_wakeup(chat_id))
        if wake_at is None:
            return None
        effective_now = _parse_dt(now or _utc_now()) or datetime.now(timezone.utc)
        return (wake_at - effective_now).total_seconds()

    def set_active_mission(
        self,
        chat_id: int,
        *,
        title: str,
        details: str,
        kind: str,
        source: str,
        task_id: int | None = None,
        phase: str = "",
        scheduled_for: str = "",
    ) -> None:
        payload = {
            "task_id": task_id,
            "title": title.strip(),
            "details": details.strip(),
            "kind": kind.strip() or "general",
            "source": source.strip() or "assistant",
            "phase": phase.strip(),
            "scheduled_for": scheduled_for.strip(),
        }
        self.set_meta(
            f"mission:{chat_id}",
            json.dumps(payload, ensure_ascii=False),
        )

    def get_active_mission(self, chat_id: int) -> ActiveMission | None:
        raw = self.get_meta(f"mission:{chat_id}", "").strip()
        if not raw:
            return None
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return None
        return ActiveMission(
            task_id=int(payload["task_id"]) if payload.get("task_id") is not None else None,
            title=str(payload.get("title") or ""),
            details=str(payload.get("details") or ""),
            kind=str(payload.get("kind") or "general"),
            source=str(payload.get("source") or "assistant"),
            phase=str(payload.get("phase") or ""),
            scheduled_for=str(payload.get("scheduled_for") or ""),
        )

    def clear_active_mission(self, chat_id: int) -> None:
        self.set_meta(f"mission:{chat_id}", "")

    def get_last_seen_user_signal(self, chat_id: int) -> int:
        raw = self.get_meta(f"user_signal:last_seen:{chat_id}", "")
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    def set_last_seen_user_signal(self, chat_id: int, signal: int) -> None:
        self.set_meta(f"user_signal:last_seen:{chat_id}", str(signal))

    def notify_due(self, chat_id: int, cooldown_sec: int, now: str | None = None) -> bool:
        if cooldown_sec <= 0:
            return True
        effective_now_raw = now or _utc_now()
        effective_now = _parse_dt(effective_now_raw)
        last_sent = _parse_dt(self.get_meta(f"notify:last_sent:{chat_id}", ""))
        if effective_now is None or last_sent is None:
            return True
        return (effective_now - last_sent).total_seconds() >= cooldown_sec

    def mark_notify_sent(self, chat_id: int, at: str | None = None) -> str:
        effective_at = at or _utc_now()
        self.set_meta(f"notify:last_sent:{chat_id}", effective_at)
        return effective_at

    def get_notify_last_sent(self, chat_id: int) -> str:
        return self.get_meta(f"notify:last_sent:{chat_id}", "")

    def get_notify_last_fingerprint(self, chat_id: int) -> str:
        return self.get_meta(f"notify:last_fingerprint:{chat_id}", "")

    def mark_notify_fingerprint(self, chat_id: int, fingerprint: str) -> None:
        self.set_meta(f"notify:last_fingerprint:{chat_id}", fingerprint)

    def mark_idle_interest_prompt(
        self,
        chat_id: int,
        *,
        user_signal: int,
        at: str | None = None,
    ) -> str:
        effective_at = at or _utc_now()
        self.set_meta(f"idle:last_prompt_at:{chat_id}", effective_at)
        self.set_meta(f"idle:last_prompt_signal:{chat_id}", str(user_signal))
        return effective_at

    def get_idle_interest_prompt_at(self, chat_id: int) -> str:
        return self.get_meta(f"idle:last_prompt_at:{chat_id}", "")

    def get_idle_interest_prompt_signal(self, chat_id: int) -> int:
        raw = self.get_meta(f"idle:last_prompt_signal:{chat_id}", "")
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    def clear_idle_interest_prompt(self, chat_id: int) -> None:
        self.set_meta(f"idle:last_prompt_at:{chat_id}", "")
        self.set_meta(f"idle:last_prompt_signal:{chat_id}", "0")

    def idle_interest_prompt_due(
        self,
        chat_id: int,
        cooldown_sec: int,
        *,
        now: str | None = None,
    ) -> bool:
        if cooldown_sec <= 0:
            return True
        effective_now = _parse_dt(now or _utc_now())
        last_prompt = _parse_dt(self.get_idle_interest_prompt_at(chat_id))
        if effective_now is None or last_prompt is None:
            return True
        return (effective_now - last_prompt).total_seconds() >= cooldown_sec

    def mark_idle_snooze_until(self, chat_id: int, until: str) -> None:
        self.set_meta(f"idle:snooze_until:{chat_id}", until)

    def get_idle_snooze_until(self, chat_id: int) -> str:
        return self.get_meta(f"idle:snooze_until:{chat_id}", "")

    def clear_idle_snooze(self, chat_id: int) -> None:
        self.set_meta(f"idle:snooze_until:{chat_id}", "")

    def idle_snoozed(self, chat_id: int, *, now: str | None = None) -> bool:
        effective_now = _parse_dt(now or _utc_now())
        snoozed_until = _parse_dt(self.get_idle_snooze_until(chat_id))
        if effective_now is None or snoozed_until is None:
            return False
        return effective_now < snoozed_until

    def set_autonomy_paused(self, chat_id: int, paused: bool) -> None:
        self.set_meta(f"autonomy:paused:{chat_id}", "1" if paused else "0")

    def autonomy_paused(self, chat_id: int) -> bool:
        return self.get_meta(f"autonomy:paused:{chat_id}", "0") == "1"

    def set_guard_waiting_approval(self, chat_id: int, waiting: bool) -> None:
        self.set_meta(f"guard:waiting_approval:{chat_id}", "1" if waiting else "0")

    def guard_waiting_approval(self, chat_id: int) -> bool:
        return self.get_meta(f"guard:waiting_approval:{chat_id}", "0") == "1"

    def set_guard_approved_once(self, chat_id: int, approved: bool) -> None:
        self.set_meta(f"guard:approved_once:{chat_id}", "1" if approved else "0")

    def guard_approved_once(self, chat_id: int) -> bool:
        return self.get_meta(f"guard:approved_once:{chat_id}", "0") == "1"

    def set_guard_block_reason(self, chat_id: int, reason: str) -> None:
        self.set_meta(f"guard:block_reason:{chat_id}", reason.strip())

    def get_guard_block_reason(self, chat_id: int) -> str:
        return self.get_meta(f"guard:block_reason:{chat_id}", "")

    def set_guard_blocked_at(self, chat_id: int, at: str) -> None:
        self.set_meta(f"guard:blocked_at:{chat_id}", at.strip())

    def get_guard_blocked_at(self, chat_id: int) -> str:
        return self.get_meta(f"guard:blocked_at:{chat_id}", "")

    def set_guard_alert_message_id(self, chat_id: int, message_id: int | None) -> None:
        self.set_meta(
            f"guard:alert_message_id:{chat_id}",
            "" if message_id is None else str(int(message_id)),
        )

    def get_guard_alert_message_id(self, chat_id: int) -> int | None:
        raw = self.get_meta(f"guard:alert_message_id:{chat_id}", "")
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None

    def set_guard_last_alert_at(self, chat_id: int, at: str) -> None:
        self.set_meta(f"guard:last_alert_at:{chat_id}", at.strip())

    def get_guard_last_alert_at(self, chat_id: int) -> str:
        return self.get_meta(f"guard:last_alert_at:{chat_id}", "")

    def set_guard_session_started_at(self, chat_id: int, at: str) -> None:
        self.set_meta(f"guard:session_started_at:{chat_id}", at.strip())

    def get_guard_session_started_at(self, chat_id: int) -> str:
        return self.get_meta(f"guard:session_started_at:{chat_id}", "")

    def set_guard_session_last_activity_at(self, chat_id: int, at: str) -> None:
        self.set_meta(f"guard:session_last_activity_at:{chat_id}", at.strip())

    def get_guard_session_last_activity_at(self, chat_id: int) -> str:
        return self.get_meta(f"guard:session_last_activity_at:{chat_id}", "")

    def set_guard_recent_call_timestamps(self, chat_id: int, timestamps: list[str]) -> None:
        self.set_meta(
            f"guard:recent_codex_call_timestamps:{chat_id}",
            json.dumps(timestamps, ensure_ascii=False),
        )

    def get_guard_recent_call_timestamps(self, chat_id: int) -> list[str]:
        raw = self.get_meta(f"guard:recent_codex_call_timestamps:{chat_id}", "")
        if not raw.strip():
            return []
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return []
        if not isinstance(payload, list):
            return []
        return [str(item) for item in payload if str(item).strip()]

    def clear_guard_session(self, chat_id: int) -> None:
        self.set_guard_session_started_at(chat_id, "")
        self.set_guard_session_last_activity_at(chat_id, "")

    def clear_guard_block(self, chat_id: int) -> None:
        self.set_guard_waiting_approval(chat_id, False)
        self.set_guard_approved_once(chat_id, False)
        self.set_guard_block_reason(chat_id, "")
        self.set_guard_blocked_at(chat_id, "")
        self.set_guard_alert_message_id(chat_id, None)
        self.set_guard_last_alert_at(chat_id, "")

    @staticmethod
    def _row_to_task(row: sqlite3.Row | dict[str, object]) -> AutonomyTask:
        schedule_value = row["schedule_id"] if "schedule_id" in row.keys() else None
        return AutonomyTask(
            id=int(row["id"]),
            chat_id=int(row["chat_id"]),
            mission_id=int(row["mission_id"]) if row["mission_id"] is not None else None,
            kind=str(row["kind"]),
            title=str(row["title"]),
            details=str(row["details"]),
            priority=int(row["priority"]),
            status=str(row["status"]),
            created_at=str(row["created_at"]),
            scheduled_for=str(row["scheduled_for"]),
            parent_task_id=int(row["parent_task_id"]) if row["parent_task_id"] is not None else None,
            source=str(row["source"]),
            started_at=str(row["started_at"]) if row["started_at"] else None,
            finished_at=str(row["finished_at"]) if row["finished_at"] else None,
            blocked_user_signal=int(row["blocked_user_signal"])
            if row["blocked_user_signal"] is not None
            else None,
            result_text=str(row["result_text"] or ""),
            error_text=str(row["error_text"] or ""),
            continuation_count=int(row["continuation_count"] or 0),
            schedule_id=int(schedule_value) if schedule_value is not None else None,
        )

    @staticmethod
    def _row_to_mission(row: sqlite3.Row | dict[str, object]) -> AutonomyMission:
        return AutonomyMission(
            id=int(row["id"]),
            chat_id=int(row["chat_id"]),
            source=str(row["source"]),
            root_objective=str(row["root_objective"] or ""),
            success_criteria=str(row["success_criteria"] or ""),
            plan_state=str(row["plan_state"] or "single_pass"),
            plan_json=json.loads(str(row["plan_json"] or "[]")),
            current_stage_index=int(row["current_stage_index"] or 0),
            status=str(row["status"] or "active"),
            started_at=str(row["started_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
            completed_at=str(row["completed_at"]) if row["completed_at"] else None,
            blocked_reason=str(row["blocked_reason"] or ""),
            current_focus=str(row["current_focus"] or ""),
            plan_updated_at=str(row["plan_updated_at"]) if row["plan_updated_at"] else None,
            last_checkpoint_summary=str(row["last_checkpoint_summary"] or ""),
            last_self_check_summary=str(row["last_self_check_summary"] or ""),
        )

    @staticmethod
    def _row_to_schedule(row: sqlite3.Row | dict[str, object]) -> AutonomySchedule:
        recurrence_raw = str(row["recurrence_json"] or "{}")
        try:
            recurrence_json = json.loads(recurrence_raw)
        except json.JSONDecodeError:
            recurrence_json = {}
        if not isinstance(recurrence_json, dict):
            recurrence_json = {}
        return AutonomySchedule(
            id=int(row["id"]),
            chat_id=int(row["chat_id"]),
            title=str(row["title"] or ""),
            prompt_text=str(row["prompt_text"] or ""),
            timezone=str(row["timezone"] or "Europe/Moscow"),
            recurrence_kind=str(row["recurrence_kind"] or ""),
            recurrence_json=recurrence_json,
            next_run_at=str(row["next_run_at"] or ""),
            last_enqueued_at=str(row["last_enqueued_at"]) if row["last_enqueued_at"] else None,
            last_started_at=str(row["last_started_at"]) if row["last_started_at"] else None,
            last_finished_at=str(row["last_finished_at"]) if row["last_finished_at"] else None,
            last_status=str(row["last_status"] or ""),
            delivery_hint=str(row["delivery_hint"] or "plain"),
            active=bool(int(row["active"] or 0)),
            created_at=str(row["created_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
        )

    def close(self) -> None:
        self._conn.close()
