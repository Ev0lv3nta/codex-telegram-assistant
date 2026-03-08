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

    def enqueue_task(
        self,
        title: str,
        details: str = "",
        *,
        chat_id: int = 0,
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
                    kind,
                    title,
                    details,
                    priority,
                    status,
                    parent_task_id,
                    source,
                    created_at,
                    scheduled_for
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    chat_id,
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
                    scheduled_for,
                    progress_text,
                    task_id,
                ),
            )

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

    @staticmethod
    def _row_to_task(row: sqlite3.Row | dict[str, object]) -> AutonomyTask:
        return AutonomyTask(
            id=int(row["id"]),
            chat_id=int(row["chat_id"]),
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
        )

    def close(self) -> None:
        self._conn.close()
