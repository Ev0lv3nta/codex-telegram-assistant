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
class Task:
    id: int
    chat_id: int
    user_id: int
    username: str
    text: str
    attachments: list[str]
    created_at: str


@dataclass
class StoredTask:
    id: int
    chat_id: int
    user_id: int
    username: str
    text: str
    attachments: list[str]
    status: str
    created_at: str
    started_at: str | None
    finished_at: str | None
    result_text: str
    error_text: str


class QueueStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            str(self._db_path), check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        with self._conn:
            self._conn.execute(
                self._tasks_schema_sql("tasks")
            )
        self._migrate_legacy_tasks_schema_if_needed()
        with self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )

    def _read_task_columns(self) -> set[str]:
        rows = self._conn.execute("PRAGMA table_info(tasks)").fetchall()
        return {str(row["name"]) for row in rows}

    @staticmethod
    def _tasks_schema_sql(table_name: str) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                username TEXT NOT NULL,
                text TEXT NOT NULL,
                attachments_json TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT,
                result_text TEXT,
                error_text TEXT
            )
        """

    def _migrate_legacy_tasks_schema_if_needed(self) -> None:
        columns = self._read_task_columns()
        if "mode" not in columns and "inbox_path" not in columns:
            return

        with self._conn:
            self._conn.execute("DROP TABLE IF EXISTS tasks_v2")
            self._conn.execute(self._tasks_schema_sql("tasks_v2"))
            self._conn.execute(
                """
                INSERT INTO tasks_v2 (
                    id,
                    chat_id,
                    user_id,
                    username,
                    text,
                    attachments_json,
                    status,
                    created_at,
                    started_at,
                    finished_at,
                    result_text,
                    error_text
                )
                SELECT
                    id,
                    chat_id,
                    user_id,
                    username,
                    text,
                    attachments_json,
                    status,
                    created_at,
                    started_at,
                    finished_at,
                    result_text,
                    error_text
                FROM tasks
                """
            )
            self._conn.execute("DROP TABLE tasks")
            self._conn.execute("ALTER TABLE tasks_v2 RENAME TO tasks")
            self._conn.execute("DELETE FROM sqlite_sequence WHERE name = 'tasks'")
            self._conn.execute(
                """
                INSERT INTO sqlite_sequence(name, seq)
                SELECT 'tasks', COALESCE(MAX(id), 0) FROM tasks
                """
            )

    def enqueue_task(
        self,
        chat_id: int,
        user_id: int,
        username: str,
        text: str,
        attachments: list[str],
    ) -> int:
        with self._lock, self._conn:
            self._set_meta_unlocked("last_active_chat_id", str(chat_id))
            next_signal = self.get_user_signal(chat_id) + 1
            self._set_meta_unlocked(f"user_signal:{chat_id}", str(next_signal))
            cursor = self._conn.execute(
                """
                INSERT INTO tasks (
                    chat_id,
                    user_id,
                    username,
                    text,
                    attachments_json,
                    status,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    chat_id,
                    user_id,
                    username,
                    text,
                    json.dumps(attachments, ensure_ascii=False),
                    "pending",
                    _utc_now(),
                ),
            )
            return int(cursor.lastrowid)

    def note_chat_activity(self, chat_id: int) -> None:
        with self._lock, self._conn:
            self._set_meta_unlocked("last_active_chat_id", str(chat_id))

    def get_last_active_chat_id(self) -> int | None:
        value = self.get_meta("last_active_chat_id", "").strip()
        if not value:
            return None
        try:
            return int(value)
        except ValueError:
            return None

    def claim_next_task(self) -> Task | None:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            row = self._conn.execute(
                """
                SELECT * FROM tasks
                WHERE status = 'pending'
                ORDER BY id ASC
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                self._conn.execute("COMMIT")
                return None
            self._conn.execute(
                "UPDATE tasks SET status = 'running', started_at = ? WHERE id = ?",
                (_utc_now(), row["id"]),
            )
            self._conn.execute("COMMIT")
            return Task(
                id=int(row["id"]),
                chat_id=int(row["chat_id"]),
                user_id=int(row["user_id"]),
                username=row["username"],
                text=row["text"],
                attachments=json.loads(row["attachments_json"]),
                created_at=row["created_at"],
            )

    def complete_task(self, task_id: int, result_text: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE tasks
                SET status = 'done', finished_at = ?, result_text = ?, error_text = NULL
                WHERE id = ?
                """,
                (_utc_now(), result_text, task_id),
            )

    def fail_task(self, task_id: int, error_text: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE tasks
                SET status = 'failed', finished_at = ?, error_text = ?
                WHERE id = ?
                """,
                (_utc_now(), error_text, task_id),
            )

    def get_meta(self, key: str, default: str = "") -> str:
        row = self._conn.execute(
            "SELECT value FROM meta WHERE key = ?", (key,)
        ).fetchone()
        if row is None:
            return default
        return str(row["value"])

    def set_meta(self, key: str, value: str) -> None:
        with self._lock, self._conn:
            self._set_meta_unlocked(key, value)

    def _set_meta_unlocked(self, key: str, value: str) -> None:
        self._conn.execute(
            """
            INSERT INTO meta(key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )

    def get_chat_session_id(self, chat_id: int) -> str:
        return self.get_meta(f"chat_session:{chat_id}", "")

    def set_chat_session_id(self, chat_id: int, session_id: str) -> None:
        self.set_meta(f"chat_session:{chat_id}", session_id)

    def clear_chat_session_id(self, chat_id: int) -> None:
        with self._lock, self._conn:
            self._conn.execute("DELETE FROM meta WHERE key = ?", (f"chat_session:{chat_id}",))

    def get_user_signal(self, chat_id: int) -> int:
        raw = self.get_meta(f"user_signal:{chat_id}", "0").strip()
        try:
            return int(raw)
        except ValueError:
            return 0

    def pending_user_tasks(self, chat_id: int) -> int:
        row = self._conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM tasks
            WHERE chat_id = ?
              AND status IN ('pending', 'running')
            """,
            (chat_id,),
        ).fetchone()
        if row is None:
            return 0
        return int(row["cnt"])

    def try_acquire_session_lease(self, chat_id: int, owner: str, ttl_sec: int) -> bool:
        owner_key = f"session_owner:{chat_id}"
        until_key = f"session_owner_until:{chat_id}"
        now = datetime.now(timezone.utc)
        lease_until = (now + timedelta(seconds=ttl_sec)).isoformat()

        with self._lock, self._conn:
            current_owner = self.get_meta(owner_key, "")
            current_until_raw = self.get_meta(until_key, "")
            current_until = _parse_dt(current_until_raw) if current_until_raw else None
            expired = current_until is None or current_until <= now

            if current_owner and current_owner != owner and not expired:
                return False

            self._set_meta_unlocked(owner_key, owner)
            self._set_meta_unlocked(until_key, lease_until)
            return True

    def release_session_lease(self, chat_id: int, owner: str) -> None:
        owner_key = f"session_owner:{chat_id}"
        until_key = f"session_owner_until:{chat_id}"
        with self._lock, self._conn:
            current_owner = self.get_meta(owner_key, "")
            if current_owner and current_owner != owner:
                return
            self._conn.execute("DELETE FROM meta WHERE key IN (?, ?)", (owner_key, until_key))

    def get_session_owner(self, chat_id: int) -> str:
        owner_key = f"session_owner:{chat_id}"
        until_key = f"session_owner_until:{chat_id}"
        current_owner = self.get_meta(owner_key, "")
        current_until_raw = self.get_meta(until_key, "")
        if not current_owner or not current_until_raw:
            return ""
        current_until = _parse_dt(current_until_raw)
        if current_until is None or current_until <= datetime.now(timezone.utc):
            return ""
        return current_owner

    def list_chat_session_ids(self) -> set[str]:
        rows = self._conn.execute(
            "SELECT value FROM meta WHERE key LIKE 'chat_session:%'"
        ).fetchall()
        result: set[str] = set()
        for row in rows:
            value = str(row["value"] or "").strip()
            if value:
                result.add(value)
        return result

    def counts(self) -> dict[str, int]:
        rows = self._conn.execute(
            """
            SELECT status, COUNT(*) AS cnt
            FROM tasks
            GROUP BY status
            """
        ).fetchall()
        result = {"pending": 0, "running": 0, "done": 0, "failed": 0}
        for row in rows:
            result[str(row["status"])] = int(row["cnt"])
        return result

    def list_tasks(
        self,
        *,
        chat_id: int | None = None,
        statuses: set[str] | None = None,
        limit: int = 20,
        order_by: str = "recent",
    ) -> list[StoredTask]:
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
        order_sql = "id DESC" if order_by == "recent" else "id ASC"
        params.append(safe_limit)
        rows = self._conn.execute(
            f"""
            SELECT * FROM tasks
            {where_sql}
            ORDER BY {order_sql}
            LIMIT ?
            """,
            params,
        ).fetchall()
        return [
            StoredTask(
                id=int(row["id"]),
                chat_id=int(row["chat_id"]),
                user_id=int(row["user_id"]),
                username=str(row["username"]),
                text=str(row["text"]),
                attachments=json.loads(row["attachments_json"]),
                status=str(row["status"]),
                created_at=str(row["created_at"]),
                started_at=str(row["started_at"]) if row["started_at"] is not None else None,
                finished_at=str(row["finished_at"]) if row["finished_at"] is not None else None,
                result_text=str(row["result_text"] or ""),
                error_text=str(row["error_text"] or ""),
            )
            for row in rows
        ]

    def close(self) -> None:
        self._conn.close()
