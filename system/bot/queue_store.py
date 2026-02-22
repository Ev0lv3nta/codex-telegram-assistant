from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
import sqlite3
import threading
from datetime import datetime, timezone


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class Task:
    id: int
    chat_id: int
    user_id: int
    username: str
    text: str
    attachments: list[str]
    created_at: str


class QueueStore:
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

    def close(self) -> None:
        self._conn.close()
