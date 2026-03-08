from __future__ import annotations

import argparse
import sqlite3
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_SERVICE_NAME = "personal-assistant-bot.service"
DEFAULT_STATE_DB = Path("/root/personal-assistant/system/tasks/bot_state.db")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _meta_key(service_name: str, suffix: str) -> str:
    return f"service_restart:{service_name}:{suffix}"


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    with conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
    return conn


def _set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    with conn:
        conn.execute(
            """
            INSERT INTO meta(key, value)
            VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (key, value),
        )


def _get_meta(conn: sqlite3.Connection, key: str, default: str = "") -> str:
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    if row is None:
        return default
    return str(row[0] or "")


def read_restart_state(db_path: Path, service_name: str = DEFAULT_SERVICE_NAME) -> dict[str, str]:
    conn = _connect(db_path)
    try:
        return {
            "service": service_name,
            "state": _get_meta(conn, _meta_key(service_name, "state")),
            "requested_at": _get_meta(conn, _meta_key(service_name, "requested_at")),
            "observed_at": _get_meta(conn, _meta_key(service_name, "observed_at")),
            "detail": _get_meta(conn, _meta_key(service_name, "detail")),
            "notify_chat_id": _get_meta(conn, _meta_key(service_name, "notify_chat_id")),
            "notify_pending": _get_meta(conn, _meta_key(service_name, "notify_pending")),
        }
    finally:
        conn.close()


def _read_service_runtime(service_name: str) -> dict[str, str]:
    completed = subprocess.run(
        [
            "/bin/systemctl",
            "show",
            service_name,
            "--property=ActiveState",
            "--property=MainPID",
            "--property=ActiveEnterTimestampMonotonic",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or f"exit={completed.returncode}").strip()
        raise RuntimeError(detail)

    values: dict[str, str] = {}
    for raw_line in (completed.stdout or "").splitlines():
        line = raw_line.strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key] = value
    return values


def mark_restart_requested(
    db_path: Path,
    service_name: str = DEFAULT_SERVICE_NAME,
    *,
    requested_at: str | None = None,
) -> str:
    effective_requested_at = requested_at or _utc_now()
    conn = _connect(db_path)
    try:
        notify_chat_id = _get_meta(conn, "last_active_chat_id")
        _set_meta(conn, _meta_key(service_name, "state"), "requested")
        _set_meta(conn, _meta_key(service_name, "requested_at"), effective_requested_at)
        _set_meta(conn, _meta_key(service_name, "observed_at"), "")
        _set_meta(conn, _meta_key(service_name, "detail"), "")
        _set_meta(conn, _meta_key(service_name, "notify_chat_id"), notify_chat_id)
        _set_meta(conn, _meta_key(service_name, "notify_pending"), "1" if notify_chat_id else "")
        return effective_requested_at
    finally:
        conn.close()


def mark_restart_observed(
    db_path: Path,
    service_name: str = DEFAULT_SERVICE_NAME,
    *,
    observed_at: str | None = None,
) -> bool:
    effective_observed_at = observed_at or _utc_now()
    conn = _connect(db_path)
    try:
        state = _get_meta(conn, _meta_key(service_name, "state"))
        requested_at = _get_meta(conn, _meta_key(service_name, "requested_at"))
        if state != "requested" or not requested_at:
            return False
        _set_meta(conn, _meta_key(service_name, "state"), "observed")
        _set_meta(conn, _meta_key(service_name, "observed_at"), effective_observed_at)
        _set_meta(conn, _meta_key(service_name, "detail"), "")
        return True
    finally:
        conn.close()


def consume_restart_notification_target(
    db_path: Path,
    service_name: str = DEFAULT_SERVICE_NAME,
) -> int | None:
    conn = _connect(db_path)
    try:
        state = _get_meta(conn, _meta_key(service_name, "state"))
        pending = _get_meta(conn, _meta_key(service_name, "notify_pending"))
        raw_chat_id = _get_meta(conn, _meta_key(service_name, "notify_chat_id"))
        if state != "observed" or pending != "1" or not raw_chat_id:
            return None
        try:
            chat_id = int(raw_chat_id)
        except ValueError:
            return None
        _set_meta(conn, _meta_key(service_name, "notify_pending"), "")
        return chat_id
    finally:
        conn.close()


def request_service_restart(
    db_path: Path,
    service_name: str = DEFAULT_SERVICE_NAME,
    *,
    wait_timeout_sec: float = 20.0,
    poll_interval_sec: float = 0.5,
) -> tuple[bool, str]:
    try:
        before = _read_service_runtime(service_name)
    except Exception as exc:
        before = {
            "ActiveState": "",
            "MainPID": "",
            "ActiveEnterTimestampMonotonic": "",
        }
        before_error = str(exc)
    else:
        before_error = ""

    requested_at = mark_restart_requested(db_path, service_name)
    command = ["/bin/systemctl", "--no-block", "restart", service_name]
    try:
        completed = subprocess.run(command, capture_output=True, text=True, check=False)
    except Exception as exc:
        conn = _connect(db_path)
        try:
            _set_meta(conn, _meta_key(service_name, "state"), "failed")
            _set_meta(conn, _meta_key(service_name, "detail"), str(exc))
        finally:
            conn.close()
        return False, str(exc)

    if completed.returncode in (0, -15):
        deadline = time.monotonic() + max(0.0, wait_timeout_sec)
        while time.monotonic() <= deadline:
            try:
                current = _read_service_runtime(service_name)
            except Exception as exc:
                last_error = str(exc)
                time.sleep(max(0.0, poll_interval_sec))
                continue

            restarted = (
                current.get("ActiveState") == "active"
                and (
                    current.get("MainPID") != before.get("MainPID")
                    or current.get("ActiveEnterTimestampMonotonic") != before.get("ActiveEnterTimestampMonotonic")
                    or not before.get("MainPID")
                )
            )
            if restarted:
                observed_at = _utc_now()
                mark_restart_observed(db_path, service_name, observed_at=observed_at)
                return True, observed_at
            time.sleep(max(0.0, poll_interval_sec))

        detail = before_error or locals().get("last_error", "") or "restart not observed within timeout"
        conn = _connect(db_path)
        try:
            _set_meta(conn, _meta_key(service_name, "detail"), detail)
        finally:
            conn.close()
        return False, detail

    detail = (completed.stderr or completed.stdout or f"exit={completed.returncode}").strip()
    conn = _connect(db_path)
    try:
        _set_meta(conn, _meta_key(service_name, "state"), "failed")
        _set_meta(conn, _meta_key(service_name, "detail"), detail)
    finally:
        conn.close()
    return False, detail


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Record and manage self-restart requests.")
    parser.add_argument("action", choices=("request", "status"))
    parser.add_argument("--service", default=DEFAULT_SERVICE_NAME)
    parser.add_argument("--db-path", default=str(DEFAULT_STATE_DB))
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    db_path = Path(args.db_path).expanduser().resolve()
    service_name = str(args.service).strip() or DEFAULT_SERVICE_NAME

    if args.action == "request":
        ok, detail = request_service_restart(db_path, service_name)
        if ok:
            print("restart-observed")
            print(f"service={service_name}")
            print(f"observed_at={detail}")
            return 0
        print("restart-failed")
        print(f"service={service_name}")
        print(f"detail={detail}")
        return 1

    state = read_restart_state(db_path, service_name)
    for key in ("service", "state", "requested_at", "observed_at", "detail", "notify_chat_id", "notify_pending"):
        print(f"{key}={state.get(key, '')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
