import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from system.bot.self_restart import (
    consume_restart_notification_target,
    mark_restart_observed,
    read_restart_state,
    request_service_restart,
)


class SelfRestartTests(unittest.TestCase):
    def test_request_service_restart_records_observed_state(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "state.db"
            responses = [
                ("ActiveState=active\nMainPID=10\nActiveEnterTimestampMonotonic=100\n", 0),
                ("", 0),
                ("ActiveState=active\nMainPID=11\nActiveEnterTimestampMonotonic=200\n", 0),
            ]

            def _fake_run(_cmd, capture_output, text, check):  # type: ignore[no-untyped-def]
                stdout, returncode = responses.pop(0)
                class _Completed:
                    stderr = ""
                result = _Completed()
                result.stdout = stdout
                result.returncode = returncode
                return result

            with patch("system.bot.self_restart.subprocess.run", side_effect=_fake_run):
                with patch("system.bot.self_restart.time.sleep"):
                    ok, observed_at = request_service_restart(db_path, "demo.service")

            self.assertTrue(ok)
            self.assertTrue(observed_at)
            state = read_restart_state(db_path, "demo.service")
            self.assertEqual(state["state"], "observed")
            self.assertTrue(state["requested_at"])
            self.assertEqual(state["observed_at"], observed_at)
            self.assertEqual(state["notify_chat_id"], "")
            self.assertEqual(state["notify_pending"], "")

    def test_mark_restart_observed_transitions_requested_state(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "state.db"
            with patch("system.bot.self_restart.subprocess.run") as run_mock:
                run_mock.return_value.returncode = 0
                run_mock.return_value.stdout = ""
                run_mock.return_value.stderr = ""
                request_service_restart(
                    db_path,
                    "demo.service",
                    wait_timeout_sec=0,
                    poll_interval_sec=0,
                )

            changed = mark_restart_observed(db_path, "demo.service", observed_at="2026-03-07T18:00:00+00:00")

            self.assertTrue(changed)
            state = read_restart_state(db_path, "demo.service")
            self.assertEqual(state["state"], "observed")
            self.assertEqual(state["observed_at"], "2026-03-07T18:00:00+00:00")
            self.assertEqual(state["detail"], "")

    def test_consume_restart_notification_target_returns_last_active_chat_once(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "state.db"
            import sqlite3

            conn = sqlite3.connect(str(db_path))
            with conn:
                conn.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
                conn.execute(
                    "INSERT INTO meta(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    ("last_active_chat_id", "5398760601"),
                )
            conn.close()

            with patch("system.bot.self_restart.subprocess.run") as run_mock:
                run_mock.return_value.returncode = 0
                run_mock.return_value.stdout = ""
                run_mock.return_value.stderr = ""
                request_service_restart(
                    db_path,
                    "demo.service",
                    wait_timeout_sec=0,
                    poll_interval_sec=0,
                )

            mark_restart_observed(db_path, "demo.service", observed_at="2026-03-07T18:00:00+00:00")
            self.assertEqual(consume_restart_notification_target(db_path, "demo.service"), 5398760601)
            self.assertIsNone(consume_restart_notification_target(db_path, "demo.service"))


if __name__ == "__main__":
    unittest.main()
