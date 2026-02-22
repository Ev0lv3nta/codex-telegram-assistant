import tempfile
import time
import unittest
from pathlib import Path

from system.bot.session_gc import _extract_session_id, gc_sessions


class SessionGcTests(unittest.TestCase):
    def test_extract_session_id(self) -> None:
        p = Path("rollout-2026-02-22T00-00-00-019c82dd-11af-78a2-95bc-d1465656100b.jsonl")
        self.assertEqual(
            _extract_session_id(p),
            "019c82dd-11af-78a2-95bc-d1465656100b",
        )

    def test_gc_deletes_old_unkept(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            sessions = root / "sessions"
            sessions.mkdir(parents=True)

            keep_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
            delete_id = "11111111-2222-3333-4444-555555555555"

            old = time.time() - 10 * 86400
            new = time.time()

            kept = sessions / f"rollout-x-{keep_id}.jsonl"
            doomed = sessions / f"rollout-x-{delete_id}.jsonl"
            recent = sessions / "rollout-x-ffffffff-1111-2222-3333-444444444444.jsonl"

            kept.write_text("k", encoding="utf-8")
            doomed.write_text("d", encoding="utf-8")
            recent.write_text("r", encoding="utf-8")

            Path(kept).touch()
            Path(doomed).touch()
            Path(recent).touch()
            # Set mtimes
            import os

            os.utime(kept, (old, old))
            os.utime(doomed, (old, old))
            os.utime(recent, (new, new))

            res = gc_sessions(sessions_dir=sessions, keep_session_ids={keep_id}, older_than_days=7)
            self.assertTrue(kept.exists())
            self.assertFalse(doomed.exists())
            self.assertTrue(recent.exists())
            self.assertEqual(res.deleted_files, 1)
            self.assertEqual(res.kept_files, 1)


if __name__ == "__main__":
    unittest.main()

