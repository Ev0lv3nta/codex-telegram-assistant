import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from system.bot.autonomy_journal import (
    AutonomyJournalEntry,
    append_autonomy_journal_entry,
    journal_rel_path_for_day,
    read_recent_autonomy_journal_entries,
)


class AutonomyJournalTests(unittest.TestCase):
    def test_append_autonomy_journal_entry_creates_daily_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            now = datetime(2026, 3, 7, 14, 5, tzinfo=ZoneInfo("Europe/Moscow"))

            target = append_autonomy_journal_entry(
                root,
                AutonomyJournalEntry(
                    status="completed",
                    title="Проверить heartbeat",
                    summary="Planner сработал и задача выполнилась.",
                    task_id=7,
                ),
                now=now,
            )

            self.assertEqual(
                target.relative_to(root).as_posix(),
                "system/tasks/autonomy_journal/2026-03-07.md",
            )
            text = target.read_text(encoding="utf-8")
            self.assertIn("# Автономность за 2026-03-07", text)
            self.assertIn("## 14:05 · completed", text)
            self.assertIn("Проверить heartbeat", text)
            self.assertIn("Planner сработал и задача выполнилась.", text)

    def test_journal_rel_path_for_day_uses_moscow_date(self) -> None:
        dt = datetime(2026, 3, 7, 0, 1, tzinfo=ZoneInfo("Europe/Moscow"))
        self.assertEqual(
            journal_rel_path_for_day(dt),
            "system/tasks/autonomy_journal/2026-03-07.md",
        )

    def test_read_recent_autonomy_journal_entries_returns_latest_blocks_first(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            now = datetime(2026, 3, 7, 14, 5, tzinfo=ZoneInfo("Europe/Moscow"))

            append_autonomy_journal_entry(
                root,
                AutonomyJournalEntry(
                    status="planned",
                    title="Первый шаг",
                    summary="Поставлена первая задача.",
                    task_id=1,
                ),
                now=now.replace(hour=14, minute=5),
            )
            append_autonomy_journal_entry(
                root,
                AutonomyJournalEntry(
                    status="completed",
                    title="Второй шаг",
                    summary="Задача завершилась полезным результатом.",
                    task_id=2,
                ),
                now=now.replace(hour=14, minute=35),
            )

            entries = read_recent_autonomy_journal_entries(root, limit=2, day=now)
            self.assertEqual(len(entries), 2)
            self.assertIn("Второй шаг", entries[0])
            self.assertIn("Первый шаг", entries[1])


if __name__ == "__main__":
    unittest.main()
