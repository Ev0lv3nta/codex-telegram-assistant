import tempfile
import unittest
from pathlib import Path

from system.bot.autonomy_requests import (
    AUTONOMY_REQUESTS_TEMPLATE,
    ensure_autonomy_requests_scaffold,
    read_active_autonomy_request_summaries,
)


class AutonomyRequestsTests(unittest.TestCase):
    def test_ensure_scaffold_creates_requests_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            created = ensure_autonomy_requests_scaffold(root)
            self.assertIsNotNone(created)
            assert created is not None
            self.assertEqual(created.read_text(encoding="utf-8"), AUTONOMY_REQUESTS_TEMPLATE)

    def test_read_active_request_summaries_ignores_non_active_tail(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            ensure_autonomy_requests_scaffold(root)
            target = root / "system" / "tasks" / "autonomy_requests.md"
            target.write_text(
                """# Автономные поручения

## Активные

### Методичка по юнит-экономике
- due: 2026-03-07 23:00 MSK
- deliverable: markdown-методичка
- details: собрать базовую структуру и ключевые метрики

### Сводка по OpenAI
- due: 2026-03-07 20:00 MSK
- deliverable: краткая сводка с ссылками

## Заметки

Тут может лежать какой-то посторонний текст, который не должен считаться активным поручением.
""",
                encoding="utf-8",
            )

            lines = read_active_autonomy_request_summaries(root, limit=5)
            self.assertEqual(len(lines), 2)
            self.assertIn("Методичка по юнит-экономике", lines[0])
            self.assertIn("Сводка по OpenAI", lines[1])
            self.assertNotIn("посторонний текст", " ".join(lines))

    def test_read_active_request_summaries_ignores_template_inside_html_comment(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            ensure_autonomy_requests_scaffold(root)

            lines = read_active_autonomy_request_summaries(root, limit=5)

            self.assertEqual(lines, [])


if __name__ == "__main__":
    unittest.main()
