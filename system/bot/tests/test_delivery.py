import tempfile
import unittest
from pathlib import Path

from system.bot.delivery import parse_agent_response, resolve_file_path_for_send


class DeliveryTests(unittest.TestCase):
    def test_parse_agent_response_extracts_send_file_directives(self) -> None:
        parsed = parse_agent_response(
            "\n".join(
                [
                    "Готово.",
                    "[[send-file:daily/2026-02-22.md]]",
                    "[[send-file:daily/2026-02-22.md]]",
                    "[[send-file: topics/plan.md ]]",
                ]
            )
        )
        self.assertEqual(parsed.text, "Готово.")
        self.assertEqual(parsed.file_paths, ["daily/2026-02-22.md", "topics/plan.md"])

    def test_resolve_file_path_for_send_success(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            target = root / "daily" / "note.md"
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text("ok", encoding="utf-8")

            resolved, detail = resolve_file_path_for_send(
                assistant_root=root,
                raw_path="daily/note.md",
                max_size_bytes=1024,
            )
            self.assertEqual(resolved, target.resolve())
            self.assertEqual(detail, "daily/note.md")
