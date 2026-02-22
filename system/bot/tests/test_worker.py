import tempfile
import unittest
from pathlib import Path

from system.bot.worker import _parse_agent_response, _resolve_file_path_for_send


class WorkerHelpersTests(unittest.TestCase):
    def test_parse_agent_response_extracts_send_file_directives(self) -> None:
        parsed = _parse_agent_response(
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

    def test_parse_agent_response_keeps_regular_text(self) -> None:
        parsed = _parse_agent_response("Просто текст без директив.")
        self.assertEqual(parsed.text, "Просто текст без директив.")
        self.assertEqual(parsed.file_paths, [])

    def test_resolve_file_path_for_send_success(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            target = root / "daily" / "note.md"
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text("ok", encoding="utf-8")

            resolved, detail = _resolve_file_path_for_send(
                assistant_root=root,
                raw_path="daily/note.md",
                max_size_bytes=1024,
            )
            self.assertEqual(resolved, target.resolve())
            self.assertEqual(detail, "daily/note.md")

    def test_resolve_file_path_for_send_rejects_outside_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            outside = Path(td).parent / "outside.md"
            outside.write_text("x", encoding="utf-8")
            try:
                resolved, detail = _resolve_file_path_for_send(
                    assistant_root=root,
                    raw_path=str(outside),
                    max_size_bytes=1024,
                )
                self.assertIsNone(resolved)
                self.assertIn("вне рабочей директории", detail)
            finally:
                outside.unlink(missing_ok=True)

    def test_resolve_file_path_for_send_rejects_large_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            target = root / "daily" / "big.md"
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text("a" * 10, encoding="utf-8")

            resolved, detail = _resolve_file_path_for_send(
                assistant_root=root,
                raw_path="daily/big.md",
                max_size_bytes=5,
            )
            self.assertIsNone(resolved)
            self.assertIn("слишком большой", detail)


if __name__ == "__main__":
    unittest.main()
