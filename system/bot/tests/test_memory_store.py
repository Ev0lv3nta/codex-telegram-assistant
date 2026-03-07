import tempfile
import unittest
from pathlib import Path

from system.bot.memory_store import (
    MEMORY_FILE_CONTENT,
    build_memory_prompt_note,
    ensure_memory_scaffold,
)


class MemoryStoreTests(unittest.TestCase):
    def test_ensure_memory_scaffold_creates_expected_files(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            created = ensure_memory_scaffold(root)

            self.assertEqual(len(created), len(MEMORY_FILE_CONTENT))
            for filename, expected_content in MEMORY_FILE_CONTENT.items():
                target = root / "memory" / filename
                self.assertTrue(target.exists())
                self.assertEqual(target.read_text(encoding="utf-8"), expected_content)

    def test_ensure_memory_scaffold_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            ensure_memory_scaffold(root)
            created = ensure_memory_scaffold(root)
            self.assertEqual(created, [])

    def test_build_memory_prompt_note_references_core_files(self) -> None:
        note = build_memory_prompt_note()
        self.assertIn("memory/about_user.md", note)
        self.assertIn("memory/about_self.md", note)
        self.assertIn("system/tasks/autonomy_requests.md", note)
        self.assertIn("topics/autonomy-companion-plan.md", note)

if __name__ == "__main__":
    unittest.main()
