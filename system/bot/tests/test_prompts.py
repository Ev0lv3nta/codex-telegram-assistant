import unittest

from system.bot.classifier import Mode
from system.bot.prompts import build_prompt


class PromptTests(unittest.TestCase):
    def test_research_prompt_uses_skill(self) -> None:
        prompt = build_prompt(
            mode=Mode.RESEARCH,
            user_text="Поищи лучшие источники",
            inbox_path="00_inbox/test.md",
            attachments=[],
        )
        self.assertIn("assistant-research", prompt)
        self.assertIn("00_inbox/test.md", prompt)

    def test_answer_prompt_has_sources_requirement(self) -> None:
        prompt = build_prompt(
            mode=Mode.ANSWER,
            user_text="Где контакт Ивана?",
            inbox_path="00_inbox/test.md",
            attachments=["88_files/file.pdf"],
        )
        self.assertIn("assistant-answer", prompt)
        self.assertIn("пути файлов", prompt)


if __name__ == "__main__":
    unittest.main()

