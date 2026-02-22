import unittest

from system.bot.classifier import Mode
from system.bot.prompts import build_prompt


class PromptTests(unittest.TestCase):
    def test_prompt_contains_inbox_and_user_text(self) -> None:
        prompt = build_prompt(
            mode=Mode.RESEARCH,
            user_text="Поищи лучшие источники",
            inbox_path="00_inbox/test.md",
            attachments=[],
        )
        self.assertIn("00_inbox/test.md", prompt)
        self.assertIn("Поищи лучшие источники", prompt)

    def test_prompt_has_no_internal_kitchen_rule(self) -> None:
        prompt = build_prompt(
            mode=Mode.ANSWER,
            user_text="Где контакт Ивана?",
            inbox_path="00_inbox/test.md",
            attachments=["88_files/file.pdf"],
        )
        self.assertIn("Не показывай внутреннюю кухню", prompt)
        self.assertIn("Изменено:", prompt)


if __name__ == "__main__":
    unittest.main()
