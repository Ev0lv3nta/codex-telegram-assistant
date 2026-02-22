import unittest

from system.bot.classifier import Mode
from system.bot.prompts import build_prompt


class PromptTests(unittest.TestCase):
    def test_prompt_is_plain_user_text_when_no_attachments(self) -> None:
        prompt = build_prompt(
            mode=Mode.RESEARCH,
            user_text="Поищи лучшие источники",
            inbox_path="00_inbox/test.md",
            attachments=[],
        )
        self.assertEqual(prompt, "Поищи лучшие источники")

    def test_prompt_has_bootstrap_prefix_for_new_session(self) -> None:
        prompt = build_prompt(
            mode=Mode.AUTO,
            user_text="Привет",
            inbox_path="",
            attachments=[],
            include_bootstrap=True,
        )
        self.assertIn("AGENTS.md", prompt)
        self.assertIn("Привет", prompt)

    def test_prompt_includes_attachment_paths(self) -> None:
        prompt = build_prompt(
            mode=Mode.ANSWER,
            user_text="Где контакт Ивана?",
            inbox_path="00_inbox/test.md",
            attachments=["88_files/file.pdf"],
        )
        self.assertIn("Где контакт Ивана?", prompt)
        self.assertIn("88_files/file.pdf", prompt)

    def test_prompt_for_attachments_without_text(self) -> None:
        prompt = build_prompt(
            mode=Mode.AUTO,
            user_text="",
            inbox_path="",
            attachments=["89_images/pic.jpg"],
        )
        self.assertIn("без текста", prompt)
        self.assertIn("89_images/pic.jpg", prompt)


if __name__ == "__main__":
    unittest.main()
