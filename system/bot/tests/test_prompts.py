import unittest

from system.bot.prompts import build_prompt


class PromptTests(unittest.TestCase):
    def test_prompt_is_plain_user_text_when_no_attachments(self) -> None:
        prompt = build_prompt(
            user_text="Поищи лучшие источники",
            attachments=[],
        )
        self.assertEqual(prompt, "Поищи лучшие источники")

    def test_prompt_has_bootstrap_prefix_for_new_session(self) -> None:
        prompt = build_prompt(
            user_text="Привет",
            attachments=[],
            include_bootstrap=True,
        )
        self.assertIn("AGENTS.md", prompt)
        self.assertIn("Привет", prompt)

    def test_prompt_includes_attachment_paths(self) -> None:
        prompt = build_prompt(
            user_text="Где контакт Ивана?",
            attachments=["88_files/file.pdf"],
        )
        self.assertIn("Где контакт Ивана?", prompt)
        self.assertIn("88_files/file.pdf", prompt)

    def test_prompt_for_attachments_without_text(self) -> None:
        prompt = build_prompt(
            user_text="",
            attachments=["89_images/pic.jpg"],
        )
        self.assertIn("без текста", prompt)
        self.assertIn("89_images/pic.jpg", prompt)


if __name__ == "__main__":
    unittest.main()
