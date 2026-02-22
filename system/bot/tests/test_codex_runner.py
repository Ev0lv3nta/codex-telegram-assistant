import unittest

from system.bot.codex_runner import CodexRunner


class CodexRunnerParseTests(unittest.TestCase):
    def test_parse_json_output_extracts_session_and_message(self) -> None:
        stdout = "\n".join(
            [
                '{"type":"thread.started","thread_id":"11111111-2222-3333-4444-555555555555"}',
                '{"type":"item.completed","item":{"type":"reasoning","text":"..."}}',
                '{"type":"item.completed","item":{"type":"agent_message","text":"Привет"}}',
            ]
        )
        session_id, message = CodexRunner._parse_json_output(stdout)
        self.assertEqual(session_id, "11111111-2222-3333-4444-555555555555")
        self.assertEqual(message, "Привет")

    def test_parse_json_output_uses_last_agent_message(self) -> None:
        stdout = "\n".join(
            [
                '{"type":"thread.started","thread_id":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}',
                '{"type":"item.completed","item":{"type":"agent_message","text":"Черновик"}}',
                '{"type":"item.completed","item":{"type":"agent_message","text":"Финал"}}',
            ]
        )
        session_id, message = CodexRunner._parse_json_output(stdout)
        self.assertEqual(session_id, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        self.assertEqual(message, "Финал")

    def test_failure_text_ignores_json_events(self) -> None:
        stdout = "\n".join(
            [
                '{"type":"thread.started","thread_id":"11111111-2222-3333-4444-555555555555"}',
                '{"type":"item.completed","item":{"type":"reasoning","text":"internal details"}}',
            ]
        )
        stderr = "ERROR codex_core::rollout::list: state db missing rollout path"
        message = CodexRunner._failure_text(stdout, stderr)
        self.assertIn("Не удалось выполнить запрос в Codex CLI.", message)
        self.assertIn("state db missing rollout path", message)
        self.assertNotIn("internal details", message)

    def test_success_text_has_human_fallback(self) -> None:
        message = CodexRunner._success_text(parsed_message="", stdout="")
        self.assertIn("модель не вернула текстовый ответ", message)


if __name__ == "__main__":
    unittest.main()
