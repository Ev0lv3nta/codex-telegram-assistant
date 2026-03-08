import unittest

from system.bot.autonomy_planner import (
    extract_autonomy_continuation,
    extract_self_review,
    parse_wakeup_decision,
)


class AutonomyPlannerTests(unittest.TestCase):
    def test_parse_wakeup_decision_parses_step(self) -> None:
        decision = parse_wakeup_decision(
            "\n".join(
                [
                    "ACTION: STEP",
                    "TITLE: Собрать подборку по теме",
                    "KIND: research",
                    "PRIORITY: 25",
                    "DETAILS:",
                    "Собрать 3-5 сильных источников.",
                    "RESULT:",
                    "Собрал 3 источника и кратко объяснил, почему они полезны.",
                ]
            )
        )
        self.assertEqual(decision.action, "STEP")
        self.assertEqual(decision.title, "Собрать подборку по теме")
        self.assertEqual(decision.kind, "research")
        self.assertEqual(decision.priority, 25)
        self.assertIn("3-5 сильных источников", decision.details)
        self.assertIn("Собрал 3 источника", decision.result_text)

    def test_parse_wakeup_decision_defaults_to_noop(self) -> None:
        decision = parse_wakeup_decision("ACTION: NOOP")
        self.assertEqual(decision.action, "NOOP")
        self.assertEqual(decision.title, "")

    def test_parse_wakeup_decision_parses_complete(self) -> None:
        decision = parse_wakeup_decision(
            "\n".join(
                [
                    "ACTION: COMPLETE",
                    "RESULT:",
                    "Эта задача уже закрыта и может быть снята с активного списка.",
                ]
            )
        )
        self.assertEqual(decision.action, "COMPLETE")
        self.assertIn("снята с активного списка", decision.result_text)

    def test_extract_autonomy_continuation_strips_control_block(self) -> None:
        clean_text, continuation = extract_autonomy_continuation(
            "\n".join(
                [
                    "Сделал первый короткий шаг и зафиксировал промежуточный результат.",
                    "",
                    "[[autonomy-next]]",
                    "ACTION: ENQUEUE",
                    "TITLE: Продолжить исследование",
                    "KIND: research",
                    "PRIORITY: 40",
                    "DELAY_SEC: 900",
                    "DETAILS:",
                    "Собрать еще 2 источника и сверить выводы.",
                    "[[/autonomy-next]]",
                ]
            )
        )
        self.assertIn("промежуточный результат", clean_text)
        self.assertNotIn("[[autonomy-next]]", clean_text)
        self.assertIsNotNone(continuation)
        assert continuation is not None
        self.assertEqual(continuation.title, "Продолжить исследование")
        self.assertEqual(continuation.kind, "research")
        self.assertEqual(continuation.priority, 40)
        self.assertEqual(continuation.delay_sec, 900)
        self.assertIn("2 источника", continuation.details)

    def test_extract_self_review_strips_internal_block(self) -> None:
        clean_text, review = extract_self_review(
            "\n".join(
                [
                    "Сделал один кодовый шаг.",
                    "",
                    "[[self-review]]",
                    "CHANGE: Добавил owner-facing pulse.",
                    "WHY: Чтобы владелец видел состояние автономности.",
                    "RISK: Можно случайно будить контур лишний раз.",
                    "CHECK: Прогнать unit-тесты и проверить live refresh.",
                    "[[/self-review]]",
                ]
            )
        )
        self.assertEqual(clean_text, "Сделал один кодовый шаг.")
        self.assertIsNotNone(review)
        assert review is not None
        self.assertIn("owner-facing pulse", review.change)
        self.assertIn("владелец видел", review.why)
        self.assertIn("будить контур", review.risk)
        self.assertIn("unit-тесты", review.check)
