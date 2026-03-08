import unittest

from system.bot.autonomy_planner import (
    extract_autonomy_continuation,
    extract_mission_plan,
    extract_self_review,
    parse_control_decision,
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
                    "MISSION_STATUS: follow_up_later",
                    "WHY_NOT_DONE_NOW: Ещё осталось свести выводы по этим источникам.",
                    "BLOCKER_TYPE: none",
                    "GOAL_CHECK: Это напрямую двигает миссию исследования.",
                    "PROGRESS_DELTA: Теперь есть базовый набор источников.",
                    "DRIFT_RISK: Низкий.",
                    "WHY_NOT_FINISHED_NOW: Остался один логический проход по выводам.",
                    "NEXT_STEP_JUSTIFICATION: Нужен ещё один связный шаг для сводки.",
                ]
            )
        )
        self.assertEqual(decision.action, "STEP")
        self.assertEqual(decision.title, "Собрать подборку по теме")
        self.assertEqual(decision.kind, "research")
        self.assertEqual(decision.priority, 25)
        self.assertIn("3-5 сильных источников", decision.details)
        self.assertIn("Собрал 3 источника", decision.result_text)
        self.assertEqual(decision.mission_status, "follow_up_later")
        self.assertEqual(decision.blocker_type, "none")
        self.assertIn("исследования", decision.goal_check)
        self.assertIn("базовый набор", decision.progress_delta)

    def test_parse_wakeup_decision_parses_stage_fields(self) -> None:
        decision = parse_wakeup_decision(
            "\n".join(
                [
                    "ACTION: STEP",
                    "TITLE: Собрать раздел 1",
                    "KIND: research",
                    "PRIORITY: 15",
                    "DETAILS:",
                    "Подготовить первый крупный кусок методички.",
                    "RESULT:",
                    "Раздел 1 собран.",
                    "PLAN_MODE: staged",
                    "ROOT_OBJECTIVE: Собрать методичку по теме",
                    "SUCCESS_CRITERIA: Готов документ с проверенными разделами.",
                    "CURRENT_STAGE: Сбор первого раздела",
                    "NEXT_STAGE: Сбор второго раздела",
                    "MISSION_STATUS: follow_up_later",
                    "STAGE_STATUS: stage_done",
                    "CHECKPOINT_SUMMARY: Первый раздел готов.",
                    "WHY_NOT_DONE_NOW: Остались ещё этапы.",
                    "BLOCKER_TYPE: none",
                    "GOAL_CHECK: Это закрывает первый этап.",
                    "PROGRESS_DELTA: Есть готовый раздел.",
                    "DRIFT_RISK: Низкий.",
                    "WHY_NOT_FINISHED_NOW: Остались следующие этапы.",
                    "NEXT_STEP_JUSTIFICATION: Нужно перейти к следующему этапу.",
                ]
            )
        )
        self.assertEqual(decision.plan_mode, "staged")
        self.assertEqual(decision.root_objective, "Собрать методичку по теме")
        self.assertEqual(decision.current_stage, "Сбор первого раздела")
        self.assertEqual(decision.next_stage, "Сбор второго раздела")
        self.assertEqual(decision.stage_status, "stage_done")
        self.assertEqual(decision.checkpoint_summary, "Первый раздел готов.")

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

    def test_extract_mission_plan_reads_stage_block(self) -> None:
        clean_text, plan = extract_mission_plan(
            "\n".join(
                [
                    "Сделал planning-pass.",
                    "",
                    "[[mission-plan]]",
                    "### Сбор материалов",
                    "goal: собрать опорные источники",
                    "done_when: есть 5 сильных источников",
                    "status: active",
                    "completion_summary: ",
                    "### Сбор черновика",
                    "goal: написать основной текст",
                    "done_when: готов черновик методички",
                    "status: pending",
                    "completion_summary: ",
                    "[[/mission-plan]]",
                ]
            )
        )
        self.assertEqual(clean_text, "Сделал planning-pass.")
        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual(len(plan.stages), 2)
        self.assertEqual(plan.stages[0].title, "Сбор материалов")
        self.assertEqual(plan.stages[0].status, "active")
        self.assertEqual(plan.stages[1].done_when, "готов черновик методички")

    def test_parse_control_decision_reads_verdict(self) -> None:
        decision = parse_control_decision(
            "\n".join(
                [
                    "VERDICT: REJECT_AS_MICROSTEP",
                    "REASON: Этот хвост локальный и его лучше дожать сейчас.",
                ]
            )
        )
        self.assertEqual(decision.verdict, "REJECT_AS_MICROSTEP")
        self.assertIn("дожать сейчас", decision.reason)

    def test_parse_control_decision_reads_force_stage_done(self) -> None:
        decision = parse_control_decision(
            "\n".join(
                [
                    "VERDICT: FORCE_STAGE_DONE",
                    "REASON: Этап уже фактически закрыт, хвост слишком мелкий.",
                ]
            )
        )
        self.assertEqual(decision.verdict, "FORCE_STAGE_DONE")
        self.assertIn("Этап уже фактически закрыт", decision.reason)
