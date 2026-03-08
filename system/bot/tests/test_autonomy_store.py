import tempfile
import unittest
from pathlib import Path

from system.bot.autonomy_store import AutonomyStore


class AutonomyStoreTests(unittest.TestCase):
    def test_enqueue_and_claim_ready_task(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = AutonomyStore(Path(td) / "bot_state.db")
            try:
                task_id = store.enqueue_task(
                    chat_id=101,
                    title="Проверить идеи",
                    details="Собрать 3 инициативы",
                    kind="research",
                    priority=20,
                    scheduled_for="2026-03-06T10:00:00+00:00",
                )

                task = store.claim_next_ready_task(chat_id=101, now="2026-03-06T10:05:00+00:00")
                self.assertIsNotNone(task)
                assert task is not None
                self.assertEqual(task.id, task_id)
                self.assertEqual(task.chat_id, 101)
                self.assertEqual(task.status, "running")
                self.assertEqual(task.kind, "research")
                self.assertEqual(task.title, "Проверить идеи")
                self.assertIsNone(task.parent_task_id)
            finally:
                store.close()

    def test_claim_respects_schedule_and_priority(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = AutonomyStore(Path(td) / "bot_state.db")
            try:
                store.enqueue_task(
                    chat_id=101,
                    title="Поздняя задача",
                    priority=5,
                    scheduled_for="2026-03-06T12:00:00+00:00",
                )
                first_id = store.enqueue_task(
                    chat_id=101,
                    title="Готовая задача",
                    priority=50,
                    scheduled_for="2026-03-06T09:00:00+00:00",
                )
                second_id = store.enqueue_task(
                    chat_id=101,
                    title="Более приоритетная готовая задача",
                    priority=10,
                    scheduled_for="2026-03-06T09:00:00+00:00",
                )

                first = store.claim_next_ready_task(chat_id=101, now="2026-03-06T09:30:00+00:00")
                self.assertIsNotNone(first)
                assert first is not None
                self.assertEqual(first.id, second_id)

                second = store.claim_next_ready_task(chat_id=101, now="2026-03-06T09:31:00+00:00")
                self.assertIsNotNone(second)
                assert second is not None
                self.assertEqual(second.id, first_id)
            finally:
                store.close()

    def test_complete_fail_and_requeue(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = AutonomyStore(Path(td) / "bot_state.db")
            try:
                task_id = store.enqueue_task(chat_id=101, title="Черновик")
                claimed = store.claim_next_ready_task(chat_id=101)
                self.assertIsNotNone(claimed)

                store.fail_task(task_id, "network issue")
                failed = store.list_tasks(chat_id=101, statuses={"failed"}, limit=5)
                self.assertEqual(len(failed), 1)
                self.assertEqual(failed[0].error_text, "network issue")

                store.requeue_task(task_id, scheduled_for="2026-03-06T12:00:00+00:00", priority=15)
                pending = store.list_tasks(chat_id=101, statuses={"pending"}, limit=5)
                self.assertEqual(len(pending), 1)
                self.assertEqual(pending[0].priority, 15)
                self.assertEqual(pending[0].scheduled_for, "2026-03-06T12:00:00+00:00")

                store.claim_next_ready_task(chat_id=101, now="2026-03-06T12:01:00+00:00")
                store.complete_task(task_id, "готово")
                done = store.list_tasks(chat_id=101, statuses={"done"}, limit=5)
                self.assertEqual(len(done), 1)
                self.assertEqual(done[0].result_text, "готово")
            finally:
                store.close()

    def test_heartbeat_meta_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = AutonomyStore(Path(td) / "bot_state.db")
            try:
                timestamp = store.mark_heartbeat("noop", "2026-03-06T11:00:00+00:00")
                self.assertEqual(timestamp, "2026-03-06T11:00:00+00:00")
                self.assertEqual(store.get_heartbeat("noop"), "2026-03-06T11:00:00+00:00")
            finally:
                store.close()

    def test_notify_due_and_mark_notify_sent(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = AutonomyStore(Path(td) / "bot_state.db")
            try:
                self.assertTrue(store.notify_due(101, cooldown_sec=3600, now="2026-03-06T10:00:00+00:00"))
                store.mark_notify_sent(101, "2026-03-06T10:00:00+00:00")
                self.assertFalse(store.notify_due(101, cooldown_sec=3600, now="2026-03-06T10:30:00+00:00"))
                self.assertTrue(store.notify_due(101, cooldown_sec=3600, now="2026-03-06T11:30:01+00:00"))
            finally:
                store.close()

    def test_idle_interest_prompt_and_snooze_meta(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = AutonomyStore(Path(td) / "bot_state.db")
            try:
                self.assertTrue(
                    store.idle_interest_prompt_due(
                        101,
                        cooldown_sec=3600,
                        now="2026-03-06T10:00:00+00:00",
                    )
                )
                store.mark_idle_interest_prompt(
                    101,
                    user_signal=7,
                    at="2026-03-06T10:00:00+00:00",
                )
                self.assertEqual(store.get_idle_interest_prompt_signal(101), 7)
                self.assertFalse(
                    store.idle_interest_prompt_due(
                        101,
                        cooldown_sec=3600,
                        now="2026-03-06T10:30:00+00:00",
                    )
                )
                store.mark_idle_snooze_until(101, "2026-03-06T15:00:00+00:00")
                self.assertTrue(store.idle_snoozed(101, now="2026-03-06T12:00:00+00:00"))
                self.assertFalse(store.idle_snoozed(101, now="2026-03-06T16:00:00+00:00"))
                store.clear_idle_snooze(101)
                store.clear_idle_interest_prompt(101)
                self.assertEqual(store.get_idle_interest_prompt_signal(101), 0)
                self.assertFalse(store.idle_snoozed(101, now="2026-03-06T12:00:00+00:00"))
            finally:
                store.close()

    def test_list_tasks_recent_and_parent_chain(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = AutonomyStore(Path(td) / "bot_state.db")
            try:
                parent_id = store.enqueue_task(chat_id=101, title="Первый шаг", priority=50)
                child_id = store.enqueue_task(
                    chat_id=101,
                    title="Второй шаг",
                    priority=10,
                    parent_task_id=parent_id,
                    source="followup",
                )

                tasks = store.list_tasks(chat_id=101, limit=5, order_by="recent")
                self.assertEqual(tasks[0].id, child_id)
                self.assertEqual(tasks[0].parent_task_id, parent_id)
                self.assertEqual(tasks[0].source, "followup")
            finally:
                store.close()

    def test_continue_task_reuses_same_record_for_future_step(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = AutonomyStore(Path(td) / "bot_state.db")
            try:
                task_id = store.enqueue_task(
                    chat_id=101,
                    title="Первый шаг",
                    details="Сделать базовый обзор",
                    kind="research",
                    priority=40,
                )
                claimed = store.claim_next_ready_task(chat_id=101)
                self.assertIsNotNone(claimed)

                store.continue_task(
                    task_id,
                    title="Продолжить обзор",
                    details="Проверить ещё один источник",
                    kind="research",
                    priority=35,
                    scheduled_for="2026-03-06T12:00:00+00:00",
                    progress_text="Уже собран первый вывод.",
                )

                pending = store.list_tasks(chat_id=101, statuses={"pending"}, limit=5)
                self.assertEqual(len(pending), 1)
                self.assertEqual(pending[0].id, task_id)
                self.assertEqual(pending[0].title, "Продолжить обзор")
                self.assertEqual(pending[0].details, "Проверить ещё один источник")
                self.assertEqual(pending[0].priority, 35)
                self.assertEqual(pending[0].scheduled_for, "2026-03-06T12:00:00+00:00")
                self.assertEqual(pending[0].result_text, "Уже собран первый вывод.")
                self.assertEqual(pending[0].continuation_count, 1)
            finally:
                store.close()

    def test_active_mission_roundtrip_keeps_scheduled_for(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = AutonomyStore(Path(td) / "bot_state.db")
            try:
                store.set_active_mission(
                    101,
                    task_id=7,
                    title="Продолжить автономную линию",
                    details="Вернуться к следующему шагу позже",
                    kind="project",
                    source="assistant",
                    phase="scheduled",
                    scheduled_for="2026-03-06T12:00:00+00:00",
                )

                mission = store.get_active_mission(101)

                self.assertIsNotNone(mission)
                assert mission is not None
                self.assertEqual(mission.task_id, 7)
                self.assertEqual(mission.title, "Продолжить автономную линию")
                self.assertEqual(mission.phase, "scheduled")
                self.assertEqual(mission.scheduled_for, "2026-03-06T12:00:00+00:00")
            finally:
                store.close()

    def test_create_and_update_root_mission(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = AutonomyStore(Path(td) / "bot_state.db")
            try:
                mission_id = store.create_mission(
                    chat_id=101,
                    source="owner_request",
                    root_objective="Подготовить исследование по рынку",
                    success_criteria="Дойти до заметного checkpoint без микродробления.",
                    current_focus="Собрать 3 сильных источника",
                )
                mission = store.get_live_mission(101, source="owner_request")
                self.assertIsNotNone(mission)
                assert mission is not None
                self.assertEqual(mission.id, mission_id)
                self.assertEqual(mission.root_objective, "Подготовить исследование по рынку")
                self.assertEqual(mission.current_focus, "Собрать 3 сильных источника")

                store.update_mission(
                    mission_id,
                    current_focus="Свести выводы",
                    last_self_check_summary="goal: рынок | progress: собраны 3 источника",
                )
                updated = store.get_mission(mission_id)
                self.assertIsNotNone(updated)
                assert updated is not None
                self.assertEqual(updated.current_focus, "Свести выводы")
                self.assertIn("собраны 3 источника", updated.last_self_check_summary)

                store.complete_mission(mission_id, current_focus="Сводка готова")
                completed = store.get_mission(mission_id)
                self.assertIsNotNone(completed)
                assert completed is not None
                self.assertEqual(completed.status, "completed")
                self.assertEqual(completed.current_focus, "Сводка готова")
                self.assertIsNotNone(completed.completed_at)
            finally:
                store.close()

    def test_mission_plan_fields_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = AutonomyStore(Path(td) / "bot_state.db")
            try:
                mission_id = store.create_mission(
                    chat_id=101,
                    source="owner_request",
                    root_objective="Собрать методичку",
                    success_criteria="Готов связный документ.",
                    plan_state="staged",
                    plan_json=[
                        {
                            "title": "Сбор материалов",
                            "goal": "Собрать опорные источники",
                            "done_when": "Есть 5 сильных источников",
                            "status": "active",
                            "completion_summary": "",
                        },
                        {
                            "title": "Черновик",
                            "goal": "Написать основной текст",
                            "done_when": "Готов первый черновик",
                            "status": "pending",
                            "completion_summary": "",
                        },
                    ],
                    current_stage_index=0,
                    current_focus="Сбор материалов",
                    plan_updated_at="2026-03-08T10:00:00+00:00",
                    last_checkpoint_summary="План создан.",
                )
                mission = store.get_mission(mission_id)
                self.assertIsNotNone(mission)
                assert mission is not None
                self.assertEqual(mission.plan_state, "staged")
                self.assertEqual(len(mission.plan_json), 2)
                self.assertEqual(mission.plan_json[0]["title"], "Сбор материалов")
                self.assertEqual(mission.current_stage_index, 0)
                self.assertEqual(mission.last_checkpoint_summary, "План создан.")

                store.update_mission(
                    mission_id,
                    current_stage_index=1,
                    plan_json=[
                        {
                            "title": "Сбор материалов",
                            "goal": "Собрать опорные источники",
                            "done_when": "Есть 5 сильных источников",
                            "status": "done",
                            "completion_summary": "Источники собраны.",
                        },
                        {
                            "title": "Черновик",
                            "goal": "Написать основной текст",
                            "done_when": "Готов первый черновик",
                            "status": "active",
                            "completion_summary": "",
                        },
                    ],
                    last_checkpoint_summary="Первый этап закрыт.",
                )
                updated = store.get_mission(mission_id)
                self.assertIsNotNone(updated)
                assert updated is not None
                self.assertEqual(updated.current_stage_index, 1)
                self.assertEqual(updated.plan_json[0]["status"], "done")
                self.assertEqual(updated.last_checkpoint_summary, "Первый этап закрыт.")
            finally:
                store.close()

    def test_task_can_be_linked_to_mission_and_listed_back(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = AutonomyStore(Path(td) / "bot_state.db")
            try:
                mission_id = store.create_mission(
                    chat_id=101,
                    source="initiative",
                    root_objective="Привести pulse в порядок",
                    success_criteria="Сделать owner-facing слой понятнее.",
                )
                task_id = store.enqueue_task(
                    chat_id=101,
                    mission_id=mission_id,
                    title="Упростить pulse",
                    kind="project",
                )
                task = store.get_next_pending_task(101)
                self.assertIsNotNone(task)
                assert task is not None
                self.assertEqual(task.mission_id, mission_id)

                mission_tasks = store.list_mission_tasks(mission_id, limit=5)
                self.assertEqual(len(mission_tasks), 1)
                self.assertEqual(mission_tasks[0].id, task_id)
                self.assertEqual(mission_tasks[0].mission_id, mission_id)
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
