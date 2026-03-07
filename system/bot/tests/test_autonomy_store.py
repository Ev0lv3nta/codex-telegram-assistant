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


if __name__ == "__main__":
    unittest.main()
