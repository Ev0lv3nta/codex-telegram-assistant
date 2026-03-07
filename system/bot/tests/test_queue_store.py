import tempfile
import unittest
from pathlib import Path

from system.bot.queue_store import QueueStore


class QueueStoreTests(unittest.TestCase):
    def test_enqueue_updates_last_active_chat_and_signal(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = QueueStore(Path(td) / "bot_state.db")
            try:
                self.assertIsNone(store.get_last_active_chat_id())
                self.assertEqual(store.get_user_signal(101), 0)

                store.enqueue_task(
                    chat_id=101,
                    user_id=1,
                    username="tester",
                    text="hello",
                    attachments=[],
                )

                self.assertEqual(store.get_last_active_chat_id(), 101)
                self.assertEqual(store.get_user_signal(101), 1)
                self.assertEqual(store.pending_user_tasks(101), 1)
            finally:
                store.close()

    def test_session_lease_blocks_other_owner_until_release(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = QueueStore(Path(td) / "bot_state.db")
            try:
                acquired = store.try_acquire_session_lease(101, "autonomy", ttl_sec=60)
                self.assertTrue(acquired)
                self.assertEqual(store.get_session_owner(101), "autonomy")

                acquired_other = store.try_acquire_session_lease(101, "user", ttl_sec=60)
                self.assertFalse(acquired_other)
                self.assertEqual(store.get_session_owner(101), "autonomy")

                store.release_session_lease(101, "autonomy")
                self.assertEqual(store.get_session_owner(101), "")

                acquired_user = store.try_acquire_session_lease(101, "user", ttl_sec=60)
                self.assertTrue(acquired_user)
                self.assertEqual(store.get_session_owner(101), "user")
            finally:
                store.close()

    def test_list_tasks_returns_recent_completed_items(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = QueueStore(Path(td) / "bot_state.db")
            try:
                first_id = store.enqueue_task(101, 1, "tester", "первая тема", [])
                second_id = store.enqueue_task(101, 1, "tester", "вторая тема", [])
                first = store.claim_next_task()
                assert first is not None
                store.complete_task(first.id, "ok")
                second = store.claim_next_task()
                assert second is not None
                store.complete_task(second.id, "ok")

                tasks = store.list_tasks(chat_id=101, statuses={"done"}, limit=2)
                self.assertEqual([task.id for task in tasks], [second_id, first_id])
                self.assertEqual(tasks[0].text, "вторая тема")
                self.assertEqual(tasks[0].status, "done")
            finally:
                store.close()
