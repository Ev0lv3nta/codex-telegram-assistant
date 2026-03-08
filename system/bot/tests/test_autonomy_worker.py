import asyncio
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from system.bot.autonomy_store import AutonomyStore, AutonomyTask
from system.bot.autonomy_worker import AutonomyWorker
from system.bot.autonomy_requests import ensure_autonomy_requests_scaffold
from system.bot.codex_runner import CodexRunResult
from system.bot.config import Settings
from system.bot.queue_store import QueueStore


def _make_settings(
    root: Path,
    *,
    autonomy_enabled: bool = True,
    autonomy_loop_poll_sec: int = 60,
    autonomy_session_step_limit: int = 4,
) -> Settings:
    return Settings(
        assistant_root=root,
        telegram_token="x",
        allowed_user_ids=set(),
        allowed_chat_ids=set(),
        poll_timeout_sec=25,
        idle_sleep_sec=0.1,
        codex_bin="codex",
        codex_timeout_sec=1800,
        codex_model="",
        codex_extra_args="",
        max_result_chars=3500,
        max_send_file_bytes=50 * 1024 * 1024,
        openrouter_api_key="test-key",
        openrouter_base_url="https://openrouter.ai/api/v1",
        openrouter_stt_model="mistralai/voxtral-small-24b-2507",
        openrouter_stt_timeout_sec=30,
        openrouter_stt_max_audio_bytes=1024 * 1024,
        state_db_path=root / "state.db",
        log_level="INFO",
        autonomy_enabled=autonomy_enabled,
        autonomy_heartbeat_sec=1,
        autonomy_loop_poll_sec=autonomy_loop_poll_sec,
        autonomy_session_step_limit=autonomy_session_step_limit,
        autonomy_notify_enabled=False,
        autonomy_notify_min_chars=20,
        autonomy_notify_cooldown_sec=60,
        autonomy_idle_ask_enabled=True,
        autonomy_idle_ask_cooldown_sec=21600,
        autonomy_idle_sleep_sec=21600,
        session_lease_sec=60,
    )


class _FakeRunner:
    def __init__(self, result: CodexRunResult) -> None:
        self._result = result
        self.calls: list[tuple[str, str]] = []

    def run(self, prompt: str, session_id: str = "") -> CodexRunResult:
        self.calls.append((prompt, session_id))
        return self._result


class _SequenceRunner:
    def __init__(self, results: list[CodexRunResult]) -> None:
        self._results = list(results)
        self.calls: list[tuple[str, str]] = []

    def run(self, prompt: str, session_id: str = "") -> CodexRunResult:
        self.calls.append((prompt, session_id))
        if not self._results:
            raise AssertionError("No more prepared runner results.")
        return self._results.pop(0)


class _FakeBot:
    def __init__(self) -> None:
        self.messages: list[tuple[int, str]] = []
        self.documents: list[tuple[int, str]] = []

    async def send_message(self, chat_id: int, text: str, **_kwargs: object) -> None:
        self.messages.append((chat_id, text))

    async def send_document(self, chat_id: int, document: object, caption: str | None = None, **_kwargs: object) -> None:
        self.documents.append((chat_id, caption or ""))

    async def send_chat_action(self, _chat_id: int, _action: object) -> None:
        return None


class AutonomyWorkerTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _write_active_request(root: Path, title: str, details: str = "") -> None:
        ensure_autonomy_requests_scaffold(root)
        target = root / "system" / "tasks" / "autonomy_requests.md"
        lines = [
            "# Автономные поручения",
            "",
            "## Активные",
            "",
            f"### {title}",
        ]
        if details:
            lines.append(f"- details: {details}")
        target.write_text("\n".join(lines) + "\n", encoding="utf-8")

    async def test_run_wakes_early_on_new_message_signal(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            settings = _make_settings(root, autonomy_loop_poll_sec=3600)
            queue_store = QueueStore(root / "state.db")
            autonomy_store = AutonomyStore(root / "state.db")
            stop_event = asyncio.Event()
            wake_event = asyncio.Event()
            runner = _FakeRunner(CodexRunResult(True, "should not run", "session-1"))
            bot = _FakeBot()
            worker = AutonomyWorker(
                settings,
                queue_store,
                autonomy_store,
                bot,
                runner,
                stop_event,
                wake_event=wake_event,
            )

            try:
                queue_store.note_chat_activity(101)
                queue_store.enqueue_task(
                    chat_id=101,
                    user_id=1,
                    username="tester",
                    text="hello",
                    attachments=[],
                )
                autonomy_store.schedule_next_wakeup_in(101, 3600)

                run_task = asyncio.create_task(worker.run())
                await asyncio.sleep(0.05)
                wake_event.set()

                for _ in range(20):
                    if autonomy_store.get_last_heartbeat_kind() == "skipped_user_pending":
                        break
                    await asyncio.sleep(0.05)

                self.assertEqual(autonomy_store.get_last_heartbeat_kind(), "skipped_user_pending")
                self.assertEqual(len(runner.calls), 0)
            finally:
                stop_event.set()
                wake_event.set()
                await run_task
                queue_store.close()
                autonomy_store.close()

    async def test_owner_request_creates_root_mission_and_links_task(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            settings = _make_settings(root, autonomy_session_step_limit=1)
            queue_store = QueueStore(root / "state.db")
            autonomy_store = AutonomyStore(root / "state.db")
            stop_event = asyncio.Event()
            runner = _FakeRunner(
                CodexRunResult(
                    True,
                    "\n".join(
                        [
                            "ACTION: STEP",
                            "TITLE: Собрать 3 источника",
                            "KIND: research",
                            "PRIORITY: 20",
                            "DETAILS:",
                            "Собрать базовый набор источников.",
                            "RESULT:",
                            "Собрал базовый набор источников по задаче владельца.",
                            "MISSION_STATUS: complete",
                            "WHY_NOT_DONE_NOW: Всё, что нужно для checkpoint, уже сделано.",
                            "BLOCKER_TYPE: none",
                            "GOAL_CHECK: Это напрямую закрывает текущий owner-request.",
                            "PROGRESS_DELTA: Источники собраны.",
                            "DRIFT_RISK: Низкий.",
                            "WHY_NOT_FINISHED_NOW: completed now",
                            "NEXT_STEP_JUSTIFICATION: no follow-up needed",
                        ]
                    ),
                    "session-owner",
                )
            )
            bot = _FakeBot()
            worker = AutonomyWorker(settings, queue_store, autonomy_store, bot, runner, stop_event)

            try:
                queue_store.note_chat_activity(101)
                self._write_active_request(
                    root,
                    "Подготовить исследование по рынку",
                    "Собрать сильные источники и коротко их оценить.",
                )
                await worker._run_once()

                done = autonomy_store.list_tasks(chat_id=101, statuses={"done"})
                self.assertEqual(len(done), 1)
                self.assertIsNotNone(done[0].mission_id)
                mission = autonomy_store.get_mission(done[0].mission_id or 0)
                self.assertIsNotNone(mission)
                assert mission is not None
                self.assertEqual(mission.source, "owner_request")
                self.assertIn("Подготовить исследование по рынку", mission.root_objective)
                self.assertEqual(mission.status, "completed")
            finally:
                queue_store.close()
                autonomy_store.close()

    async def test_run_once_skips_when_autonomy_paused_for_chat(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            settings = _make_settings(root)
            queue_store = QueueStore(root / "state.db")
            autonomy_store = AutonomyStore(root / "state.db")
            stop_event = asyncio.Event()
            runner = _FakeRunner(CodexRunResult(True, "should not run", "session-1"))
            bot = _FakeBot()
            worker = AutonomyWorker(settings, queue_store, autonomy_store, bot, runner, stop_event)

            try:
                queue_store.note_chat_activity(101)
                autonomy_store.set_autonomy_paused(101, True)
                autonomy_store.enqueue_task(chat_id=101, title="Автономная задача")

                await worker._run_once()

                pending = autonomy_store.list_tasks(chat_id=101, statuses={"pending"})
                self.assertEqual(len(pending), 1)
                self.assertEqual(len(runner.calls), 0)
                self.assertEqual(autonomy_store.get_mode(101), "stopped")
            finally:
                queue_store.close()
                autonomy_store.close()

    async def test_run_does_not_spin_when_autonomy_paused_and_next_wakeup_cleared(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            settings = _make_settings(root, autonomy_loop_poll_sec=3600)
            queue_store = QueueStore(root / "state.db")
            autonomy_store = AutonomyStore(root / "state.db")
            stop_event = asyncio.Event()
            wake_event = asyncio.Event()
            runner = _FakeRunner(CodexRunResult(True, "should not run", "session-1"))
            bot = _FakeBot()
            worker = AutonomyWorker(
                settings,
                queue_store,
                autonomy_store,
                bot,
                runner,
                stop_event,
                wake_event=wake_event,
            )

            try:
                queue_store.note_chat_activity(101)
                autonomy_store.set_autonomy_paused(101, True)
                autonomy_store.clear_next_wakeup(101)

                run_task = asyncio.create_task(worker.run())
                await asyncio.sleep(0.1)

                self.assertFalse(run_task.done())
                self.assertEqual(autonomy_store.get_mode(101), "")
                self.assertEqual(len(runner.calls), 0)
            finally:
                stop_event.set()
                wake_event.set()
                await run_task
                queue_store.close()
                autonomy_store.close()

    async def test_run_once_completes_ready_task(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            settings = _make_settings(root)
            queue_store = QueueStore(root / "state.db")
            autonomy_store = AutonomyStore(root / "state.db")
            stop_event = asyncio.Event()
            runner = _FakeRunner(CodexRunResult(True, "автономный результат", "session-1"))
            bot = _FakeBot()
            worker = AutonomyWorker(settings, queue_store, autonomy_store, bot, runner, stop_event)

            try:
                queue_store.note_chat_activity(101)
                ensure_autonomy_requests_scaffold(root)
                queue_store.set_chat_session_id(101, "session-0")
                autonomy_store.enqueue_task(
                    chat_id=101,
                    title="Проверить идею",
                    details="Один шаг",
                    kind="research",
                )

                await worker._run_once()

                done = autonomy_store.list_tasks(chat_id=101, statuses={"done"})
                self.assertEqual(len(done), 1)
                self.assertIn("автономный результат", done[0].result_text)
                self.assertEqual(queue_store.get_chat_session_id(101), "session-1")
                self.assertEqual(queue_store.get_session_owner(101), "")
                self.assertEqual(len(runner.calls), 1)
                journal = (
                    root
                    / "system"
                    / "tasks"
                    / "autonomy_journal"
                    / datetime.now(ZoneInfo("Europe/Moscow")).strftime("%Y-%m-%d.md")
                )
                self.assertTrue(journal.exists())
                self.assertIn("Проверить идею", journal.read_text(encoding="utf-8"))
            finally:
                queue_store.close()
                autonomy_store.close()

    async def test_run_once_skips_when_user_tasks_pending(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            settings = _make_settings(root)
            queue_store = QueueStore(root / "state.db")
            autonomy_store = AutonomyStore(root / "state.db")
            stop_event = asyncio.Event()
            runner = _FakeRunner(CodexRunResult(True, "should not run", "session-1"))
            bot = _FakeBot()
            worker = AutonomyWorker(settings, queue_store, autonomy_store, bot, runner, stop_event)

            try:
                queue_store.enqueue_task(
                    chat_id=101,
                    user_id=1,
                    username="tester",
                    text="hello",
                    attachments=[],
                )
                autonomy_store.enqueue_task(chat_id=101, title="Автономная задача")

                await worker._run_once()

                pending = autonomy_store.list_tasks(chat_id=101, statuses={"pending"})
                self.assertEqual(len(pending), 1)
                self.assertEqual(len(runner.calls), 0)
            finally:
                queue_store.close()
                autonomy_store.close()

    async def test_run_once_executes_spontaneous_wakeup_step_when_backlog_empty(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            settings = _make_settings(root)
            queue_store = QueueStore(root / "state.db")
            autonomy_store = AutonomyStore(root / "state.db")
            stop_event = asyncio.Event()
            runner = _FakeRunner(
                CodexRunResult(
                    True,
                    "\n".join(
                        [
                            "ACTION: STEP",
                            "TITLE: Подготовить исследование",
                            "KIND: research",
                            "PRIORITY: 30",
                            "DETAILS:",
                            "Собрать краткий план следующего шага.",
                            "RESULT:",
                            "Собрал короткий стартовый план исследования.",
                        ]
                    ),
                    "session-1",
                )
            )
            bot = _FakeBot()
            worker = AutonomyWorker(settings, queue_store, autonomy_store, bot, runner, stop_event)

            try:
                queue_store.note_chat_activity(101)
                ensure_autonomy_requests_scaffold(root)
                task = queue_store.enqueue_task(
                    chat_id=101,
                    user_id=1,
                    username="tester",
                    text="Я сейчас изучаю юнит-экономику",
                    attachments=[],
                )
                claimed = queue_store.claim_next_task()
                self.assertIsNotNone(claimed)
                if claimed is not None:
                    queue_store.complete_task(task, "ok")
                await worker._run_once()

                done = autonomy_store.list_tasks(chat_id=101, statuses={"done"})
                self.assertEqual(len(done), 1)
                self.assertEqual(done[0].title, "Подготовить исследование")
                self.assertEqual(done[0].source, "heartbeat")
                self.assertEqual(queue_store.get_chat_session_id(101), "session-1")
                self.assertEqual(len(runner.calls), 1)
                self.assertIn("Я сейчас изучаю юнит-экономику", runner.calls[0][0])
                self.assertIn("При необходимости сам открой нужные файлы workspace", runner.calls[0][0])
                self.assertIn("/root/personal-assistant/memory/about_user.md", runner.calls[0][0])
                self.assertIn("/root/personal-assistant/system/tasks/autonomy_requests.md", runner.calls[0][0])
                self.assertIn("ACTION: STEP", runner.calls[0][0])
            finally:
                queue_store.close()
                autonomy_store.close()

    async def test_run_once_does_not_start_spontaneous_wakeup_when_future_task_exists(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            settings = _make_settings(root)
            queue_store = QueueStore(root / "state.db")
            autonomy_store = AutonomyStore(root / "state.db")
            stop_event = asyncio.Event()
            runner = _FakeRunner(
                CodexRunResult(
                    True,
                    "ACTION: STEP\nTITLE: Не должно выполниться\nKIND: research\nPRIORITY: 50\nDETAILS:\n-\nRESULT:\nНе должно выполниться.",
                    "session-1",
                )
            )
            bot = _FakeBot()
            worker = AutonomyWorker(settings, queue_store, autonomy_store, bot, runner, stop_event)

            try:
                queue_store.note_chat_activity(101)
                ensure_autonomy_requests_scaffold(root)
                autonomy_store.enqueue_task(
                    chat_id=101,
                    title="Отложенный follow-up",
                    details="Нужно дождаться scheduled_for",
                    kind="research",
                    scheduled_for="2099-01-01T00:00:00+00:00",
                    source="followup",
                )

                await worker._run_once()

                pending = autonomy_store.list_tasks(chat_id=101, statuses={"pending"})
                done = autonomy_store.list_tasks(chat_id=101, statuses={"done"})
                self.assertEqual(len(pending), 1)
                self.assertEqual(len(done), 0)
                self.assertEqual(len(runner.calls), 0)
                self.assertEqual(autonomy_store.get_last_heartbeat_kind(), "sleeping_scheduled")
                mission = autonomy_store.get_active_mission(101)
                self.assertIsNotNone(mission)
                assert mission is not None
                self.assertEqual(mission.title, "Отложенный follow-up")
                self.assertEqual(mission.scheduled_for, "2099-01-01T00:00:00+00:00")
            finally:
                queue_store.close()
                autonomy_store.close()

    async def test_run_once_skips_followup_after_user_interrupt_in_spontaneous_step(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            settings = _make_settings(root)
            queue_store = QueueStore(root / "state.db")
            autonomy_store = AutonomyStore(root / "state.db")
            stop_event = asyncio.Event()

            class _WakeupInterruptingRunner:
                def __init__(self) -> None:
                    self.calls: list[tuple[str, str]] = []

                def run(self, prompt: str, session_id: str = "") -> CodexRunResult:
                    self.calls.append((prompt, session_id))
                    queue_store.enqueue_task(
                        chat_id=101,
                        user_id=1,
                        username="tester",
                        text="прерываю wakeup",
                        attachments=[],
                    )
                    return CodexRunResult(
                        True,
                        "\n".join(
                            [
                                "ACTION: STEP",
                                "TITLE: Подготовить исследование",
                                "KIND: research",
                                "PRIORITY: 30",
                                "DETAILS:",
                                "Собрать краткий план следующего шага.",
                                "RESULT:",
                                "Собрал короткий план.",
                                "",
                                "[[autonomy-next]]",
                                "ACTION: ENQUEUE",
                                "TITLE: Продолжить исследование",
                                "KIND: research",
                                "PRIORITY: 35",
                                "DELAY_SEC: 120",
                                "DETAILS:",
                                "Сделать ещё один короткий шаг.",
                                "[[/autonomy-next]]",
                            ]
                        ),
                        "session-1",
                    )

            runner = _WakeupInterruptingRunner()
            bot = _FakeBot()
            worker = AutonomyWorker(settings, queue_store, autonomy_store, bot, runner, stop_event)

            try:
                queue_store.note_chat_activity(101)
                ensure_autonomy_requests_scaffold(root)
                user_task_id = queue_store.enqueue_task(
                    chat_id=101,
                    user_id=1,
                    username="tester",
                    text="Надо сделать короткий автономный ресерч",
                    attachments=[],
                )
                claimed = queue_store.claim_next_task()
                self.assertIsNotNone(claimed)
                queue_store.complete_task(user_task_id, "ok")
                await worker._run_once()

                pending = autonomy_store.list_tasks(chat_id=101, statuses={"pending"})
                self.assertEqual(len(pending), 0)
                done = autonomy_store.list_tasks(chat_id=101, statuses={"done"})
                self.assertEqual(len(done), 1)
                self.assertIn("autonomy-paused", done[0].result_text)
                self.assertEqual(len(runner.calls), 1)
            finally:
                queue_store.close()
                autonomy_store.close()

    async def test_run_once_closes_current_task_when_runner_returns_complete(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            settings = _make_settings(root)
            queue_store = QueueStore(root / "state.db")
            autonomy_store = AutonomyStore(root / "state.db")
            stop_event = asyncio.Event()
            runner = _FakeRunner(
                CodexRunResult(
                    True,
                    "\n".join(
                        [
                            "ACTION: COMPLETE",
                            "RESULT:",
                            "Поручение уже закрыто и больше не должно оставаться среди активных.",
                        ]
                    ),
                    "session-1",
                )
            )
            bot = _FakeBot()
            worker = AutonomyWorker(settings, queue_store, autonomy_store, bot, runner, stop_event)

            try:
                queue_store.note_chat_activity(101)
                ensure_autonomy_requests_scaffold(root)
                queue_store.set_chat_session_id(101, "session-0")
                task_id = autonomy_store.enqueue_task(
                    chat_id=101,
                    title="Старая follow-up задача",
                    details="Закрыть, если поручение уже завершено",
                    kind="research",
                    source="followup",
                )

                await worker._run_once()

                done = autonomy_store.list_tasks(chat_id=101, statuses={"done"})
                self.assertEqual(len(done), 1)
                self.assertEqual(done[0].id, task_id)
                self.assertIn("уже закрыто", done[0].result_text)
                pending = autonomy_store.list_tasks(chat_id=101, statuses={"pending"})
                self.assertEqual(len(pending), 0)
            finally:
                queue_store.close()
                autonomy_store.close()

    async def test_run_once_notifies_on_meaningful_result_when_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            settings = _make_settings(root)
            settings = Settings(
                **{
                    **settings.__dict__,
                    "autonomy_notify_enabled": True,
                    "autonomy_notify_min_chars": 10,
                }
            )
            queue_store = QueueStore(root / "state.db")
            autonomy_store = AutonomyStore(root / "state.db")
            stop_event = asyncio.Event()
            runner = _FakeRunner(
                CodexRunResult(
                    True,
                    "Собрал короткую, но уже полезную автономную заметку для владельца.",
                    "session-2",
                )
            )
            bot = _FakeBot()
            worker = AutonomyWorker(settings, queue_store, autonomy_store, bot, runner, stop_event)

            try:
                queue_store.note_chat_activity(101)
                autonomy_store.enqueue_task(chat_id=101, title="Подготовить заметку")

                await worker._run_once()

                self.assertEqual(len(bot.messages), 1)
                self.assertIn("Автономно", bot.messages[0][1])
            finally:
                queue_store.close()
                autonomy_store.close()

    async def test_run_once_sends_compact_owner_facing_autonomy_update(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            settings = _make_settings(root)
            settings = Settings(
                **{
                    **settings.__dict__,
                    "autonomy_notify_enabled": True,
                    "autonomy_notify_min_chars": 1,
                    "autonomy_notify_cooldown_sec": 0,
                }
            )
            queue_store = QueueStore(root / "state.db")
            autonomy_store = AutonomyStore(root / "state.db")
            stop_event = asyncio.Event()
            runner = _FakeRunner(
                CodexRunResult(
                    True,
                    "\n".join(
                        [
                            "Фикс внесён и проверен тестами.",
                            "",
                            "Что изменено:",
                            "- в `system/bot/autonomy_worker.py` обновлена логика уведомлений",
                            "",
                            "Проверка:",
                            "- python3 -m unittest system.bot.tests.test_autonomy_worker",
                            "",
                            "Self-check: шаг полезный, следующий хороший шаг я сделаю сам.",
                        ]
                    ),
                    "session-2",
                )
            )
            bot = _FakeBot()
            worker = AutonomyWorker(settings, queue_store, autonomy_store, bot, runner, stop_event)

            try:
                queue_store.note_chat_activity(101)
                autonomy_store.enqueue_task(chat_id=101, title="Сделать компактный отчёт")

                await worker._run_once()

                self.assertEqual(len(bot.messages), 1)
                self.assertIn("Автономно:", bot.messages[0][1])
                self.assertIn("Фикс внесён и проверен тестами.", bot.messages[0][1])
                self.assertIn("python3 -m unittest", bot.messages[0][1])
                self.assertIn("Self-check", bot.messages[0][1])
            finally:
                queue_store.close()
                autonomy_store.close()

    async def test_run_once_does_not_notify_low_value_service_status(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            settings = _make_settings(root)
            settings = Settings(
                **{
                    **settings.__dict__,
                    "autonomy_notify_enabled": True,
                    "autonomy_notify_min_chars": 1,
                    "autonomy_notify_cooldown_sec": 0,
                }
            )
            queue_store = QueueStore(root / "state.db")
            autonomy_store = AutonomyStore(root / "state.db")
            stop_event = asyncio.Event()
            runner = _FakeRunner(
                CodexRunResult(
                    True,
                    "Сервис бота сейчас живой и в состоянии active. MainPID=61674.",
                    "session-2",
                )
            )
            bot = _FakeBot()
            worker = AutonomyWorker(settings, queue_store, autonomy_store, bot, runner, stop_event)

            try:
                queue_store.note_chat_activity(101)
                autonomy_store.enqueue_task(chat_id=101, title="Проверить сервис", kind="review")

                await worker._run_once()

                self.assertEqual(bot.messages, [])
            finally:
                queue_store.close()
                autonomy_store.close()

    async def test_run_once_does_not_notify_internal_complete_closure(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            settings = _make_settings(root)
            settings = Settings(
                **{
                    **settings.__dict__,
                    "autonomy_notify_enabled": True,
                    "autonomy_notify_min_chars": 1,
                    "autonomy_notify_cooldown_sec": 0,
                }
            )
            queue_store = QueueStore(root / "state.db")
            autonomy_store = AutonomyStore(root / "state.db")
            stop_event = asyncio.Event()
            runner = _FakeRunner(
                CodexRunResult(
                    True,
                    "\n".join(
                        [
                            "ACTION: COMPLETE",
                            "RESULT:",
                            "Текущую внутреннюю верификационную линию можно закрыть без нового шага.",
                        ]
                    ),
                    "session-2",
                )
            )
            bot = _FakeBot()
            worker = AutonomyWorker(settings, queue_store, autonomy_store, bot, runner, stop_event)

            try:
                queue_store.note_chat_activity(101)
                autonomy_store.enqueue_task(chat_id=101, title="Внутренне закрыть хвост")

                await worker._run_once()

                self.assertEqual(bot.messages, [])
            finally:
                queue_store.close()
                autonomy_store.close()

    async def test_run_once_does_not_notify_project_update_without_notify_owner_marker(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            settings = _make_settings(root)
            settings = Settings(
                **{
                    **settings.__dict__,
                    "autonomy_notify_enabled": True,
                    "autonomy_notify_min_chars": 1,
                    "autonomy_notify_cooldown_sec": 0,
                }
            )
            queue_store = QueueStore(root / "state.db")
            autonomy_store = AutonomyStore(root / "state.db")
            stop_event = asyncio.Event()
            runner = _FakeRunner(
                CodexRunResult(
                    True,
                    "Сделал внутренний project-шаг и подготовил основу для следующего слоя.",
                    "session-2",
                )
            )
            bot = _FakeBot()
            worker = AutonomyWorker(settings, queue_store, autonomy_store, bot, runner, stop_event)

            try:
                queue_store.note_chat_activity(101)
                autonomy_store.enqueue_task(chat_id=101, title="Тихий project-шаг", kind="project")

                await worker._run_once()

                self.assertEqual(bot.messages, [])
            finally:
                queue_store.close()
                autonomy_store.close()

    async def test_run_once_notifies_project_update_with_notify_owner_marker(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            settings = _make_settings(root)
            settings = Settings(
                **{
                    **settings.__dict__,
                    "autonomy_notify_enabled": True,
                    "autonomy_notify_min_chars": 1,
                    "autonomy_notify_cooldown_sec": 0,
                }
            )
            queue_store = QueueStore(root / "state.db")
            autonomy_store = AutonomyStore(root / "state.db")
            stop_event = asyncio.Event()
            runner = _FakeRunner(
                CodexRunResult(
                    True,
                    "\n".join(
                        [
                            "Системно приглушил owner-facing шум от внутренних project-закрытий.",
                            "",
                            "[[notify-owner]]",
                            "REASON: Это реально меняет то, что владелец увидит в чате.",
                            "[[/notify-owner]]",
                        ]
                    ),
                    "session-2",
                )
            )
            bot = _FakeBot()
            worker = AutonomyWorker(settings, queue_store, autonomy_store, bot, runner, stop_event)

            try:
                queue_store.note_chat_activity(101)
                autonomy_store.enqueue_task(chat_id=101, title="Шумный project-шаг", kind="project")

                await worker._run_once()

                self.assertEqual(len(bot.messages), 1)
                self.assertIn("Автономно:", bot.messages[0][1])
                self.assertIn("Системно приглушил owner-facing шум", bot.messages[0][1])
                self.assertNotIn("[[notify-owner]]", bot.messages[0][1])
            finally:
                queue_store.close()
                autonomy_store.close()

    def test_owner_notification_text_keeps_multiple_meaningful_lines(self) -> None:
        task = AutonomyTask(
            id=1,
            chat_id=101,
            mission_id=None,
            kind="research",
            title="Разобрать Ouroboros",
            details="",
            priority=10,
            status="done",
            created_at="2026-03-07T20:00:00+00:00",
            scheduled_for="2026-03-07T20:00:00+00:00",
            parent_task_id=None,
            source="assistant",
            started_at=None,
            finished_at=None,
            blocked_user_signal=None,
            result_text="",
            error_text="",
        )
        text = "\n".join(
            [
                "У Ouroboros есть сильные идеи, но почти все они опасны в полном объёме.",
                "1. Стоит брать жёстко оформленное identity/constitution-ядро.",
                "2. Стоит брать облегчённый review-контур перед изменениями в себе.",
                "3. Не стоит брать постоянное background consciousness.",
            ]
        )

        result = AutonomyWorker._owner_notification_text(task, text)

        self.assertIn("У Ouroboros есть сильные идеи", result)
        self.assertIn("1. Стоит брать", result)
        self.assertIn("2. Стоит брать", result)
        self.assertIn("3. Не стоит брать", result)

    def test_owner_notification_text_strips_autonomy_control_block(self) -> None:
        task = AutonomyTask(
            id=1,
            chat_id=101,
            mission_id=None,
            kind="note",
            title="Подготовить Markdown",
            details="",
            priority=10,
            status="done",
            created_at="2026-03-07T20:00:00+00:00",
            scheduled_for="2026-03-07T20:00:00+00:00",
            parent_task_id=None,
            source="assistant",
            started_at=None,
            finished_at=None,
            blocked_user_signal=None,
            result_text="",
            error_text="",
        )
        text = "\n".join(
            [
                "Готов чистовой драфт.",
                "",
                "[[autonomy-next]]",
                "ACTION: ENQUEUE",
                "TITLE: Следующий шаг",
                "KIND: note",
                "PRIORITY: 200",
                "DELAY_SEC: 300",
                "DETAILS:",
                "Сделать ещё один шаг.",
                "[[/autonomy-next]]",
            ]
        )

        result = AutonomyWorker._owner_notification_text(task, text)

        self.assertEqual(result, "Готов чистовой драфт.")

    def test_owner_notification_text_strips_self_review_block(self) -> None:
        task = AutonomyTask(
            id=1,
            chat_id=101,
            mission_id=None,
            kind="project",
            title="Докрутить pulse",
            details="",
            priority=10,
            status="done",
            created_at="2026-03-07T20:00:00+00:00",
            scheduled_for="2026-03-07T20:00:00+00:00",
            parent_task_id=None,
            source="assistant",
            started_at=None,
            finished_at=None,
            blocked_user_signal=None,
            result_text="",
            error_text="",
        )
        text = "\n".join(
            [
                "Pulse стал понятнее.",
                "",
                "[[self-review]]",
                "CHANGE: Упростил owner-facing слой.",
                "WHY: Чтобы не было техшума.",
                "RISK: Можно скрыть полезную служебную деталь.",
                "CHECK: Проверить тесты и live refresh.",
                "[[/self-review]]",
            ]
        )

        result = AutonomyWorker._owner_notification_text(task, text)

        self.assertEqual(result, "Pulse стал понятнее.")

    def test_owner_notification_text_strips_notify_owner_block(self) -> None:
        task = AutonomyTask(
            id=1,
            chat_id=101,
            mission_id=None,
            kind="project",
            title="Докрутить silent updates",
            details="",
            priority=10,
            status="done",
            created_at="2026-03-07T20:00:00+00:00",
            scheduled_for="2026-03-07T20:00:00+00:00",
            parent_task_id=None,
            source="assistant",
            started_at=None,
            finished_at=None,
            blocked_user_signal=None,
            result_text="",
            error_text="",
        )
        text = "\n".join(
            [
                "Теперь внутренние project-шаги могут идти тихо.",
                "",
                "[[notify-owner]]",
                "REASON: Это реально заметный owner-facing сдвиг.",
                "[[/notify-owner]]",
            ]
        )

        result = AutonomyWorker._owner_notification_text(task, text)

        self.assertEqual(result, "Теперь внутренние project-шаги могут идти тихо.")

    async def test_run_once_sleeps_quietly_when_idle(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            settings = _make_settings(root)
            settings = Settings(
                **{
                    **settings.__dict__,
                    "autonomy_idle_ask_enabled": True,
                    "autonomy_idle_ask_cooldown_sec": 21600,
                }
            )
            queue_store = QueueStore(root / "state.db")
            autonomy_store = AutonomyStore(root / "state.db")
            stop_event = asyncio.Event()
            runner = _FakeRunner(CodexRunResult(True, "should not run", "session-2"))
            bot = _FakeBot()
            worker = AutonomyWorker(settings, queue_store, autonomy_store, bot, runner, stop_event)

            try:
                queue_store.note_chat_activity(101)
                ensure_autonomy_requests_scaffold(root)

                await worker._run_once()

                self.assertEqual(len(bot.messages), 0)
                self.assertEqual(len(runner.calls), 0)
                self.assertEqual(autonomy_store.get_last_heartbeat_kind(), "sleeping_idle")
                self.assertEqual(autonomy_store.get_mode(101), "idle")
            finally:
                queue_store.close()
                autonomy_store.close()

    async def test_run_once_sleeps_for_hours_after_negative_idle_reply(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            settings = _make_settings(root)
            settings = Settings(
                **{
                    **settings.__dict__,
                    "autonomy_idle_ask_enabled": True,
                    "autonomy_idle_ask_cooldown_sec": 21600,
                    "autonomy_idle_sleep_sec": 21600,
                }
            )
            queue_store = QueueStore(root / "state.db")
            autonomy_store = AutonomyStore(root / "state.db")
            stop_event = asyncio.Event()
            runner = _FakeRunner(CodexRunResult(True, "should not run", "session-2"))
            bot = _FakeBot()
            worker = AutonomyWorker(settings, queue_store, autonomy_store, bot, runner, stop_event)

            try:
                queue_store.note_chat_activity(101)
                ensure_autonomy_requests_scaffold(root)

                await worker._run_once()
                user_task_id = queue_store.enqueue_task(
                    chat_id=101,
                    user_id=1,
                    username="tester",
                    text="нет, мне сейчас ничего не нужно",
                    attachments=[],
                )
                claimed = queue_store.claim_next_task()
                self.assertIsNotNone(claimed)
                queue_store.complete_task(user_task_id, "ok")

                await worker._run_once()
                await worker._run_once()

                self.assertEqual(len(runner.calls), 0)
                self.assertEqual(len(bot.messages), 0)
                self.assertEqual(autonomy_store.get_last_heartbeat_kind(), "sleeping_user_declined")
                self.assertTrue(autonomy_store.idle_snoozed(101))
            finally:
                queue_store.close()
                autonomy_store.close()

    async def test_run_once_can_notify_with_short_question_when_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            settings = _make_settings(root)
            settings = Settings(
                **{
                    **settings.__dict__,
                    "autonomy_notify_enabled": True,
                    "autonomy_notify_min_chars": 100,
                    "autonomy_notify_cooldown_sec": 0,
                }
            )
            queue_store = QueueStore(root / "state.db")
            autonomy_store = AutonomyStore(root / "state.db")
            stop_event = asyncio.Event()
            runner = _FakeRunner(
                CodexRunResult(
                    True,
                    "Нужно уточнение: делать методичку краткой или подробной?",
                    "session-2",
                )
            )
            bot = _FakeBot()
            worker = AutonomyWorker(settings, queue_store, autonomy_store, bot, runner, stop_event)

            try:
                queue_store.note_chat_activity(101)
                autonomy_store.enqueue_task(chat_id=101, title="Уточнить формат методички")

                await worker._run_once()

                self.assertEqual(len(bot.messages), 1)
                self.assertIn("Нужно уточнение", bot.messages[0][1])
                waiting = autonomy_store.list_tasks(chat_id=101, statuses={"waiting_user"})
                self.assertEqual(len(waiting), 1)
                journal = (
                    root
                    / "system"
                    / "tasks"
                    / "autonomy_journal"
                    / datetime.now(ZoneInfo("Europe/Moscow")).strftime("%Y-%m-%d.md")
                )
                self.assertTrue(journal.exists())
                journal_text = journal.read_text(encoding="utf-8")
                self.assertIn("· waiting_user", journal_text)
            finally:
                queue_store.close()
                autonomy_store.close()

    async def test_run_once_does_not_duplicate_waiting_user_blocker(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            settings = _make_settings(root)
            settings = Settings(
                **{
                    **settings.__dict__,
                    "autonomy_notify_enabled": True,
                    "autonomy_notify_min_chars": 1,
                    "autonomy_notify_cooldown_sec": 0,
                }
            )
            queue_store = QueueStore(root / "state.db")
            autonomy_store = AutonomyStore(root / "state.db")
            stop_event = asyncio.Event()
            runner = _FakeRunner(
                CodexRunResult(
                    True,
                    "Подтверди рестарт сервиса, и я проверю следующий heartbeat?",
                    "session-2",
                )
            )
            bot = _FakeBot()
            worker = AutonomyWorker(settings, queue_store, autonomy_store, bot, runner, stop_event)

            try:
                queue_store.note_chat_activity(101)
                autonomy_store.enqueue_task(chat_id=101, title="Проверить рестарт")

                await worker._run_once()
                autonomy_store.schedule_next_wakeup_in(101, 0)
                await worker._run_once()

                self.assertEqual(len(bot.messages), 1)
                self.assertEqual(len(runner.calls), 2)
                waiting = autonomy_store.list_tasks(chat_id=101, statuses={"waiting_user"})
                self.assertEqual(len(waiting), 1)
                self.assertEqual(autonomy_store.get_last_heartbeat_kind(), "noop")
            finally:
                queue_store.close()
                autonomy_store.close()

    async def test_run_once_can_do_other_work_while_task_waits_for_user(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            settings = _make_settings(root)
            settings = Settings(
                **{
                    **settings.__dict__,
                    "autonomy_notify_enabled": True,
                    "autonomy_notify_min_chars": 1,
                    "autonomy_notify_cooldown_sec": 0,
                }
            )
            queue_store = QueueStore(root / "state.db")
            autonomy_store = AutonomyStore(root / "state.db")
            stop_event = asyncio.Event()
            runner = _SequenceRunner(
                [
                    CodexRunResult(
                        True,
                        "Подтверди рестарт сервиса, и я проверю следующий heartbeat?",
                        "session-2",
                    ),
                    CodexRunResult(
                        True,
                        "\n".join(
                            [
                                "ACTION: STEP",
                                "TITLE: Проверить ещё один безопасный хвост",
                                "KIND: review",
                                "PRIORITY: 40",
                                "DETAILS:",
                                "Сделать ещё одну безопасную проверку, пока первый хвост ждёт ответа.",
                                "RESULT:",
                                "Нашёл ещё один безопасный наблюдательный хвост без внешнего блокера.",
                            ]
                        ),
                        "session-2",
                    ),
                ]
            )
            bot = _FakeBot()
            worker = AutonomyWorker(settings, queue_store, autonomy_store, bot, runner, stop_event)

            try:
                queue_store.note_chat_activity(101)
                autonomy_store.enqueue_task(chat_id=101, title="Проверить рестарт")

                await worker._run_once()
                autonomy_store.schedule_next_wakeup_in(101, 0)
                await worker._run_once()

                waiting = autonomy_store.list_tasks(chat_id=101, statuses={"waiting_user"})
                done = autonomy_store.list_tasks(chat_id=101, statuses={"done"})
                self.assertEqual(len(waiting), 1)
                self.assertEqual(len(done), 1)
                self.assertIn("без внешнего блокера", done[0].result_text)
                self.assertEqual(len(runner.calls), 2)
            finally:
                queue_store.close()
                autonomy_store.close()

    async def test_waiting_task_does_not_resume_on_irrelevant_user_signal(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            settings = _make_settings(root)
            settings = Settings(
                **{
                    **settings.__dict__,
                    "autonomy_notify_enabled": True,
                    "autonomy_notify_min_chars": 1,
                    "autonomy_notify_cooldown_sec": 0,
                }
            )
            queue_store = QueueStore(root / "state.db")
            autonomy_store = AutonomyStore(root / "state.db")
            stop_event = asyncio.Event()
            runner = _SequenceRunner(
                [
                    CodexRunResult(
                        True,
                        "Подтверди рестарт сервиса, и я проверю следующий heartbeat?",
                        "session-2",
                    ),
                    CodexRunResult(
                        True,
                        "\n".join(
                            [
                                "ACTION: STEP",
                                "TITLE: Другая спонтанная задача",
                                "KIND: review",
                                "PRIORITY: 40",
                                "DETAILS:",
                                "Проверить другой безопасный хвост.",
                                "RESULT:",
                                "Нашёл другой безопасный хвост без возобновления waiting_user.",
                            ]
                        ),
                        "session-2",
                    ),
                ]
            )
            bot = _FakeBot()
            worker = AutonomyWorker(settings, queue_store, autonomy_store, bot, runner, stop_event)

            try:
                queue_store.note_chat_activity(101)
                autonomy_store.enqueue_task(chat_id=101, title="Проверить рестарт")

                await worker._run_once()

                waiting = autonomy_store.list_tasks(chat_id=101, statuses={"waiting_user"})
                self.assertEqual(len(waiting), 1)

                user_task_id = queue_store.enqueue_task(
                    chat_id=101,
                    user_id=1,
                    username="tester",
                    text="кстати, упакуй проект в архив",
                    attachments=[],
                )
                claimed = queue_store.claim_next_task()
                self.assertIsNotNone(claimed)
                queue_store.complete_task(user_task_id, "ok")

                await worker._run_once()

                waiting = autonomy_store.list_tasks(chat_id=101, statuses={"waiting_user"})
                done = autonomy_store.list_tasks(chat_id=101, statuses={"done"})
                self.assertEqual(len(waiting), 1)
                self.assertEqual(len(done), 1)
                self.assertIn("другой безопасный хвост", done[0].result_text)
                self.assertEqual(len(runner.calls), 2)
            finally:
                queue_store.close()
                autonomy_store.close()

    async def test_waiting_task_resumes_after_new_user_signal(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            settings = _make_settings(root)
            settings = Settings(
                **{
                    **settings.__dict__,
                    "autonomy_notify_enabled": True,
                    "autonomy_notify_min_chars": 1,
                    "autonomy_notify_cooldown_sec": 0,
                }
            )
            queue_store = QueueStore(root / "state.db")
            autonomy_store = AutonomyStore(root / "state.db")
            stop_event = asyncio.Event()
            runner = _SequenceRunner(
                [
                    CodexRunResult(
                        True,
                        "Подтверди рестарт сервиса, и я проверю следующий heartbeat?",
                        "session-2",
                    ),
                    CodexRunResult(
                        True,
                        "Проверил после подтверждения: задача доведена до конца.",
                        "session-2",
                    ),
                ]
            )
            bot = _FakeBot()
            worker = AutonomyWorker(settings, queue_store, autonomy_store, bot, runner, stop_event)

            try:
                queue_store.note_chat_activity(101)
                autonomy_store.enqueue_task(chat_id=101, title="Проверить рестарт")

                await worker._run_once()

                waiting = autonomy_store.list_tasks(chat_id=101, statuses={"waiting_user"})
                self.assertEqual(len(waiting), 1)
                self.assertEqual(len(bot.messages), 1)

                user_task_id = queue_store.enqueue_task(
                    chat_id=101,
                    user_id=1,
                    username="tester",
                    text="подтверждаю",
                    attachments=[],
                )
                claimed = queue_store.claim_next_task()
                self.assertIsNotNone(claimed)
                queue_store.complete_task(user_task_id, "ok")

                await worker._run_once()

                done = autonomy_store.list_tasks(chat_id=101, statuses={"done"})
                self.assertEqual(len(done), 1)
                self.assertIn("доведена до конца", done[0].result_text)
                self.assertEqual(len(runner.calls), 2)
            finally:
                queue_store.close()
                autonomy_store.close()

    async def test_run_once_inlines_multistep_maintenance_followup(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            settings = _make_settings(root)
            queue_store = QueueStore(root / "state.db")
            autonomy_store = AutonomyStore(root / "state.db")
            stop_event = asyncio.Event()
            runner = _SequenceRunner(
                [
                    CodexRunResult(
                        True,
                        "\n".join(
                            [
                                "ACTION: STEP",
                                "TITLE: Найти причину бага",
                                "KIND: maintenance",
                                "PRIORITY: 20",
                                "DETAILS:",
                                "Локализовать причину.",
                                "RESULT:",
                                "Нашёл корень проблемы.",
                                "",
                                "[[autonomy-next]]",
                                "ACTION: ENQUEUE",
                                "TITLE: Исправить причину бага",
                                "KIND: maintenance",
                                "PRIORITY: 10",
                                "DELAY_SEC: 0",
                                "DETAILS:",
                                "Внести исправление и проверить.",
                                "[[/autonomy-next]]",
                            ]
                        ),
                        "session-5",
                    ),
                    CodexRunResult(
                        True,
                        "\n".join(
                            [
                                "VERDICT: APPROVE_CONTINUE_NOW",
                                "REASON: Локальный хвост лучше дожать в этом же сеансе.",
                            ]
                        ),
                        "session-5",
                    ),
                    CodexRunResult(
                        True,
                        "\n".join(
                            [
                                "ACTION: STEP",
                                "TITLE: Исправить причину бага",
                                "KIND: maintenance",
                                "PRIORITY: 10",
                                "DETAILS:",
                                "Исправить и проверить.",
                                "RESULT:",
                                "Исправил баг и проверил результат.",
                            ]
                        ),
                        "session-5",
                    ),
                ]
            )
            bot = _FakeBot()
            worker = AutonomyWorker(settings, queue_store, autonomy_store, bot, runner, stop_event)

            try:
                queue_store.note_chat_activity(101)
                autonomy_store.enqueue_task(chat_id=101, title="Починить автономный баг", kind="maintenance")

                await worker._run_once()

                done = autonomy_store.list_tasks(chat_id=101, statuses={"done"})
                pending = autonomy_store.list_tasks(chat_id=101, statuses={"pending"})
                self.assertEqual(len(done), 1)
                self.assertEqual(len(pending), 0)
                self.assertIn("Нашёл корень проблемы.", done[0].result_text)
                self.assertIn("Исправил баг и проверил результат.", done[0].result_text)
                self.assertEqual(len(runner.calls), 3)
            finally:
                queue_store.close()
                autonomy_store.close()

    async def test_run_once_rejects_micro_followup_and_finishes_in_same_session(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            settings = _make_settings(root)
            queue_store = QueueStore(root / "state.db")
            autonomy_store = AutonomyStore(root / "state.db")
            stop_event = asyncio.Event()
            runner = _SequenceRunner(
                [
                    CodexRunResult(
                        True,
                        "\n".join(
                            [
                                "ACTION: STEP",
                                "TITLE: Подготовить owner-facing сводку",
                                "KIND: project",
                                "PRIORITY: 20",
                                "DETAILS:",
                                "Сделать основной шаг по сводке.",
                                "RESULT:",
                                "Сделал основной шаг по сводке.",
                                "MISSION_STATUS: follow_up_later",
                                "WHY_NOT_DONE_NOW: Остался маленький косметический хвост.",
                                "BLOCKER_TYPE: none",
                                "GOAL_CHECK: Основная часть миссии уже закрыта.",
                                "PROGRESS_DELTA: Готова почти вся сводка.",
                                "DRIFT_RISK: Есть риск зря дробить хвост.",
                                "WHY_NOT_FINISHED_NOW: Остался очень маленький хвост.",
                                "NEXT_STEP_JUSTIFICATION: Хочу вынести косметику в отдельный wake-up.",
                                "",
                                "[[autonomy-next]]",
                                "ACTION: ENQUEUE",
                                "TITLE: Докрутить крошечный хвост",
                                "KIND: project",
                                "PRIORITY: 30",
                                "DELAY_SEC: 120",
                                "DETAILS:",
                                "Переименовать одну строку и вернуться позже.",
                                "[[/autonomy-next]]",
                            ]
                        ),
                        "session-micro",
                    ),
                    CodexRunResult(
                        True,
                        "\n".join(
                            [
                                "VERDICT: REJECT_AS_MICROSTEP",
                                "REASON: Этот хвост нужно дожать сейчас в том же сеансе.",
                            ]
                        ),
                        "session-micro",
                    ),
                    CodexRunResult(
                        True,
                        "\n".join(
                            [
                                "ACTION: STEP",
                                "TITLE: Закрыть owner-facing сводку",
                                "KIND: project",
                                "PRIORITY: 20",
                                "DETAILS:",
                                "Дожать косметический хвост и проверить итог.",
                                "RESULT:",
                                "Дожал хвост и закрыл owner-facing сводку без нового wake-up.",
                                "MISSION_STATUS: complete",
                                "WHY_NOT_DONE_NOW: Всё завершено.",
                                "BLOCKER_TYPE: none",
                                "GOAL_CHECK: Миссия полностью закрыта.",
                                "PROGRESS_DELTA: Хвост устранён.",
                                "DRIFT_RISK: Низкий.",
                                "WHY_NOT_FINISHED_NOW: completed now",
                                "NEXT_STEP_JUSTIFICATION: no follow-up needed",
                            ]
                        ),
                        "session-micro",
                    ),
                ]
            )
            bot = _FakeBot()
            worker = AutonomyWorker(settings, queue_store, autonomy_store, bot, runner, stop_event)

            try:
                queue_store.note_chat_activity(101)
                autonomy_store.enqueue_task(chat_id=101, title="Собрать owner-facing сводку", kind="project")

                await worker._run_once()

                done = autonomy_store.list_tasks(chat_id=101, statuses={"done"})
                pending = autonomy_store.list_tasks(chat_id=101, statuses={"pending"})
                self.assertEqual(len(done), 1)
                self.assertEqual(len(pending), 0)
                self.assertIn("Дожал хвост", done[0].result_text)
                self.assertEqual(len(runner.calls), 3)
            finally:
                queue_store.close()
                autonomy_store.close()

    async def test_run_once_deduplicates_same_autonomy_notification(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            settings = _make_settings(root)
            settings = Settings(
                **{
                    **settings.__dict__,
                    "autonomy_notify_enabled": True,
                    "autonomy_notify_min_chars": 1,
                    "autonomy_notify_cooldown_sec": 0,
                }
            )
            queue_store = QueueStore(root / "state.db")
            autonomy_store = AutonomyStore(root / "state.db")
            stop_event = asyncio.Event()
            runner = _FakeRunner(
                CodexRunResult(
                    True,
                    "Собрал одинаковый короткий автономный итог.",
                    "session-2",
                )
            )
            bot = _FakeBot()
            worker = AutonomyWorker(settings, queue_store, autonomy_store, bot, runner, stop_event)

            try:
                queue_store.note_chat_activity(101)
                autonomy_store.enqueue_task(chat_id=101, title="Шаг 1")
                autonomy_store.enqueue_task(chat_id=101, title="Шаг 2")

                await worker._run_once()
                await worker._run_once()

                self.assertEqual(len(bot.messages), 1)
            finally:
                queue_store.close()
                autonomy_store.close()

    async def test_run_once_reschedules_same_task_from_autonomy_control_block(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            settings = _make_settings(root)
            queue_store = QueueStore(root / "state.db")
            autonomy_store = AutonomyStore(root / "state.db")
            stop_event = asyncio.Event()
            runner = _FakeRunner(
                CodexRunResult(
                    True,
                    "\n".join(
                        [
                            "Сделал первый шаг и подготовил основу.",
                            "",
                            "[[autonomy-next]]",
                            "ACTION: ENQUEUE",
                            "TITLE: Продолжить короткое исследование",
                            "KIND: research",
                            "PRIORITY: 35",
                            "DELAY_SEC: 120",
                            "DETAILS:",
                            "Проверить еще один источник и сверить вывод.",
                            "[[/autonomy-next]]",
                        ]
                    ),
                    "session-3",
                )
            )
            bot = _FakeBot()
            worker = AutonomyWorker(settings, queue_store, autonomy_store, bot, runner, stop_event)

            try:
                queue_store.note_chat_activity(101)
                autonomy_store.enqueue_task(chat_id=101, title="Первый шаг", kind="research")

                await worker._run_once()

                done = autonomy_store.list_tasks(chat_id=101, statuses={"done"})
                pending = autonomy_store.list_tasks(chat_id=101, statuses={"pending"})
                self.assertEqual(len(done), 0)
                self.assertEqual(len(pending), 1)
                self.assertEqual(pending[0].source, "assistant")
                self.assertIsNone(pending[0].parent_task_id)
                self.assertEqual(pending[0].title, "Продолжить короткое исследование")
                self.assertIn("Сделал первый шаг и подготовил основу.", pending[0].result_text)
                self.assertEqual(
                    pending[0].details,
                    "Проверить еще один источник и сверить вывод.",
                )
                journal = (
                    root
                    / "system"
                    / "tasks"
                    / "autonomy_journal"
                    / datetime.now(ZoneInfo("Europe/Moscow")).strftime("%Y-%m-%d.md")
                )
                self.assertTrue(journal.exists())
                journal_text = journal.read_text(encoding="utf-8")
                self.assertIn("· continued", journal_text)
                self.assertIn("Продолжить короткое исследование", journal_text)
            finally:
                queue_store.close()
                autonomy_store.close()

    async def test_run_once_suppresses_followup_when_continuation_limit_reached(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            settings = _make_settings(root)
            settings = Settings(
                **{
                    **settings.__dict__,
                    "autonomy_max_task_continuations": 2,
                }
            )
            queue_store = QueueStore(root / "state.db")
            autonomy_store = AutonomyStore(root / "state.db")
            stop_event = asyncio.Event()
            runner = _FakeRunner(
                CodexRunResult(
                    True,
                    "\n".join(
                        [
                            "Сделал крупный кусок, но модель всё ещё пытается поставить ещё один follow-up.",
                            "",
                            "[[autonomy-next]]",
                            "ACTION: ENQUEUE",
                            "TITLE: Ещё один мелкий хвост",
                            "KIND: project",
                            "PRIORITY: 20",
                            "DELAY_SEC: 60",
                            "DETAILS:",
                            "Крошечное продолжение, которого уже не должно быть.",
                            "[[/autonomy-next]]",
                        ]
                    ),
                    "session-6",
                )
            )
            bot = _FakeBot()
            worker = AutonomyWorker(settings, queue_store, autonomy_store, bot, runner, stop_event)

            try:
                queue_store.note_chat_activity(101)
                task_id = autonomy_store.enqueue_task(chat_id=101, title="Большая линия", kind="project")
                autonomy_store.claim_next_ready_task(chat_id=101)
                autonomy_store.continue_task(
                    task_id,
                    title="Большая линия",
                    details="Первое продолжение",
                    kind="project",
                    priority=30,
                    scheduled_for="2026-03-06T12:00:00+00:00",
                    progress_text="Первый проход завершён.",
                )
                autonomy_store.claim_next_ready_task(chat_id=101, now="2026-03-06T12:00:01+00:00")
                autonomy_store.continue_task(
                    task_id,
                    title="Большая линия",
                    details="Второе продолжение",
                    kind="project",
                    priority=30,
                    scheduled_for="2026-03-06T12:01:00+00:00",
                    progress_text="Второй проход завершён.",
                )

                await worker._run_once()

                pending = autonomy_store.list_tasks(chat_id=101, statuses={"pending"})
                done = autonomy_store.list_tasks(chat_id=101, statuses={"done"})
                self.assertEqual(len(pending), 0)
                self.assertEqual(len(done), 1)
                self.assertIn("autonomy-followup-suppressed", done[0].result_text)
                journal = (
                    root
                    / "system"
                    / "tasks"
                    / "autonomy_journal"
                    / datetime.now(ZoneInfo("Europe/Moscow")).strftime("%Y-%m-%d.md")
                )
                self.assertTrue(journal.exists())
                self.assertIn("· completed", journal.read_text(encoding="utf-8"))
            finally:
                queue_store.close()
                autonomy_store.close()

    async def test_run_once_does_not_enqueue_followup_after_user_interrupt(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            settings = _make_settings(root)
            queue_store = QueueStore(root / "state.db")
            autonomy_store = AutonomyStore(root / "state.db")
            stop_event = asyncio.Event()
            bot = _FakeBot()

            class _InterruptingRunner:
                def __init__(self) -> None:
                    self.calls: list[tuple[str, str]] = []

                def run(self, prompt: str, session_id: str = "") -> CodexRunResult:
                    self.calls.append((prompt, session_id))
                    queue_store.enqueue_task(
                        chat_id=101,
                        user_id=1,
                        username="tester",
                        text="прерываю",
                        attachments=[],
                    )
                    return CodexRunResult(
                        True,
                        "\n".join(
                            [
                                "Сделал шаг, но пользователь уже написал.",
                                "",
                                "[[autonomy-next]]",
                                "ACTION: ENQUEUE",
                                "TITLE: Не должно появиться",
                                "KIND: research",
                                "PRIORITY: 20",
                                "DELAY_SEC: 60",
                                "DETAILS:",
                                "Этот follow-up должен быть пропущен.",
                                "[[/autonomy-next]]",
                            ]
                        ),
                        "session-4",
                    )

            runner = _InterruptingRunner()
            worker = AutonomyWorker(settings, queue_store, autonomy_store, bot, runner, stop_event)

            try:
                queue_store.note_chat_activity(101)
                autonomy_store.enqueue_task(chat_id=101, title="Шаг", kind="research")

                await worker._run_once()

                pending = autonomy_store.list_tasks(chat_id=101, statuses={"pending"})
                self.assertEqual(len(pending), 0)
                done = autonomy_store.list_tasks(chat_id=101, statuses={"done"})
                self.assertEqual(len(done), 1)
                self.assertIn("autonomy-paused", done[0].result_text)
            finally:
                queue_store.close()
                autonomy_store.close()

    async def test_owner_request_can_start_staged_mission_plan(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            settings = _make_settings(root)
            queue_store = QueueStore(root / "state.db")
            autonomy_store = AutonomyStore(root / "state.db")
            stop_event = asyncio.Event()
            runner = _SequenceRunner(
                [
                    CodexRunResult(
                        True,
                        "\n".join(
                            [
                                "ACTION: STEP",
                                "TITLE: Собрать материалы",
                                "KIND: research",
                                "PRIORITY: 20",
                                "DETAILS:",
                                "Составить план работы и открыть первый этап.",
                                "RESULT:",
                                "План миссии собран, первый этап открыт.",
                                "PLAN_MODE: staged",
                                "ROOT_OBJECTIVE: Собрать методичку по теме",
                                "SUCCESS_CRITERIA: Есть готовый документ с вычитанными разделами.",
                                "CURRENT_STAGE: Сбор материалов",
                                "NEXT_STAGE: Черновик",
                                "MISSION_STATUS: follow_up_later",
                                "STAGE_STATUS: continue_stage",
                                "CHECKPOINT_SUMMARY: План готов и первый этап активен.",
                                "WHY_NOT_DONE_NOW: Дальше уже отдельный этап работы.",
                                "BLOCKER_TYPE: none",
                                "GOAL_CHECK: Это создаёт внятную рамку миссии.",
                                "PROGRESS_DELTA: Этапы определены.",
                                "DRIFT_RISK: Низкий.",
                                "WHY_NOT_FINISHED_NOW: Остались этапы выполнения.",
                                "NEXT_STEP_JUSTIFICATION: Нужно продолжить текущий этап.",
                                "",
                                "[[mission-plan]]",
                                "### Сбор материалов",
                                "goal: собрать опорные источники",
                                "done_when: есть 5 сильных источников",
                                "status: active",
                                "completion_summary: ",
                                "### Черновик",
                                "goal: написать основной текст",
                                "done_when: готов первый черновик",
                                "status: pending",
                                "completion_summary: ",
                                "### Вычитка",
                                "goal: проверить цельность текста",
                                "done_when: финальная версия вычитана",
                                "status: pending",
                                "completion_summary: ",
                                "[[/mission-plan]]",
                            ]
                        ),
                        "session-plan",
                    ),
                    CodexRunResult(
                        True,
                        "VERDICT: APPROVE_FOLLOWUP\nREASON: План создан, продолжение оправдано на следующем wake-up.",
                        "session-plan",
                    ),
                ]
            )
            bot = _FakeBot()
            worker = AutonomyWorker(settings, queue_store, autonomy_store, bot, runner, stop_event)

            try:
                queue_store.note_chat_activity(101)
                self._write_active_request(root, "Собрать методичку по теме")

                await worker._run_once()

                mission = autonomy_store.get_live_mission(101, source="owner_request")
                self.assertIsNotNone(mission)
                assert mission is not None
                self.assertEqual(mission.plan_state, "staged")
                self.assertEqual(len(mission.plan_json), 3)
                self.assertEqual(mission.current_stage_index, 0)
                self.assertEqual(mission.plan_json[0]["title"], "Сбор материалов")
                pending = autonomy_store.list_tasks(chat_id=101, statuses={"pending"})
                self.assertEqual(len(pending), 1)
                self.assertIn("Сбор материалов", pending[0].title)
            finally:
                queue_store.close()
                autonomy_store.close()

    async def test_force_stage_done_advances_to_next_stage(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            settings = _make_settings(root, autonomy_session_step_limit=1)
            queue_store = QueueStore(root / "state.db")
            autonomy_store = AutonomyStore(root / "state.db")
            stop_event = asyncio.Event()
            runner = _SequenceRunner(
                [
                    CodexRunResult(
                        True,
                        "\n".join(
                            [
                                "ACTION: STEP",
                                "TITLE: Сбор материалов",
                                "KIND: research",
                                "PRIORITY: 20",
                                "DETAILS:",
                                "Дособрать и зафиксировать материалы.",
                                "RESULT:",
                                "Материалы уже собраны, остался только косметический хвост.",
                                "PLAN_MODE: staged",
                                "ROOT_OBJECTIVE: Собрать методичку по теме",
                                "SUCCESS_CRITERIA: Есть готовый документ с вычитанными разделами.",
                                "CURRENT_STAGE: Сбор материалов",
                                "NEXT_STAGE: Черновик",
                                "MISSION_STATUS: follow_up_later",
                                "STAGE_STATUS: continue_stage",
                                "CHECKPOINT_SUMMARY: Материалы фактически готовы.",
                                "WHY_NOT_DONE_NOW: Хочу ещё один маленький хвост.",
                                "BLOCKER_TYPE: none",
                                "GOAL_CHECK: Это завершает первый этап.",
                                "PROGRESS_DELTA: Все материалы уже на месте.",
                                "DRIFT_RISK: Есть риск микродробления.",
                                "WHY_NOT_FINISHED_NOW: Остался следующий этап.",
                                "NEXT_STEP_JUSTIFICATION: Хочу отложить маленький хвост.",
                                "",
                                "[[mission-plan]]",
                                "### Сбор материалов",
                                "goal: собрать опорные источники",
                                "done_when: есть 5 сильных источников",
                                "status: active",
                                "completion_summary: ",
                                "### Черновик",
                                "goal: написать основной текст",
                                "done_when: готов первый черновик",
                                "status: pending",
                                "completion_summary: ",
                                "[[/mission-plan]]",
                                "",
                                "[[autonomy-next]]",
                                "ACTION: ENQUEUE",
                                "TITLE: Ещё один мелкий хвост",
                                "KIND: research",
                                "PRIORITY: 20",
                                "DELAY_SEC: 60",
                                "DETAILS:",
                                "Доделать косметический хвост позже.",
                                "[[/autonomy-next]]",
                            ]
                        ),
                        "session-stage",
                    ),
                    CodexRunResult(
                        True,
                        "VERDICT: FORCE_STAGE_DONE\nREASON: Этап уже закрыт, хвост слишком мелкий.",
                        "session-stage",
                    ),
                ]
            )
            bot = _FakeBot()
            worker = AutonomyWorker(settings, queue_store, autonomy_store, bot, runner, stop_event)

            try:
                queue_store.note_chat_activity(101)
                self._write_active_request(root, "Собрать методичку по теме")

                await worker._run_once()

                mission = autonomy_store.get_live_mission(101, source="owner_request")
                self.assertIsNotNone(mission)
                assert mission is not None
                self.assertEqual(mission.plan_state, "staged")
                self.assertEqual(mission.current_stage_index, 1)
                self.assertEqual(mission.plan_json[0]["status"], "done")
                self.assertIn("Материалы фактически готовы", mission.plan_json[0]["completion_summary"])
                pending = autonomy_store.list_tasks(chat_id=101, statuses={"pending"})
                self.assertEqual(len(pending), 1)
                self.assertIn("Черновик", pending[0].title)
            finally:
                queue_store.close()
                autonomy_store.close()
