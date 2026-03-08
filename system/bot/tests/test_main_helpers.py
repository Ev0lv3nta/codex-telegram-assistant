import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from system.bot.autonomy_store import AutonomyStore
from system.bot.config import Settings
from system.bot.main import (
    _allowed_update_types,
    _build_bot_commands,
    _build_pulse_keyboard,
    _delete_attachment_file,
    _enqueue_restart_success_task,
    _note_chat_activity_from_message,
    _note_passive_owner_touch,
    _render_codex_cli_status,
    _stop_autonomy_now,
    _render_autonomy_pulse,
    _schedule_autonomy_snooze,
    _nudge_autonomy_wakeup,
    _render_autonomy_status,
    _transcribe_voice_if_needed,
    _wake_autonomy_now,
)
from system.bot.queue_store import QueueStore
from system.bot.stt_openrouter import SttResult


def _make_settings(root: Path) -> Settings:
    return Settings(
        assistant_root=root,
        telegram_token="x",
        allowed_user_ids=set(),
        allowed_chat_ids=set(),
        poll_timeout_sec=25,
        idle_sleep_sec=1.0,
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
    )


class _FakeSttClient:
    def __init__(self, result: SttResult) -> None:
        self._result = result
        self.calls = 0

    def transcribe_file(self, _path: Path, _duration_sec: int = 0) -> SttResult:
        self.calls += 1
        return self._result


class MainHelpersTests(unittest.IsolatedAsyncioTestCase):
    async def test_transcribe_merges_transcript_with_existing_text(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            audio = root / "88_files" / "voice.oga"
            audio.parent.mkdir(parents=True, exist_ok=True)
            audio.write_bytes(b"audio")

            settings = _make_settings(root)
            message = SimpleNamespace(voice=object(), audio=None)
            stt_client = _FakeSttClient(SttResult(True, "привет мир", ""))

            text, error, rel_path = await _transcribe_voice_if_needed(
                message=message,
                settings=settings,
                stt_client=stt_client,
                text="Проверка связи",
                attachments=["88_files/voice.oga"],
            )

            self.assertEqual(error, "")
            self.assertEqual(rel_path, "88_files/voice.oga")
            self.assertIn("Проверка связи", text)
            self.assertIn("[Расшифровка голосового]", text)
            self.assertIn("привет мир", text)
            self.assertEqual(stt_client.calls, 1)

    async def test_transcribe_failure_keeps_existing_text(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            audio = root / "88_files" / "voice.oga"
            audio.parent.mkdir(parents=True, exist_ok=True)
            audio.write_bytes(b"audio")

            settings = _make_settings(root)
            message = SimpleNamespace(voice=object(), audio=None)
            stt_client = _FakeSttClient(SttResult(False, "", "STT down"))

            text, error, rel_path = await _transcribe_voice_if_needed(
                message=message,
                settings=settings,
                stt_client=stt_client,
                text="Текст есть",
                attachments=["88_files/voice.oga"],
            )

            self.assertEqual(text, "Текст есть")
            self.assertEqual(rel_path, "88_files/voice.oga")
            self.assertIn("Голосовое не обработано", error)
            self.assertIn("STT down", error)
            self.assertEqual(stt_client.calls, 1)

    async def test_transcribe_failure_without_text_returns_error(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            audio = root / "88_files" / "voice.oga"
            audio.parent.mkdir(parents=True, exist_ok=True)
            audio.write_bytes(b"audio")

            settings = _make_settings(root)
            message = SimpleNamespace(voice=object(), audio=None)
            stt_client = _FakeSttClient(SttResult(False, "", "boom"))

            text, error, rel_path = await _transcribe_voice_if_needed(
                message=message,
                settings=settings,
                stt_client=stt_client,
                text="",
                attachments=["88_files/voice.oga"],
            )

            self.assertEqual(text, "")
            self.assertEqual(rel_path, "88_files/voice.oga")
            self.assertIn("Голосовое не обработано", error)
            self.assertIn("boom", error)
            self.assertEqual(stt_client.calls, 1)


class FileCleanupTests(unittest.TestCase):
    def test_delete_attachment_file_removes_existing_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            target = root / "88_files" / "voice.oga"
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(b"audio")

            _delete_attachment_file(root, "88_files/voice.oga")
            self.assertFalse(target.exists())


class ChatActivityTests(unittest.TestCase):
    def test_note_chat_activity_from_command_message_updates_last_active_chat(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = QueueStore(root / "state.db")
            try:
                message = SimpleNamespace(chat=SimpleNamespace(id=202))
                _note_chat_activity_from_message(store, message)
                self.assertEqual(store.get_last_active_chat_id(), 202)
            finally:
                store.close()

    def test_nudge_autonomy_wakeup_sets_next_wakeup_and_event(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = AutonomyStore(root / "state.db")
            wake_event = SimpleNamespace(set_called=False)

            class _WakeEvent:
                def set(self_inner) -> None:
                    wake_event.set_called = True

            try:
                message = SimpleNamespace(chat=SimpleNamespace(id=202))
                _nudge_autonomy_wakeup(store, message, _WakeEvent())
                self.assertTrue(wake_event.set_called)
                self.assertTrue(store.get_next_wakeup(202))
            finally:
                store.close()

    def test_nudge_autonomy_wakeup_is_noop_when_autonomy_stopped(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = AutonomyStore(root / "state.db")
            wake_event = SimpleNamespace(set_called=False)

            class _WakeEvent:
                def set(self_inner) -> None:
                    wake_event.set_called = True

            try:
                store.set_autonomy_paused(202, True)
                message = SimpleNamespace(chat=SimpleNamespace(id=202))
                _nudge_autonomy_wakeup(store, message, _WakeEvent())
                self.assertFalse(wake_event.set_called)
                self.assertEqual(store.get_next_wakeup(202), "")
            finally:
                store.close()

    def test_note_passive_owner_touch_does_not_change_next_wakeup(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            queue_store = QueueStore(root / "queue.db")
            autonomy_store = AutonomyStore(root / "state.db")
            try:
                autonomy_store.set_next_wakeup(202, "2026-03-08T00:10:00+00:00")
                message = SimpleNamespace(chat=SimpleNamespace(id=202))
                _note_passive_owner_touch(queue_store, message)
                self.assertEqual(queue_store.get_last_active_chat_id(), 202)
                self.assertEqual(
                    autonomy_store.get_next_wakeup(202),
                    "2026-03-08T00:10:00+00:00",
                )
            finally:
                queue_store.close()
                autonomy_store.close()

    def test_schedule_autonomy_snooze_sets_sleeping_idle_state(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = AutonomyStore(root / "state.db")
            try:
                until = _schedule_autonomy_snooze(store, 202, hours=6)
                self.assertEqual(store.get_idle_snooze_until(202), until)
                self.assertEqual(store.get_next_wakeup(202), until)
                self.assertEqual(store.get_mode(202), "sleeping_idle")
            finally:
                store.close()

    def test_stop_and_wake_autonomy_toggle_pause_state(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = AutonomyStore(root / "state.db")
            try:
                _stop_autonomy_now(store, 202)
                self.assertTrue(store.autonomy_paused(202))
                self.assertEqual(store.get_mode(202), "stopped")
                self.assertEqual(store.get_next_wakeup(202), "")

                _wake_autonomy_now(store, 202)
                self.assertFalse(store.autonomy_paused(202))
                self.assertEqual(store.get_mode(202), "idle")
                self.assertTrue(store.get_next_wakeup(202))
            finally:
                store.close()

    def test_enqueue_restart_success_task_creates_system_task(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = QueueStore(root / "state.db")
            try:
                task_id = _enqueue_restart_success_task(store, 202, "demo.service")
                self.assertGreater(task_id, 0)
                task = store.claim_next_task()
                assert task is not None
                self.assertEqual(task.chat_id, 202)
                self.assertEqual(task.user_id, 0)
                self.assertEqual(task.username, "system")
                self.assertIn("self-restart `demo.service`", task.text)
            finally:
                store.close()


class AutonomyStatusRenderTests(unittest.TestCase):
    def test_build_bot_commands_includes_pulse(self) -> None:
        commands = _build_bot_commands()
        self.assertEqual(
            [item.command for item in commands],
            ["start", "pulse", "status", "codexstatus", "autonomy", "restart"],
        )

    def test_render_codex_cli_status_reads_local_limits(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            codex_home = Path(td)
            (codex_home / "sessions" / "2026" / "03" / "08").mkdir(parents=True, exist_ok=True)
            (codex_home / "version.json").write_text('{"version":"0.111.0"}', encoding="utf-8")
            (codex_home / "config.toml").write_text(
                'model = "gpt-5.4"\nmodel_reasoning_effort = "high"\n',
                encoding="utf-8",
            )
            session_file = codex_home / "sessions" / "2026" / "03" / "08" / "rollout-test.jsonl"
            session_file.write_text(
                "\n".join(
                    [
                        '{"timestamp":"2026-03-08T10:00:00Z","payload":{"type":"token_count","rate_limits":{"primary":{"used_percent":20,"resets_at":1773000000},"secondary":{"used_percent":35,"resets_at":1773600000},"plan_type":"plus"}}}',
                    ]
                ),
                encoding="utf-8",
            )

            text = _render_codex_cli_status(codex_home)

            self.assertIn("version: 0.111.0", text)
            self.assertIn("model: gpt-5.4", text)
            self.assertIn("reasoning: high", text)
            self.assertIn("5h limit: 80% left", text)
            self.assertIn("weekly limit: 65% left", text)
            self.assertIn("plan: plus", text)

    def test_render_codex_cli_status_prefers_requested_chat_session_and_context_left(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            codex_home = Path(td)
            target_session = "019c8920-41f4-75a3-aaac-63d21c697f8e"
            (codex_home / "sessions" / "2026" / "03" / "08").mkdir(parents=True, exist_ok=True)
            (codex_home / "config.toml").write_text(
                'model = "gpt-5.4"\nmodel_reasoning_effort = "medium"\n',
                encoding="utf-8",
            )
            session_file = (
                codex_home / "sessions" / "2026" / "03" / "08" / f"rollout-test-{target_session}.jsonl"
            )
            session_file.write_text(
                "\n".join(
                    [
                        '{"timestamp":"2026-03-08T10:00:00Z","type":"session_meta","payload":{"id":"019c8920-41f4-75a3-aaac-63d21c697f8e"}}',
                        '{"timestamp":"2026-03-08T10:00:01Z","type":"event_msg","payload":{"type":"task_started","model_context_window":2000}}',
                        '{"timestamp":"2026-03-08T10:00:02Z","type":"turn_context","payload":{"model":"gpt-5.4","effort":"high"}}',
                        '{"timestamp":"2026-03-08T10:00:03Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":500}},"rate_limits":{"primary":{"used_percent":10,"resets_at":1773000000},"secondary":{"used_percent":25,"resets_at":1773600000},"plan_type":"plus"}}}',
                    ]
                ),
                encoding="utf-8",
            )

            text = _render_codex_cli_status(
                chat_session_id=target_session,
                codex_home=codex_home,
            )

            self.assertIn(f"chat session: {target_session}", text)
            self.assertIn("session file: rollout-test-", text)
            self.assertIn("reasoning: high", text)
            self.assertIn("context left: 75% left", text)
            self.assertIn("5h limit: 90% left", text)

    def test_allowed_update_types_include_callback_query(self) -> None:
        self.assertEqual(_allowed_update_types(), ["message", "callback_query"])

    def test_build_pulse_keyboard_has_refresh_button(self) -> None:
        markup = _build_pulse_keyboard()
        self.assertEqual(len(markup.inline_keyboard), 4)
        refresh_button = markup.inline_keyboard[0][0]
        self.assertEqual(refresh_button.text, "Обновить pulse")
        self.assertEqual(refresh_button.callback_data, "autonomy:pulse")
        snooze_button = markup.inline_keyboard[1][0]
        wake_button = markup.inline_keyboard[2][0]
        stop_button = markup.inline_keyboard[3][0]
        self.assertEqual(snooze_button.text, "Пауза 6ч")
        self.assertEqual(snooze_button.callback_data, "autonomy:pulse:snooze:6h")
        self.assertEqual(wake_button.text, "Разбудить сейчас")
        self.assertEqual(wake_button.callback_data, "autonomy:pulse:wake:now")
        self.assertEqual(stop_button.text, "Остановить автономность")
        self.assertEqual(stop_button.callback_data, "autonomy:pulse:stop")

    def test_build_pulse_keyboard_shows_start_when_stopped(self) -> None:
        markup = _build_pulse_keyboard(stopped=True)
        self.assertEqual(len(markup.inline_keyboard), 2)
        start_button = markup.inline_keyboard[1][0]
        self.assertEqual(start_button.text, "Запустить автономность")
        self.assertEqual(start_button.callback_data, "autonomy:pulse:start")

    def test_render_autonomy_status_includes_meta_and_followup_chain(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = AutonomyStore(Path(td) / "state.db")
            try:
                parent_id = store.enqueue_task(chat_id=101, title="Первый шаг", kind="research")
                store.enqueue_task(
                    chat_id=101,
                    title="Второй шаг",
                    kind="research",
                    source="followup",
                    parent_task_id=parent_id,
                )
                store.mark_heartbeat("loop", "2026-03-06T20:00:00+00:00")
                store.mark_heartbeat("planned", "2026-03-06T20:00:05+00:00")
                store.mark_notify_sent(101, "2026-03-06T18:00:00+00:00")

                text = _render_autonomy_status(store, 101, 1800)

                self.assertIn("heartbeat:", text)
                self.assertIn("last heartbeat status:", text)
                self.assertIn("next heartbeat:", text)
                self.assertIn("last notify:", text)
                self.assertIn("src=followup", text)
                self.assertIn(f"parent={parent_id}", text)
            finally:
                store.close()

    def test_render_autonomy_pulse_is_short_and_owner_facing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = AutonomyStore(Path(td) / "state.db")
            try:
                store.set_mode(101, "sleeping_scheduled")
                store.set_next_wakeup(101, "2026-03-08T00:10:00+00:00")
                store.set_active_mission(
                    101,
                    task_id=7,
                    title="Собрать owner-facing слой состояния",
                    details="Один спокойный шаг",
                    kind="project",
                    source="assistant",
                    phase="running",
                )
                store.enqueue_task(
                    chat_id=101,
                    title="Продолжить owner-facing слой состояния",
                    kind="project",
                    scheduled_for="2026-03-08T00:10:00+00:00",
                )

                text = _render_autonomy_pulse(store, 101)

                self.assertIn("Пульс автономности:", text)
                self.assertIn("режим: sleeping_scheduled", text)
                self.assertIn("следующий wake-up: 08.03 03:10 MSK", text)
                self.assertIn("текущая линия: Собрать owner-facing слой состояния", text)
                self.assertIn("есть запланированное продолжение", text)
                self.assertNotIn("src=", text)
                self.assertNotIn("parent=", text)
                self.assertNotIn("2026-03-08T00:10:00+00:00", text)
            finally:
                store.close()

    def test_render_autonomy_pulse_uses_next_pending_step_when_no_active_mission(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = AutonomyStore(Path(td) / "state.db")
            try:
                store.set_mode(101, "sleeping_scheduled")
                store.set_next_wakeup(101, "2026-03-08T00:15:00+00:00")
                store.enqueue_task(
                    chat_id=101,
                    title="Проверить живой pulse после рестарта",
                    kind="project",
                    scheduled_for="2026-03-08T00:15:00+00:00",
                )

                text = _render_autonomy_pulse(store, 101)

                self.assertIn("следующий шаг: Проверить живой pulse после рестарта", text)
                self.assertNotIn("текущая линия: (нет активной миссии)", text)
            finally:
                store.close()

    def test_render_autonomy_pulse_uses_scheduled_active_mission_as_next_step(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = AutonomyStore(Path(td) / "state.db")
            try:
                store.set_mode(101, "sleeping_scheduled")
                store.set_next_wakeup(101, "2026-03-08T00:15:00+00:00")
                store.set_active_mission(
                    101,
                    task_id=7,
                    title="Продолжить реализацию active mission",
                    details="Вернуться после паузы",
                    kind="project",
                    source="assistant",
                    phase="scheduled",
                    scheduled_for="2026-03-08T00:15:00+00:00",
                )

                text = _render_autonomy_pulse(store, 101)

                self.assertIn("следующий шаг: Продолжить реализацию active mission", text)
                self.assertNotIn("текущая линия: Продолжить реализацию active mission", text)
            finally:
                store.close()

    def test_render_autonomy_pulse_shows_stopped_state(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = AutonomyStore(Path(td) / "state.db")
            try:
                store.set_autonomy_paused(101, True)
                store.set_mode(101, "stopped")

                text = _render_autonomy_pulse(store, 101)

                self.assertIn("режим: stopped", text)
                self.assertIn("следующий wake-up: (остановлен)", text)
                self.assertIn("автономный контур остановлен", text)
            finally:
                store.close()

    def test_render_autonomy_pulse_shows_snoozed_until_for_sleeping_idle(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = AutonomyStore(Path(td) / "state.db")
            try:
                store.set_mode(101, "sleeping_idle")
                store.set_next_wakeup(101, "2026-03-08T03:00:00+00:00")
                store.mark_idle_snooze_until(101, "2026-03-08T03:00:00+00:00")

                text = _render_autonomy_pulse(store, 101)

                self.assertIn("статус: притушен до 08.03 06:00 MSK", text)
                self.assertNotIn("явного автономного хвоста сейчас нет", text)
            finally:
                store.close()

    def test_render_autonomy_pulse_prefers_waiting_phase_status(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = AutonomyStore(Path(td) / "state.db")
            try:
                store.set_mode(101, "waiting_user")
                store.set_active_mission(
                    101,
                    task_id=9,
                    title="Уточнить следующий шаг у владельца",
                    details="Нужно дождаться ответа",
                    kind="project",
                    source="assistant",
                    phase="waiting_user",
                )

                text = _render_autonomy_pulse(store, 101)

                self.assertIn("текущая линия: Уточнить следующий шаг у владельца", text)
                self.assertIn("статус: ждёт ответа владельца", text)
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
