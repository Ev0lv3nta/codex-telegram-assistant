import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from system.bot.autonomy_store import AutonomyStore
from system.bot.config import Settings
from system.bot.main import (
    _delete_attachment_file,
    _note_chat_activity_from_message,
    _render_autonomy_status,
    _transcribe_voice_if_needed,
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


class AutonomyStatusRenderTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
