import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock
import urllib.error

from system.bot.config import Settings
from system.bot.stt_openrouter import (
    OpenRouterSttClient,
    _extract_message_text,
    _extract_transcript_from_json,
    _guess_audio_format,
    _looks_suspicious_transcript,
)


def _make_settings(root: Path, api_key: str) -> Settings:
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
        openrouter_api_key=api_key,
        openrouter_base_url="https://openrouter.ai/api/v1",
        openrouter_stt_model="mistralai/voxtral-small-24b-2507",
        openrouter_stt_timeout_sec=30,
        openrouter_stt_max_audio_bytes=1024 * 1024,
        state_db_path=root / "state.db",
        log_level="INFO",
    )


class _DummyResponse:
    def __init__(self, payload: dict) -> None:
        self._raw = json.dumps(payload).encode("utf-8")

    def __enter__(self) -> "_DummyResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def read(self) -> bytes:
        return self._raw


class OpenRouterSttTests(unittest.TestCase):
    def test_guess_audio_format(self) -> None:
        self.assertEqual(_guess_audio_format(Path("voice.ogg")), "ogg")
        self.assertEqual(_guess_audio_format(Path("voice.oga")), "ogg")
        self.assertEqual(_guess_audio_format(Path("voice.opus")), "ogg")
        self.assertEqual(_guess_audio_format(Path("voice.wav")), "wav")
        self.assertEqual(_guess_audio_format(Path("voice.unknown")), "")

    def test_extract_message_text_from_list_content(self) -> None:
        content = [
            {"type": "output_text", "text": "line1"},
            {"type": "output_text", "text": "line2"},
        ]
        self.assertEqual(_extract_message_text(content), "line1\nline2")

    def test_extract_transcript_from_json(self) -> None:
        self.assertEqual(
            _extract_transcript_from_json('{"transcript":"привет мир"}'),
            "привет мир",
        )
        self.assertEqual(
            _extract_transcript_from_json('```json\n{"transcript":"ok"}\n```'),
            "ok",
        )
        self.assertEqual(_extract_transcript_from_json("not json"), "")

    def test_looks_suspicious_transcript(self) -> None:
        self.assertFalse(_looks_suspicious_transcript("нормальная расшифровка текста", 0))
        self.assertTrue(_looks_suspicious_transcript("1 2 3 4 5", 15))
        self.assertFalse(_looks_suspicious_transcript("коротко", 3))

    def test_transcribe_file_fails_without_api_key(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            audio = root / "voice.ogg"
            audio.write_bytes(b"123")

            client = OpenRouterSttClient(_make_settings(root, api_key=""))
            result = client.transcribe_file(audio)
            self.assertFalse(result.success)
            self.assertIn("OPENROUTER_API_KEY", result.error)

    def test_transcribe_file_success(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            audio = root / "voice.ogg"
            audio.write_bytes(b"123")
            client = OpenRouterSttClient(_make_settings(root, api_key="test-key"))

            payload = {
                "choices": [
                    {
                        "message": {
                            "content": '{"transcript":"привет мир"}',
                        }
                    }
                ]
            }
            with mock.patch("urllib.request.urlopen", return_value=_DummyResponse(payload)):
                result = client.transcribe_file(audio)
            self.assertTrue(result.success)
            self.assertEqual(result.text, "привет мир")

    def test_transcribe_file_retries_without_response_format(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            audio = root / "voice.ogg"
            audio.write_bytes(b"123")
            client = OpenRouterSttClient(_make_settings(root, api_key="test-key"))

            bad_payload = {
                "choices": [
                    {
                        "message": {
                            "content": "1 2 3 4 5",
                        }
                    }
                ]
            }
            good_payload = {
                "choices": [
                    {
                        "message": {
                            "content": '{"transcript":"тестовая расшифровка"}',
                        }
                    }
                ]
            }

            with mock.patch(
                "urllib.request.urlopen",
                side_effect=[_DummyResponse(bad_payload), _DummyResponse(good_payload)],
            ):
                result = client.transcribe_file(audio)
            self.assertTrue(result.success)
            self.assertEqual(result.text, "тестовая расшифровка")

    def test_transcribe_file_rejects_non_transcript_response(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            audio = root / "voice.ogg"
            audio.write_bytes(b"123")
            client = OpenRouterSttClient(_make_settings(root, api_key="test-key"))

            payload = {
                "choices": [
                    {
                        "message": {
                            "content": "1 2 3 4 5",
                        }
                    }
                ]
            }
            with mock.patch(
                "urllib.request.urlopen",
                side_effect=[_DummyResponse(payload), _DummyResponse(payload)],
            ):
                result = client.transcribe_file(audio)
            self.assertFalse(result.success)
            self.assertIn("не в формате", result.error)

    def test_transcribe_file_rejects_suspiciously_short_transcript(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            audio = root / "voice.ogg"
            audio.write_bytes(b"123")
            client = OpenRouterSttClient(_make_settings(root, api_key="test-key"))

            payload = {
                "choices": [
                    {
                        "message": {
                            "content": '{"transcript":"1 2 3 4 5"}',
                        }
                    }
                ]
            }
            with mock.patch(
                "urllib.request.urlopen",
                side_effect=[_DummyResponse(payload), _DummyResponse(payload)],
            ):
                result = client.transcribe_file(audio, duration_sec=15)
            self.assertFalse(result.success)
            self.assertIn("подозрительно короткую", result.error)

    def test_transcribe_file_http_error(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            audio = root / "voice.ogg"
            audio.write_bytes(b"123")
            client = OpenRouterSttClient(_make_settings(root, api_key="test-key"))

            error_body = io.BytesIO(json.dumps({"error": {"message": "bad request"}}).encode("utf-8"))
            http_error = urllib.error.HTTPError(
                url="https://openrouter.ai/api/v1/chat/completions",
                code=400,
                msg="Bad Request",
                hdrs=None,
                fp=error_body,
            )

            with mock.patch("urllib.request.urlopen", side_effect=http_error):
                result = client.transcribe_file(audio)
            self.assertFalse(result.success)
            self.assertIn("HTTP 400", result.error)
            self.assertIn("bad request", result.error)


if __name__ == "__main__":
    unittest.main()
