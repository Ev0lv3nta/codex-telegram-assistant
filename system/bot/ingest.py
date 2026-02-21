from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import re

from .telegram_api import TelegramAPI


def _slug(text: str, fallback: str = "item", max_len: int = 64) -> str:
    ascii_text = (
        text.encode("ascii", errors="ignore").decode("ascii").strip().lower()
    )
    cleaned = re.sub(r"[^a-z0-9]+", "-", ascii_text).strip("-")
    if not cleaned:
        cleaned = fallback
    return cleaned[:max_len]


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _unique_path(base_dir: Path, filename: str) -> Path:
    candidate = base_dir / filename
    if not candidate.exists():
        return candidate
    stem = candidate.stem
    suffix = candidate.suffix
    for index in range(1, 1000):
        candidate = base_dir / f"{stem}-{index}{suffix}"
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"Failed to allocate unique path for {filename}")


def _message_datetime(message: dict) -> datetime:
    unix_ts = int(message.get("date", int(_now_utc().timestamp())))
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc)


def download_attachments(
    api: TelegramAPI,
    assistant_root: Path,
    message: dict,
) -> list[str]:
    result_paths: list[str] = []
    message_id = int(message.get("message_id", 0))
    timestamp = _message_datetime(message).strftime("%Y%m%d-%H%M%S")

    specs: list[tuple[str, str, str]] = []

    photos = message.get("photo") or []
    if photos:
        photo = photos[-1]
        specs.append((photo["file_id"], "89_images", f"photo-{message_id}.jpg"))

    document = message.get("document")
    if document:
        filename = document.get("file_name") or f"document-{message_id}"
        mime_type = (document.get("mime_type") or "").lower()
        target_dir = "89_images" if mime_type.startswith("image/") else "88_files"
        specs.append((document["file_id"], target_dir, filename))

    voice = message.get("voice")
    if voice:
        specs.append((voice["file_id"], "88_files", f"voice-{message_id}.ogg"))

    audio = message.get("audio")
    if audio:
        filename = audio.get("file_name") or f"audio-{message_id}.mp3"
        specs.append((audio["file_id"], "88_files", filename))

    video = message.get("video")
    if video:
        specs.append((video["file_id"], "88_files", f"video-{message_id}.mp4"))

    video_note = message.get("video_note")
    if video_note:
        specs.append((video_note["file_id"], "88_files", f"video-note-{message_id}.mp4"))

    for file_id, folder, hinted_name in specs:
        metadata = api.get_file(file_id)
        remote_path = metadata["file_path"]
        remote_suffix = Path(remote_path).suffix
        hint_stem = _slug(Path(hinted_name).stem, fallback="file")
        suffix = remote_suffix or Path(hinted_name).suffix or ".bin"
        filename = f"{timestamp}-{hint_stem}{suffix}"
        base_dir = assistant_root / folder
        base_dir.mkdir(parents=True, exist_ok=True)
        output_path = _unique_path(base_dir, filename)
        output_path.write_bytes(api.download_file(remote_path))
        result_paths.append(output_path.relative_to(assistant_root).as_posix())

    return result_paths


def write_inbox_markdown(
    assistant_root: Path,
    message: dict,
    text: str,
    mode: str,
    attachments: list[str],
) -> str:
    chat = message.get("chat") or {}
    sender = message.get("from") or {}
    message_id = int(message.get("message_id", 0))
    dt = _message_datetime(message)
    stamp = dt.strftime("%Y-%m-%d_%H%M%S")
    filename = f"{stamp}_tg-{chat.get('id', 0)}-{message_id}.md"
    inbox_path = assistant_root / "00_inbox" / filename
    inbox_path.parent.mkdir(parents=True, exist_ok=True)

    attachment_lines = "\n".join(f"- `{item}`" for item in attachments)
    if not attachment_lines:
        attachment_lines = "- (none)"

    body = f"""# Telegram Inbox Item

- received_at_utc: {dt.isoformat()}
- chat_id: {chat.get("id", 0)}
- chat_type: {chat.get("type", "unknown")}
- user_id: {sender.get("id", 0)}
- username: {sender.get("username", "")}
- message_id: {message_id}
- suggested_mode: {mode}

## Attachments
{attachment_lines}

## Text
{text if text.strip() else "(empty text)"}
"""
    inbox_path.write_text(body, encoding="utf-8")
    return inbox_path.relative_to(assistant_root).as_posix()

