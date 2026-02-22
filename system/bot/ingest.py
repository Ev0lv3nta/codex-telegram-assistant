from __future__ import annotations

from datetime import datetime, timezone
import io
from pathlib import Path
import re

from aiogram import Bot
from aiogram.types import Message


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


def _message_datetime(message: Message) -> datetime:
    dt = message.date
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


async def download_attachments(
    bot: Bot,
    assistant_root: Path,
    message: Message,
) -> list[str]:
    result_paths: list[str] = []
    message_id = int(message.message_id or 0)
    timestamp = _message_datetime(message).strftime("%Y%m%d-%H%M%S")

    specs: list[tuple[str, str, str]] = []

    photos = message.photo or []
    if photos:
        photo = photos[-1]
        specs.append((photo.file_id, "89_images", f"photo-{message_id}.jpg"))

    document = message.document
    if document:
        filename = document.file_name or f"document-{message_id}"
        mime_type = (document.mime_type or "").lower()
        target_dir = "89_images" if mime_type.startswith("image/") else "88_files"
        specs.append((document.file_id, target_dir, filename))

    voice = message.voice
    if voice:
        specs.append((voice.file_id, "88_files", f"voice-{message_id}.ogg"))

    audio = message.audio
    if audio:
        filename = audio.file_name or f"audio-{message_id}.mp3"
        specs.append((audio.file_id, "88_files", filename))

    video = message.video
    if video:
        specs.append((video.file_id, "88_files", f"video-{message_id}.mp4"))

    video_note = message.video_note
    if video_note:
        specs.append((video_note.file_id, "88_files", f"video-note-{message_id}.mp4"))

    for file_id, folder, hinted_name in specs:
        metadata = await bot.get_file(file_id)
        remote_path = metadata.file_path or ""
        remote_suffix = Path(remote_path).suffix
        hint_stem = _slug(Path(hinted_name).stem, fallback="file")
        suffix = remote_suffix or Path(hinted_name).suffix or ".bin"
        filename = f"{timestamp}-{hint_stem}{suffix}"
        base_dir = assistant_root / folder
        base_dir.mkdir(parents=True, exist_ok=True)
        output_path = _unique_path(base_dir, filename)
        buffer = io.BytesIO()
        await bot.download(file=file_id, destination=buffer)
        output_path.write_bytes(buffer.getvalue())
        result_paths.append(output_path.relative_to(assistant_root).as_posix())

    return result_paths
