from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os


def _parse_int(value: str | None, default: int) -> int:
    if value is None or value.strip() == "":
        return default
    return int(value)


def _parse_int_set(value: str | None) -> set[int]:
    if not value:
        return set()
    result: set[int] = set()
    for token in value.split(","):
        token = token.strip()
        if token:
            result.add(int(token))
    return result


@dataclass(frozen=True)
class Settings:
    assistant_root: Path
    telegram_token: str
    allowed_user_ids: set[int]
    allowed_chat_ids: set[int]
    poll_timeout_sec: int
    idle_sleep_sec: float
    codex_bin: str
    codex_timeout_sec: int
    codex_model: str
    codex_extra_args: str
    max_result_chars: int
    state_db_path: Path
    log_level: str

    @classmethod
    def from_env(cls) -> "Settings":
        default_root = Path(__file__).resolve().parents[2]
        assistant_root = Path(
            os.getenv("ASSISTANT_ROOT", str(default_root))
        ).expanduser().resolve()

        token = os.getenv("TG_BOT_TOKEN", "").strip()
        if not token:
            raise ValueError("Missing required env var: TG_BOT_TOKEN")

        state_db = Path(
            os.getenv(
                "BOT_STATE_DB",
                str(assistant_root / "system" / "tasks" / "bot_state.db"),
            )
        ).expanduser().resolve()

        return cls(
            assistant_root=assistant_root,
            telegram_token=token,
            allowed_user_ids=_parse_int_set(os.getenv("TG_ALLOWED_USER_IDS")),
            allowed_chat_ids=_parse_int_set(os.getenv("TG_ALLOWED_CHAT_IDS")),
            poll_timeout_sec=_parse_int(os.getenv("TG_POLL_TIMEOUT_SEC"), 25),
            idle_sleep_sec=float(os.getenv("BOT_IDLE_SLEEP_SEC", "1.0")),
            codex_bin=os.getenv("CODEX_BIN", "codex"),
            codex_timeout_sec=_parse_int(os.getenv("CODEX_TIMEOUT_SEC"), 1800),
            codex_model=os.getenv("CODEX_MODEL", "").strip(),
            codex_extra_args=os.getenv("CODEX_EXTRA_ARGS", "").strip(),
            max_result_chars=_parse_int(os.getenv("BOT_MAX_RESULT_CHARS"), 3500),
            state_db_path=state_db,
            log_level=os.getenv("BOT_LOG_LEVEL", "INFO").upper(),
        )
