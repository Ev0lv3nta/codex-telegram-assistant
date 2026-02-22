from __future__ import annotations

from .classifier import Mode

AGENTS_MD_PATH = "/root/personal-assistant/AGENTS.md"


def _attachments_block(attachments: list[str]) -> str:
    if not attachments:
        return ""
    lines = [f"- `{item}`" for item in attachments]
    return "\n".join(lines)


def _bootstrap_prefix(include_bootstrap: bool) -> str:
    if not include_bootstrap:
        return ""
    return (
        "Перед выполнением запроса открой и прочитай файл "
        f"`{AGENTS_MD_PATH}`. Следуй ему как основным инструкциям этой сессии.\n\n"
    )


def build_prompt(
    mode: Mode,
    user_text: str,
    inbox_path: str,
    attachments: list[str],
    include_bootstrap: bool = False,
) -> str:
    _ = mode, inbox_path
    text = (user_text or "").strip()
    attachment_block = _attachments_block(attachments)
    prefix = _bootstrap_prefix(include_bootstrap)

    if text and not attachments:
        return f"{prefix}{text}"

    if text and attachments:
        return (
            f"{prefix}{text}\n\n"
            "Вложения пользователя (пути на сервере):\n"
            f"{attachment_block}"
        )

    return (
        f"{prefix}"
        "Пользователь отправил вложения без текста.\n"
        "Вложения пользователя (пути на сервере):\n"
        f"{attachment_block}"
    )
