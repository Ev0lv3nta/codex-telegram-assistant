from __future__ import annotations

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


def _send_files_protocol_note() -> str:
    return (
        "Если нужно отправить пользователю один или несколько файлов в Telegram, "
        "добавь в КОНЕЦ ответа отдельные строки формата:\n"
        "[[send-file:daily/2026-02-22.md]]\n"
        "[[send-file:topics/note.md]]\n"
        "Каждый путь указывай отдельно, только путь на сервере. "
        "Не оборачивай эти строки в код-блок."
    )


def _risky_action_confirmation_note() -> str:
    return (
        "Перед любым рискованным действием (удаление/перезапуск сервисов/массовые правки) "
        "сначала запроси у пользователя явное подтверждение, затем выполняй."
    )


def build_prompt(
    user_text: str,
    attachments: list[str],
    include_bootstrap: bool = False,
) -> str:
    text = (user_text or "").strip()
    attachment_block = _attachments_block(attachments)
    prefix = _bootstrap_prefix(include_bootstrap)
    send_files_note = _send_files_protocol_note()
    risky_note = _risky_action_confirmation_note()

    if text and not attachments:
        return f"{prefix}{text}\n\n{send_files_note}\n\n{risky_note}"

    if text and attachments:
        return (
            f"{prefix}{text}\n\n"
            "Вложения пользователя (пути на сервере):\n"
            f"{attachment_block}\n\n"
            f"{send_files_note}\n\n"
            f"{risky_note}"
        )

    return (
        f"{prefix}"
        "Пользователь отправил вложения без текста.\n"
        "Вложения пользователя (пути на сервере):\n"
        f"{attachment_block}\n\n"
        f"{send_files_note}\n\n"
        f"{risky_note}"
    )
