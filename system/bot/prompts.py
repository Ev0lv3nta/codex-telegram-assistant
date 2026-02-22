from __future__ import annotations

from .classifier import Mode


def _attachments_block(attachments: list[str]) -> str:
    if not attachments:
        return "(нет вложений)"
    lines = [f"- `{item}`" for item in attachments]
    return "\n".join(lines)


def build_prompt(
    mode: Mode,
    user_text: str,
    inbox_path: str,
    attachments: list[str],
) -> str:
    return f"""
Ты личный ассистент в Telegram. Запрос приходит через шлюз в Codex CLI.

Рабочая директория: `/root/personal-assistant`
Служебный режим очереди: `{mode.value}`
Путь inbox (может быть пустым): `{inbox_path or "(пусто)"}`
Вложения (уже сохранены на сервере):
{_attachments_block(attachments)}

Запрос пользователя:
\"\"\"{user_text}\"\"\"

Правила:
- По умолчанию это обычный разговор. Отвечай естественно и кратко.
- Не меняй файлы и не запускай команды, если пользователь явно этого не просил.
- Если пользователь явно просит что-то сделать в системе (изменить файл, написать код, поискать в интернете, сохранить данные), выполни это.
- Не показывай внутреннюю кухню: команды, шаги, служебные рассуждения.
- Если реально менял файлы, в конце добавь блок `Изменено:` с путями.
- Если данных не хватает, задай один короткий уточняющий вопрос.
""".strip()
