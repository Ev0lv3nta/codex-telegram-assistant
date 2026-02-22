from __future__ import annotations

from .classifier import Mode


def _attachments_block(attachments: list[str]) -> str:
    if not attachments:
        return "- (нет вложений)"
    lines = [f"- `{item}`" for item in attachments]
    return "\n".join(lines)


def build_prompt(
    mode: Mode,
    user_text: str,
    inbox_path: str,
    attachments: list[str],
) -> str:
    return f"""
Ты работаешь в репозитории personal-assistant.
Строго следуй правилам из `AGENTS.md`.
Режим из очереди (служебно): `{mode.value}`
Inbox файл: `{inbox_path}`
Вложения:
{_attachments_block(attachments)}

Запрос пользователя:
\"\"\"{user_text}\"\"\"

Твоя роль: единый личный ассистент. Сам выбирай нужный способ работы без режимов и без просьбы выбирать кнопки/команды.

Если уместно, используй навыки из `.agents/skills/`, но не упоминай это пользователю.

Правила ответа пользователю:
- Пиши по-человечески и по делу.
- Не показывай внутреннюю кухню (skills, inbox, команды, шаги терминала, служебные режимы).
- Если изменял файлы, добавь короткий блок `Изменено:` и перечисли пути.
- Если файлы не менялись, не пиши про это отдельно.
- Если не хватает данных, задай один короткий уточняющий вопрос.
""".strip()
