from __future__ import annotations

from .classifier import Mode


def _attachments_block(attachments: list[str]) -> str:
    if not attachments:
        return "- (нет вложений)"
    lines = [f"- `{item}`" for item in attachments]
    return "\n".join(lines)


def _common_prefix(
    mode: Mode, user_text: str, inbox_path: str, attachments: list[str], skill_name: str
) -> str:
    return f"""
Ты работаешь в репозитории personal-assistant.
Строго следуй правилам из `AGENTS.md`.
Используй skill `{skill_name}`.

Режим: `{mode.value}`
Inbox файл: `{inbox_path}`
Вложения:
{_attachments_block(attachments)}

Запрос пользователя:
\"\"\"{user_text}\"\"\"

Требование к финальному ответу:
- Что сделал
- Какие файлы обновил
- Что требует уточнения (если есть)
""".strip()


def build_prompt(
    mode: Mode,
    user_text: str,
    inbox_path: str,
    attachments: list[str],
) -> str:
    if mode is Mode.RESEARCH:
        extra = """
Сделай web research и сохрани результат в vault по правилам.
Если есть полезные ссылки "на потом", обнови `01_capture/read_later.md`.
"""
        return _common_prefix(
            mode, user_text, inbox_path, attachments, "assistant-research"
        ) + "\n\n" + extra.strip()

    if mode is Mode.ANSWER:
        extra = """
Найди информацию в vault и ответь пользователю.
Не меняй файлы без явной необходимости.
Обязательно укажи пути файлов, откуда взяты данные.
"""
        return _common_prefix(
            mode, user_text, inbox_path, attachments, "assistant-answer"
        ) + "\n\n" + extra.strip()

    if mode is Mode.FINANCE:
        extra = """
Обработай задачу как финансовую.
Если это запись операции, обнови `system/finance/finance.db` и добавь след в daily.
Если это запрос отчета, построй ответ по данным БД и укажи метод расчета.
Следуй правилам из `90_memory/finance_rules.md`.
"""
        return _common_prefix(
            mode, user_text, inbox_path, attachments, "assistant-finance"
        ) + "\n\n" + extra.strip()

    if mode is Mode.MAINTENANCE:
        extra = """
Это режим обслуживания.
Можно менять `system/`, `.agents/skills/`, `AGENTS.md` только в рамках запроса.
После изменений обнови `99_process/assistant_changelog.md`.
"""
        return _common_prefix(
            mode, user_text, inbox_path, attachments, "assistant-maintenance"
        ) + "\n\n" + extra.strip()

    return _common_prefix(mode, user_text, inbox_path, attachments, "assistant-intake")

