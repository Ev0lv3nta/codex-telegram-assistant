from __future__ import annotations

from enum import Enum
import re


class Mode(str, Enum):
    AUTO = "auto"
    INTAKE = "intake"
    RESEARCH = "research"
    ANSWER = "answer"
    FINANCE = "finance"
    MAINTENANCE = "maintenance"


BUTTON_LABELS: dict[Mode, str] = {
    Mode.AUTO: "Авто",
    Mode.INTAKE: "Интейк",
    Mode.RESEARCH: "Исследование",
    Mode.ANSWER: "Ответ",
    Mode.FINANCE: "Финансы",
    Mode.MAINTENANCE: "Обслуживание",
}


LABEL_TO_MODE: dict[str, Mode] = {
    value.lower(): key for key, value in BUTTON_LABELS.items()
}


RESEARCH_KEYWORDS = {
    "поищи",
    "поиск",
    "найди в интернете",
    "исследуй",
    "собери источники",
    "web search",
    "гугл",
}

FINANCE_KEYWORDS = {
    "расход",
    "доход",
    "трата",
    "потратил",
    "потратила",
    "бюджет",
    "зарплата",
    "чек",
}

MAINTENANCE_KEYWORDS = {
    "система:",
    "обнови agents.md",
    "добавь скилл",
    "исправь бота",
    "почини бота",
    "перезапусти сервис",
    "обнови пайплайн",
}

ANSWER_KEYWORDS = {
    "найди",
    "вспомни",
    "где у меня",
    "покажи",
    "какой был",
    "дай ссылку",
    "что я писал",
}

CURRENCY_RE = re.compile(r"\b(rub|usd|eur|uah|kzt|₽|\$|€)\b", re.IGNORECASE)


def mode_from_label(text: str) -> Mode | None:
    if not text:
        return None
    return LABEL_TO_MODE.get(text.strip().lower())


def parse_mode_command(text: str) -> Mode | None:
    if not text:
        return None
    value = text.strip().lower()
    if not value.startswith("/mode"):
        return None
    parts = value.split(maxsplit=1)
    if len(parts) < 2:
        return None
    alias = parts[1].strip()
    aliases = {
        "auto": Mode.AUTO,
        "интейк": Mode.INTAKE,
        "intake": Mode.INTAKE,
        "исследование": Mode.RESEARCH,
        "research": Mode.RESEARCH,
        "ответ": Mode.ANSWER,
        "answer": Mode.ANSWER,
        "финансы": Mode.FINANCE,
        "finance": Mode.FINANCE,
        "обслуживание": Mode.MAINTENANCE,
        "maintenance": Mode.MAINTENANCE,
    }
    return aliases.get(alias)


def classify_text(text: str) -> Mode:
    normalized = (text or "").strip().lower()
    if not normalized:
        return Mode.INTAKE

    if any(token in normalized for token in MAINTENANCE_KEYWORDS):
        return Mode.MAINTENANCE

    if CURRENCY_RE.search(normalized) or any(
        token in normalized for token in FINANCE_KEYWORDS
    ):
        return Mode.FINANCE

    if any(token in normalized for token in RESEARCH_KEYWORDS):
        return Mode.RESEARCH

    if "?" in normalized or "？" in normalized or any(
        token in normalized for token in ANSWER_KEYWORDS
    ):
        return Mode.ANSWER

    return Mode.INTAKE


def keyboard_markup() -> dict:
    return {
        "keyboard": [
            [
                {"text": BUTTON_LABELS[Mode.AUTO]},
                {"text": BUTTON_LABELS[Mode.INTAKE]},
                {"text": BUTTON_LABELS[Mode.RESEARCH]},
            ],
            [
                {"text": BUTTON_LABELS[Mode.ANSWER]},
                {"text": BUTTON_LABELS[Mode.FINANCE]},
                {"text": BUTTON_LABELS[Mode.MAINTENANCE]},
            ],
        ],
        "resize_keyboard": True,
        "is_persistent": True,
    }
