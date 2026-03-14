from __future__ import annotations

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

GUARD_APPROVE_CALLBACK_DATA = "autonomy:guard:approve_once"
GUARD_STOP_CALLBACK_DATA = "autonomy:guard:stop"


def build_guard_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Разрешить один сеанс",
                    callback_data=GUARD_APPROVE_CALLBACK_DATA,
                )
            ],
            [
                InlineKeyboardButton(
                    text="Остановить автономность",
                    callback_data=GUARD_STOP_CALLBACK_DATA,
                )
            ],
        ]
    )
