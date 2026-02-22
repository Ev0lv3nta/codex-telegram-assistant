# Personal Assistant (Codex CLI + Telegram)

Этот репозиторий содержит Telegram-бота на `aiogram`, который проксирует сообщения в Codex CLI и
поддерживает "долгую" сессию (контекст) на чат.

## Структура

- `system/bot/` код Telegram-шлюза и воркера
- `daily/` ежедневные заметки (формат Obsidian)
- `topics/` тематические заметки
- `system/tasks/bot_state.db` SQLite для очереди и привязки `chat_id -> session_id`
- `~/.codex/sessions/` локальные сохраненные сессии Codex CLI (rollout-*.jsonl)

Вложения из Telegram сохраняются на диск по мере прихода (папки создаются при необходимости):
- `88_files/` документы/аудио/видео
- `89_images/` изображения

## Команды бота

- `/start` краткая справка
- `/status` состояние очереди + текущий session id
- `/reset` сбросить сессию чата (новый контекст)
- `/gc [days]` почистить старые сохраненные сессии Codex CLI на диске (по умолчанию 7 дней)

## Как это работает (коротко)

1. Бот получает сообщение и (если есть) скачивает вложения.
2. Ставит задачу в очередь SQLite.
3. Worker вызывает `codex exec` для новой сессии или `codex exec resume <session_id>` для продолжения.
4. Ответ отправляется обратно в Telegram.

## Настройка

Минимум в `system/bot/.env`:
- `TG_BOT_TOKEN=...`
- `TG_ALLOWED_USER_IDS=...`

Модель/effort задаются через `CODEX_MODEL` и `CODEX_EXTRA_ARGS`.
