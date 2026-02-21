# Personal Assistant (Codex + Telegram)

Репозиторий содержит:

- Vault-память (`00_inbox`, `01_capture`, `90_memory`, ...)
- Project skills для Codex (`.agents/skills`)
- Runtime Telegram-бота (`system/bot`)
- CI/CD workflow-файлы (`.github/workflows`)

## Локальный запуск

1. `bash system/runbooks/bootstrap.sh`
2. Заполнить `system/bot/.env`
3. `bash system/runbooks/run_bot.sh`

## Минимум для `.env`

- `TG_BOT_TOKEN=<telegram bot token>`
- `TG_ALLOWED_USER_IDS=<твой telegram user id>`

## Как это работает

1. Бот получает сообщение.
2. Сохраняет сырье в `00_inbox/`.
3. Ставит задачу в очередь SQLite.
4. Worker запускает `codex exec` с нужным skill-режимом.
5. Codex обновляет vault.
6. Бот отправляет итог в Telegram.
7. При успехе делает git commit; push пытается раз в сутки (UTC) по настройке.

