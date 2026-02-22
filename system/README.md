# System

Эта папка содержит исполняемую часть: Telegram-бот, очередь задач, локальные БД и runbook-и.

## Что уже есть

- `system/bot/main.py`: long-polling Telegram-бот.
- `system/bot/queue_store.py`: очередь задач на SQLite (`system/tasks/bot_state.db`).
- `system/bot/worker.py`: запуск `codex exec`/`codex exec resume` и отправка результата обратно в Telegram.
- `system/runbooks/bootstrap.sh`: подготовка окружения.
- `system/runbooks/run_bot.sh`: запуск бота.
- `system/runbooks/systemd/personal-assistant-bot.service`: шаблон unit-файла.

## Быстрый запуск

1. Перейти в корень проекта.
2. Выполнить `bash system/runbooks/bootstrap.sh`.
3. Заполнить `system/bot/.env` (минимум `TG_BOT_TOKEN`, желательно `TG_ALLOWED_USER_IDS`).
4. Запустить `bash system/runbooks/run_bot.sh`.

## Секреты и переносимость

- Перенос делается копированием всей папки проекта.
- Секреты не коммитятся (`system/bot/.env`, `system/secrets/` в `.gitignore`).

## CI/CD

- CI: `.github/workflows/ci.yml` (compile + unit tests).
- CD: `.github/workflows/cd.yml` (rsync + remote bootstrap + restart service).

Нужные секреты для CD:

- `DEPLOY_HOST`
- `DEPLOY_PORT` (опционально, по умолчанию `22`)
- `DEPLOY_USER`
- `DEPLOY_PATH`
- `DEPLOY_SSH_KEY`
- `DEPLOY_SERVICE` (опционально: имя systemd service для рестарта)
