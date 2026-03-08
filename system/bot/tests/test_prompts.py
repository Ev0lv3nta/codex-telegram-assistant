import unittest

from system.bot.prompts import build_autonomy_wakeup_prompt, build_prompt


class PromptTests(unittest.TestCase):
    def test_prompt_is_plain_user_text_when_no_attachments(self) -> None:
        prompt = build_prompt(
            user_text="Поищи лучшие источники",
            attachments=[],
        )
        self.assertIn("Поищи лучшие источники", prompt)
        self.assertIn("Рабочая память ассистента", prompt)
        self.assertIn("memory/about_user.md", prompt)
        self.assertIn("[[send-file:", prompt)
        self.assertIn("сначала запроси у пользователя явное подтверждение", prompt)

    def test_prompt_has_bootstrap_prefix_for_new_session(self) -> None:
        prompt = build_prompt(
            user_text="Привет",
            attachments=[],
            include_bootstrap=True,
        )
        self.assertIn("AGENTS.md", prompt)
        self.assertIn("Привет", prompt)
        self.assertIn("topics/autonomy-companion-plan.md", prompt)
        self.assertIn("[[send-file:", prompt)
        self.assertIn("сначала запроси у пользователя явное подтверждение", prompt)

    def test_prompt_includes_attachment_paths(self) -> None:
        prompt = build_prompt(
            user_text="Где контакт Ивана?",
            attachments=["88_files/file.pdf"],
        )
        self.assertIn("Где контакт Ивана?", prompt)
        self.assertIn("88_files/file.pdf", prompt)
        self.assertIn("memory/open_loops.md", prompt)
        self.assertIn("[[send-file:", prompt)
        self.assertIn("сначала запроси у пользователя явное подтверждение", prompt)

    def test_prompt_for_attachments_without_text(self) -> None:
        prompt = build_prompt(
            user_text="",
            attachments=["89_images/pic.jpg"],
        )
        self.assertIn("без текста", prompt)
        self.assertIn("89_images/pic.jpg", prompt)
        self.assertIn("Переписывай память тогда", prompt)
        self.assertIn("[[send-file:", prompt)
        self.assertIn("сначала запроси у пользователя явное подтверждение", prompt)

    def test_autonomy_wakeup_prompt_mentions_single_safe_step(self) -> None:
        prompt = build_autonomy_wakeup_prompt(
            current_task_id=7,
            current_task_title="Подготовить идею",
            current_task_details="Сделать один шаг и не уходить в длинную миссию",
            current_task_kind="research",
            recent_task_lines=["#5 [done] Старая задача — без нового результата"],
            recent_journal_lines=["## 10:30 · completed - Итог: сделал краткий шаг"],
            recent_user_lines=["Я сейчас изучаю юнит-экономику"],
            include_bootstrap=True,
        )
        self.assertIn("автономный heartbeat-сеанс", prompt)
        self.assertIn("один осмысленный и безопасный автономный сеанс", prompt)
        self.assertIn("self-check", prompt)
        self.assertIn("Недавний контекст пробуждения", prompt)
        self.assertIn("Я сейчас изучаю юнит-экономику", prompt)
        self.assertIn("При необходимости сам открой нужные файлы workspace", prompt)
        self.assertIn("/root/personal-assistant/memory/about_user.md", prompt)
        self.assertIn("/root/personal-assistant/topics/assistant-constitution.md", prompt)
        self.assertIn("/root/personal-assistant/system/tasks/autonomy_requests.md", prompt)
        self.assertIn("id: 7", prompt)
        self.assertIn("Подготовить идею", prompt)
        self.assertIn("Если владелец напишет, у него приоритет", prompt)
        self.assertIn("ты можешь сделать это прямо сейчас", prompt)
        self.assertIn("короткий уточняющий вопрос", prompt)
        self.assertIn("1-3 короткие строки", prompt)
        self.assertIn("не перечисляй тестовые команды", prompt)
        self.assertIn("ACTION: COMPLETE", prompt)
        self.assertIn("[[autonomy-next]]", prompt)
        self.assertIn("DELAY_SEC", prompt)
        self.assertIn("ACTION: STEP", prompt)
        self.assertIn("ACTION: NOOP", prompt)
        self.assertIn("Не выбирай шаги, чей единственный результат", prompt)
        self.assertIn("Не дроби задачу на микрошаги", prompt)
        self.assertIn("AGENTS.md", prompt)

    def test_autonomy_wakeup_prompt_adds_self_review_for_project_tasks(self) -> None:
        prompt = build_autonomy_wakeup_prompt(
            current_task_id=9,
            current_task_title="Докрутить owner-facing pulse",
            current_task_details="Сделать короткий шаг в коде",
            current_task_kind="project",
            include_bootstrap=True,
        )
        self.assertIn("Если этот шаг меняет самого ассистента", prompt)
        self.assertIn("что именно меняешь, зачем, главный риск и как проверишь результат", prompt)
        self.assertIn("[[self-review]]", prompt)
        self.assertIn("CHANGE: ...", prompt)
        self.assertIn("[[notify-owner]]", prompt)
        self.assertIn("Без этого блока внутренний project/maintenance/review шаг лучше считать тихим", prompt)

    def test_autonomy_wakeup_prompt_warns_about_excessive_followups(self) -> None:
        prompt = build_autonomy_wakeup_prompt(
            current_task_id=11,
            current_task_title="Дожать runtime-хвост",
            current_task_details="Закрыть оставшийся узел без нового микрофоллоу-апа",
            current_task_kind="project",
            current_task_continuation_count=2,
            include_bootstrap=True,
        )
        self.assertIn("continuation_count: 2", prompt)
        self.assertIn("Не дроби её на новый микрошаг", prompt)
        self.assertIn("Лимит мелких follow-up'ов", prompt)

    def test_autonomy_wakeup_prompt_allows_noop_without_current_task(self) -> None:
        prompt = build_autonomy_wakeup_prompt(
            recent_task_lines=["#4 [done] Проверить идею"],
            recent_journal_lines=["## 11:00 · completed - Итог: поставлена задача"],
            recent_user_lines=["Мне интересна unit-экономика"],
            include_bootstrap=True,
        )
        self.assertIn("Если действительно делать нечего, ответь ровно `ACTION: NOOP`", prompt)
        self.assertIn("ACTION: STEP", prompt)
        self.assertIn("ACTION: COMPLETE", prompt)
        self.assertIn("Мне интересна unit-экономика", prompt)
        self.assertIn("При необходимости сам открой нужные файлы workspace", prompt)
        self.assertIn("/root/personal-assistant/topics/assistant-constitution.md", prompt)
        self.assertIn("/root/personal-assistant/topics/autonomy-companion-plan.md", prompt)
        self.assertIn("Не дёргай владельца по каждой мелочи", prompt)
        self.assertIn("AGENTS.md", prompt)


if __name__ == "__main__":
    unittest.main()
