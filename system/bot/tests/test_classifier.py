import unittest

from system.bot.classifier import Mode, classify_text, mode_from_label, parse_mode_command


class ClassifierTests(unittest.TestCase):
    def test_mode_button(self) -> None:
        self.assertEqual(mode_from_label("Финансы"), Mode.FINANCE)

    def test_parse_mode_command(self) -> None:
        self.assertEqual(parse_mode_command("/mode research"), Mode.RESEARCH)
        self.assertEqual(parse_mode_command("/mode auto"), Mode.AUTO)

    def test_classify_research(self) -> None:
        self.assertEqual(
            classify_text("Поищи в интернете лучшие статьи по агентам"),
            Mode.RESEARCH,
        )

    def test_classify_finance(self) -> None:
        self.assertEqual(
            classify_text("Расход 1200 RUB на кафе"),
            Mode.FINANCE,
        )

    def test_classify_answer(self) -> None:
        self.assertEqual(
            classify_text("Где у меня был рецепт блинчиков?"),
            Mode.ANSWER,
        )

    def test_classify_answer_question_mark_in_middle(self) -> None:
        self.assertEqual(
            classify_text("Как ты работаешь? В общих чертах простыми словами"),
            Mode.ANSWER,
        )

    def test_classify_default(self) -> None:
        self.assertEqual(classify_text("Сохрани мою заметку про сегодня"), Mode.INTAKE)


if __name__ == "__main__":
    unittest.main()
