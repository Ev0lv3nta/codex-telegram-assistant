import unittest

from system.bot.main import _is_smalltalk, _message_has_attachments


class MainHelpersTests(unittest.TestCase):
    def test_smalltalk_true(self) -> None:
        self.assertTrue(_is_smalltalk("привет"))
        self.assertTrue(_is_smalltalk("hello!"))

    def test_smalltalk_false(self) -> None:
        self.assertFalse(_is_smalltalk("привет, найди мне статью по grpc"))
        self.assertFalse(_is_smalltalk("сохрани заметку"))

    def test_message_has_attachments(self) -> None:
        self.assertTrue(_message_has_attachments({"photo": [{"file_id": "x"}]}))
        self.assertFalse(_message_has_attachments({"text": "hi"}))


if __name__ == "__main__":
    unittest.main()
