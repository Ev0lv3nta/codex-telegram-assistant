import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from system.bot.schedule_parser import (
    build_schedule_intent_prompt,
    compute_next_run_at,
    describe_recurrence,
    parse_schedule_intent_response,
)


class ScheduleParserTests(unittest.TestCase):
    def test_parse_model_response_for_daily_schedule(self) -> None:
        response = """
        {"action":"create","title":"Daily digest","prompt_text":"Присылай HTML-дашборд по AI-новостям.","recurrence_kind":"daily","recurrence_json":{"time":"20:00"},"timezone":"Europe/Moscow","delivery_hint":"html"}
        """
        intent = parse_schedule_intent_response(response)
        self.assertEqual(intent.action, "create")
        self.assertEqual(intent.recurrence_kind, "daily")
        self.assertEqual(intent.recurrence_json, {"time": "20:00"})
        self.assertEqual(intent.delivery_hint, "html")

    def test_compute_next_run_daily(self) -> None:
        now = datetime(2026, 3, 14, 18, 0, tzinfo=ZoneInfo("Europe/Moscow"))
        next_run = compute_next_run_at(
            "daily",
            {"time": "20:00"},
            "Europe/Moscow",
            now=now,
        )
        self.assertEqual(next_run, "2026-03-14T17:00:00+00:00")

    def test_compute_next_run_weekly_rolls_forward(self) -> None:
        now = datetime(2026, 3, 14, 18, 0, tzinfo=ZoneInfo("Europe/Moscow"))
        next_run = compute_next_run_at(
            "weekly",
            {"weekday": 0, "time": "09:00"},
            "Europe/Moscow",
            now=now,
        )
        self.assertEqual(next_run, "2026-03-16T06:00:00+00:00")

    def test_build_prompt_mentions_json_only(self) -> None:
        prompt = build_schedule_intent_prompt("каждый день в 20:00 присылай сводку")
        self.assertIn("ТОЛЬКО один JSON-объект", prompt)
        self.assertIn("no_schedule_intent", prompt)

    def test_describe_recurrence(self) -> None:
        described = describe_recurrence(
            "weekly",
            {"weekday": 4, "time": "10:30"},
            timezone_name="Europe/Moscow",
        )
        self.assertIn("пятницу", described)
        self.assertIn("10:30", described)


if __name__ == "__main__":
    unittest.main()
