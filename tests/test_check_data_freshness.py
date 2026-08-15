from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from scripts.check_data_freshness import evaluate_city_rows, parse_utc_timestamp


class ParseUtcTimestampTests(unittest.TestCase):
    def test_accepts_z_suffix(self) -> None:
        self.assertEqual(
            parse_utc_timestamp("2026-08-15T12:00:00Z"),
            datetime(2026, 8, 15, 12, tzinfo=timezone.utc),
        )

    def test_rejects_invalid_value(self) -> None:
        self.assertIsNone(parse_utc_timestamp("not-a-date"))


class EvaluateCityRowsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.now = datetime(2026, 8, 15, 12, tzinfo=timezone.utc)

    def test_uses_newest_row_for_each_city(self) -> None:
        rows = [
            {"city_key": "miami", "fetched_at": (self.now - timedelta(days=2)).isoformat()},
            {"city_key": "miami", "fetched_at": (self.now - timedelta(hours=1)).isoformat()},
        ]
        problems = evaluate_city_rows(
            resource="weather_forecasts",
            city_keys=["miami"],
            rows=rows,
            timestamp_field="fetched_at",
            max_age_hours=6,
            now=self.now,
        )
        self.assertEqual(problems, [])

    def test_reports_missing_stale_and_future_rows(self) -> None:
        rows = [
            {"city_key": "miami", "generated_at": (self.now - timedelta(hours=7)).isoformat()},
            {"city_key": "tokyo", "generated_at": (self.now + timedelta(hours=1)).isoformat()},
        ]
        problems = evaluate_city_rows(
            resource="city_reports",
            city_keys=["miami", "tokyo", "paris"],
            rows=rows,
            timestamp_field="generated_at",
            max_age_hours=6,
            now=self.now,
        )
        self.assertEqual([problem.city_key for problem in problems], ["miami", "tokyo", "paris"])
        self.assertIn("stale", problems[0].reason)
        self.assertIn("future", problems[1].reason)
        self.assertIn("missing", problems[2].reason)


if __name__ == "__main__":
    unittest.main()
