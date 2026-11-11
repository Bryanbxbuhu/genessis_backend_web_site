from unittest import TestCase
from unittest.mock import Mock, patch

import config
from city_context import CityContextTool
from test_llm import completion


class WebsiteFallbackTests(TestCase):
    def setUp(self):
        self.tool = CityContextTool(datastore=Mock())
        self.tool._ddg_search_with_timeout = Mock(return_value=[
            {"href": "https://fixture.example/location", "title": "Fixture Rental", "body": "Official"},
        ])

    def tearDown(self):
        self.tool._session.close()

    @patch("city_context.time.sleep")
    def test_missing_website_uses_search_result(self, sleep):
        facilities = [{"name": "Fixture Rental"}]
        self.tool._enrich_facility_websites(facilities, "Fixture City", "car rental")
        self.assertEqual(facilities[0]["website"], "https://fixture.example/location")

    @patch("city_context.time.sleep")
    def test_existing_website_does_not_search(self, sleep):
        facilities = [{"name": "Fixture Rental", "website": "https://fixture.example/location"}]
        self.tool._enrich_facility_websites(facilities, "Fixture City", "car rental")
        self.tool._ddg_search_with_timeout.assert_not_called()

    @patch("city_context.time.sleep")
    def test_multiple_candidates_use_configured_model(self, sleep):
        self.tool.openai_client = Mock()
        self.tool.openai_client.chat.completions.create.return_value = completion(
            '{"website":"https://fixture.example/location"}'
        )
        self.tool._ddg_search_with_timeout.return_value.append(
            {"href": "https://other.example/location", "title": "Other", "body": "Other"}
        )
        facilities = [{"name": "Fixture Rental"}]
        self.tool._enrich_facility_websites(facilities, "Fixture City", "car rental")
        self.assertEqual(facilities[0]["website"], "https://fixture.example/location")
        self.assertEqual(self.tool.openai_client.chat.completions.create.call_args.kwargs["model"], config.OPENAI_MODEL)

    @patch("city_context.time.sleep")
    def test_rate_limit_retries_then_recovers(self, sleep):
        results = self.tool._ddg_search_with_timeout.return_value
        self.tool._ddg_search_with_timeout.side_effect = [RuntimeError("RateLimit"), results]
        facilities = [{"name": "Fixture Rental"}]
        self.tool._enrich_facility_websites(facilities, "Fixture City", "car rental")
        self.assertEqual(facilities[0]["website"], "https://fixture.example/location")
        self.assertEqual(self.tool._ddg_search_with_timeout.call_count, 2)
