import copy
import io
import json
from contextlib import redirect_stdout
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import config
from report_loader import ReportDataLoader
from travel_agent import TravelIntelAgent, get_city_config
from test_llm import completion


class ReportGenerationTests(unittest.TestCase):
    def setUp(self):
        self.agent = object.__new__(TravelIntelAgent)
        self.agent.datastore = Mock()
        self.agent.client = Mock()
        self.agent.travel_advisory = Mock()
        self.agent.travel_advisory.get_advisory.return_value = {}
        self.agent.forecast_client = Mock()
        self.agent.forecast_client.get_forecast.return_value = {}
        self.agent.client.chat.completions.create.return_value = completion(json.dumps({
            "top_actions": ["Monitor the supplied sources"],
            "whats_happening": [], "risk_badges": [], "cues": [], "incidents": [],
            "hospitals": [{"name": "Invented hospital"}],
            "emergency_contacts": {"police": "invented"},
        }))
        self.bundle = {
            "curated_places": {"hospital": [{"name": "Verified hospital", "website": "https://fixture.example"}]},
            "transit": [{"name": "Verified transit"}],
            "emergency_contacts": {"police": "911"},
            "coverage": {"transit": True, "hospitals": True},
        }

    def generate(self, **kwargs):
        with patch("travel_agent.get_city_config", return_value=config.CITIES["miami"]), \
                patch("wikimedia_enrichment.is_wikimedia_enrichment_enabled", return_value=False), \
                patch.object(ReportDataLoader, "load_db_bundle", return_value=copy.deepcopy(self.bundle)) as loader, \
                redirect_stdout(io.StringIO()):
            self.bundle_loader = loader
            return self.agent.build_report_data("Miami, Florida, United States", [], {}, **kwargs)

    def test_uses_configured_model_and_preserves_database_ownership(self):
        report, briefs = self.generate(return_news_briefs=True)
        self.assertEqual(report["hospitals"][0]["name"], "Verified hospital")
        self.assertEqual(report["emergency_contacts"], {"police": "911"})
        self.assertEqual(report["transit"], self.bundle["transit"])
        self.assertEqual(briefs, [])
        request = self.agent.client.chat.completions.create.call_args.kwargs
        self.assertEqual(request["model"], config.OPENAI_MODEL)
        self.assertEqual(request["response_format"], {"type": "json_object"})
        self.assertNotIn("tools", request)

    def test_driving_profile_is_preserved(self):
        profile = {"will_drive": True, "needs_idp": True, "rental_provider": "Fixture"}
        report = self.generate(trip_profile=profile)
        self.assertEqual(report["trip_profile"], profile)
        self.assertEqual(report["driving_pack"], {"checklist": [], "rental_notes": [], "accident_steps": []})

    def test_invalid_model_result_fails_before_database_merge(self):
        for response in (completion(content="not json"), completion(finish_reason="length"),
                         completion(content=None, refusal="Refused"), completion(content="[]")):
            with self.subTest(response=response):
                self.agent.client.chat.completions.create.return_value = response
                with self.assertRaises(RuntimeError):
                    self.generate()
                self.bundle_loader.assert_not_called()
        self.agent.datastore.upsert_city_report.assert_not_called()

    def test_missing_key_fails_before_constructing_clients(self):
        with patch("travel_agent.OpenAI") as client, self.assertRaisesRegex(RuntimeError, "OPENAI_API_KEY"):
            TravelIntelAgent(api_key=" ", datastore=Mock())
        client.assert_not_called()


class ReportLoaderCountryTests(unittest.TestCase):
    def test_database_only_city_uses_country_for_advisory(self):
        store = Mock()
        store.client.table.return_value.select.return_value.eq.return_value.limit.return_value.execute.return_value = SimpleNamespace(
            data=[{"display_name": "Fixture City", "country_code": "GB"}]
        )
        store.get_city_context.return_value = None
        loader = ReportDataLoader(datastore=store)
        loader._load_medical_facilities = Mock(return_value=[])
        loader._load_transit = Mock(return_value=[])
        loader._load_weather = Mock(return_value=None)
        loader._load_incidents = Mock(return_value=[])
        loader._load_curated_places = Mock(return_value=[])
        loader._fallback_list_from_previous_report = Mock(return_value=[])
        loader._load_emergency_contacts = Mock(return_value={})
        loader._load_travel_advisory = Mock(return_value={})
        loader._load_parking_link = Mock(return_value=None)
        loader._load_ai_generated_content = Mock(return_value={})
        report = loader.load_report_data("fixture-city")
        self.assertEqual(report.city_name, "Fixture City")
        self.assertEqual(loader._load_travel_advisory.call_args.args[0]["country_code"], "GB")


class CityConfigTests(unittest.TestCase):
    def test_database_disabled_city_is_not_reenabled_by_local_config(self):
        store = Mock()
        store.client.table.return_value.select.return_value.eq.return_value.limit.return_value.execute.return_value = SimpleNamespace(
            data=[{"display_name": "Miami", "enabled": False}]
        )
        with self.assertRaisesRegex(ValueError, "disabled"):
            get_city_config("miami", datastore=store)

    def test_database_failure_uses_config_fallback(self):
        store = Mock()
        store.client.table.side_effect = RuntimeError("Database unavailable")
        self.assertEqual(get_city_config("miami", datastore=store), config.CITIES["miami"])
