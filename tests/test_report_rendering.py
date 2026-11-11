from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import pymupdf

from config_registry import load_registry
from helpers.region_scope import REGION_CITY_KEYS
from report_data_contract import make_empty_report_data, validate_report_data
from report_renderer import find_section_pages, render_html


ROOT = Path(__file__).resolve().parents[1]


class ReportRenderingTests(unittest.TestCase):
    def test_every_configured_city_has_renderable_empty_report(self):
        cities, _ = load_registry()
        for key, city in cities.items():
            with self.subTest(city=key):
                report = make_empty_report_data(city_key=key, window_hours=48)
                report["destination"] = city["name"]
                report["country_code"] = city.get("country_code", "")
                validate_report_data(report)
                html = render_html(ROOT / "templates", "report_new.html", report)
                self.assertIn("</html>", html)
                self.assertIn('class="toc-page-number"', html)
                self.assertNotIn('\u0192?"', html)

    def test_region_schedule_covers_enabled_cities_exactly_once(self):
        cities, _ = load_registry()
        enabled = {key for key, city in cities.items() if city.get("enabled", True)}
        scheduled = [key for keys in REGION_CITY_KEYS.values() for key in keys]
        self.assertEqual(set(scheduled), enabled)
        self.assertEqual(len(scheduled), len(set(scheduled)))

    def test_section_detection_ignores_toc_and_finds_shared_page_headings(self):
        with TemporaryDirectory() as temp:
            path = Path(temp) / "sections.pdf"
            with pymupdf.open() as doc:
                page = doc.new_page()
                page.insert_text((40, 40), "Table of Contents", fontsize=16)
                page.insert_text((40, 70), "Executive Summary", fontsize=8)
                page.insert_text((40, 90), "City Essentials", fontsize=8)
                page = doc.new_page()
                page.insert_text((40, 40), "Executive Summary", fontsize=16)
                page.insert_text((40, 160), "City Essentials", fontsize=16)
                page = doc.new_page()
                page.insert_text((40, 40), "Incident Analysis", fontsize=16)
                page.insert_text((40, 160), "Intelligence Sources", fontsize=16)
                doc.save(path)
            self.assertEqual(find_section_pages(path), {
                "Executive Summary": 1, "City Essentials": 1,
                "Current Incidents": 2, "Contacts & Sources": 2,
            })
