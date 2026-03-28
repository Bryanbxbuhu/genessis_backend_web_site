#!/usr/bin/env python3
"""Temporary runner: call DB-first report generator for all enabled cities.

This avoids LLM API calls and ensures canonical DB-backed fields (including
`travel_essentials`) are written to `city_reports.report_data`.

Delete this file after running.
"""
import sys
from pathlib import Path
import config
from travel_agent import TravelIntelAgent


def main():
    cities = getattr(config, "CITIES", {})
    enabled = [k for k, v in cities.items() if v.get("enabled", True)]
    if not enabled:
        print("No enabled cities found")
        sys.exit(1)

    agent = TravelIntelAgent(api_key=getattr(config, "OPENAI_API_KEY", None))

    for city_key in enabled:
        try:
            print(f"\n=== DB-first report for: {city_key} ===")
            out = agent.generate_location_brief_db_first(location=city_key, output_dir=Path('reports'), skip_pdf=True)
            print(f"Wrote: {out}")
        except Exception as e:
            print(f"Failed for {city_key}: {e}")
            import traceback
            traceback.print_exc()

if __name__ == '__main__':
    main()
