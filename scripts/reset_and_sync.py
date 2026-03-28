#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Reset and Sync Runtime Data

Combined workflow that:
1. Resets runtime news/weather data (preserves city context & config)
2. Syncs fresh data from all sources

This is the recommended workflow for daily updates in production environments
(GitHub Actions, scheduled tasks, etc.).

Equivalent to:
  python scripts/reset_runtime_news.py -y && python scripts/sync_supabase.py --all --skip-context --force
"""

import argparse
import sys
from pathlib import Path
from typing import List, Optional

# Fix Windows console encoding issues
if sys.platform == "win32" and hasattr(sys.stdout, "buffer"):
    import codecs

    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, "strict")
    sys.stderr = codecs.getwriter("utf-8")(sys.stderr.buffer, "strict")

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from storage import get_datastore
import config
from helpers.feed_sync_report import FeedSyncReporter
from helpers.region_scope import normalize_region, select_city_keys_for_region


def reset_runtime_news(city_keys: Optional[List[str]] = None) -> bool:
    """
    Reset runtime news/weather data while preserving config and city context.

    Args:
        city_keys: Optional subset of city keys for scoped runtime reset.
                   When omitted, full reset behavior is preserved.

    Returns:
        True if successful
    """
    try:
        datastore = get_datastore()

        if not hasattr(datastore, "client"):
            print("ERROR Datastore does not support Supabase operations")
            return False

        print("\n" + "=" * 60)
        print("STEP 1: RESET RUNTIME NEWS DATA")
        print("=" * 60)

        scoped_city_keys = [city_key for city_key in (city_keys or []) if city_key]
        if scoped_city_keys:
            print(f"\nClearing runtime data for scoped cities: {', '.join(scoped_city_keys)}")
            datastore.client.table("feed_item_cities").delete().in_("city_key", scoped_city_keys).execute()
            print(f"   OK feed_item_cities links cleared for {len(scoped_city_keys)} city keys")
            datastore.client.table("weather_forecasts").delete().in_("city_key", scoped_city_keys).execute()
            print(f"   OK weather_forecasts cleared for {len(scoped_city_keys)} city keys")
        else:
            print("\nClearing runtime data (preserves city context & config)...")

            # Delete all records (CASCADE should handle feed_item_cities)
            datastore.client.table("feed_items").delete().neq(
                "id",
                "00000000-0000-0000-0000-000000000000",
            ).execute()
            print("   OK feed_items cleared")

            datastore.client.table("weather_forecasts").delete().neq(
                "id",
                "00000000-0000-0000-0000-000000000000",
            ).execute()
            print("   OK weather_forecasts cleared")

        # NOTE: Do NOT clear `city_reports` here - reports are preserved
        # across runtime resets so historical and archived reports remain.

        print("OK Runtime data reset complete\n")
        return True

    except Exception as e:
        print(f"\nERROR Failed to reset runtime data: {e}")
        import traceback

        traceback.print_exc()
        return False


def sync_all_cities(
    skip_context: bool = True,
    city_keys: Optional[List[str]] = None,
    region: Optional[str] = None,
) -> bool:
    """
    Sync data for enabled cities, optionally scoped to a region/city subset.

    Args:
        skip_context: If True, skip city context sync (hospitals, etc.)
        city_keys: Optional list of city keys to sync
        region: Optional normalized region name for logging

    Returns:
        True if successful
    """
    reporter = FeedSyncReporter(project_root / "reports" / "feed_health")
    try:
        # Import sync functions
        from scripts.sync_supabase import sync_city, fetch_canada_advisories, fetch_us_state_dept_advisories

        datastore = get_datastore()

        scope_label = f"region: {region}" if region else "all enabled cities"
        print("=" * 60)
        print(f"STEP 2: SYNC FRESH DATA ({scope_label})")
        print("=" * 60)

        # Fetch global advisories
        print("\nFetching global sources (travel advisories)...")

        canada_record = {
            "source_key": "canada_travel_advisories",
            "name": "Canada Travel Advisories",
            "human_name": "Canada Travel Advisories",
            "primary_url": "https://data.international.gc.ca/travel-voyage/index-updated.json",
            "final_url_used": "https://data.international.gc.ca/travel-voyage/index-updated.json",
            "status": "warning",
            "error_message": "no_items_returned",
        }
        try:
            advisory_items = fetch_canada_advisories("US")
            canada_count = len(advisory_items)
            canada_record.update(
                {
                    "total_entries_parsed": canada_count,
                    "items_returned_after_filtering": canada_count,
                    "status": "ok" if canada_count > 0 else "warning",
                    "error_message": None if canada_count > 0 else "no_items_returned",
                }
            )
            if advisory_items:
                datastore.upsert_feed_items(advisory_items)
                print("   OK Stored Canada travel advisory (US)")
        except Exception as e:
            canada_record.update({"status": "error", "error_message": str(e)})
            print(f"   WARN Failed to fetch Canada advisories: {e}")
        reporter.add_record(canada_record)

        us_state_record = {
            "source_key": "us_travel_advisories",
            "name": "US State Department Travel Advisories",
            "human_name": "US State Department Travel Advisories",
            "primary_url": "https://travel.state.gov/_res/rss/TAsTWs.xml",
            "final_url_used": "https://travel.state.gov/_res/rss/TAsTWs.xml",
            "status": "warning",
            "error_message": "no_items_returned",
        }
        try:
            us_state_items = fetch_us_state_dept_advisories()
            us_count = len(us_state_items)
            us_state_record.update(
                {
                    "total_entries_parsed": us_count,
                    "items_returned_after_filtering": us_count,
                    "status": "ok" if us_count > 0 else "warning",
                    "error_message": None if us_count > 0 else "no_items_returned",
                }
            )
            if us_state_items:
                datastore.upsert_feed_items(us_state_items)
                print(f"   OK Stored {len(us_state_items)} US State Dept advisories")
        except Exception as e:
            us_state_record.update({"status": "error", "error_message": str(e)})
            print(f"   WARN Failed to fetch US State Dept advisories: {e}")
        reporter.add_record(us_state_record)

        if city_keys:
            target_city_keys = [city_key for city_key in city_keys if city_key in config.CITIES]
        else:
            target_city_keys = [
                city_key
                for city_key, city_config in config.CITIES.items()
                if isinstance(city_config, dict) and city_config.get("enabled", True)
            ]

        failed_cities: List[str] = []
        for city_key in target_city_keys:
            city_config = config.CITIES.get(city_key)
            if not isinstance(city_config, dict):
                print(f"\nSkipping invalid city config entry: {city_key}")
                continue
            if not city_config.get("enabled", True):
                print(f"\nSkipping disabled city: {city_key}")
                continue

            success = sync_city(
                city_key=city_key,
                city_config=city_config,
                datastore=datastore,
                skip_context=skip_context,
                context_only=False,
                force=True,
                reporter=reporter,
            )

            if not success:
                failed_cities.append(city_key)

        if failed_cities:
            print(f"\nWARN Failed cities: {', '.join(failed_cities)}")
            return False

        print("\nOK City sync complete")
        return True

    except Exception as e:
        print(f"\nERROR Failed to sync cities: {e}")
        import traceback

        traceback.print_exc()
        return False
    finally:
        try:
            json_path, md_path = reporter.write_reports()
            print(f"\nFeed health report JSON: {json_path}")
            print(f"Feed health report Markdown: {md_path}")
            reporter.print_console_summary()
        except Exception as report_exc:
            print(f"\nWARN Failed to write feed health report: {report_exc}")


def main():
    parser = argparse.ArgumentParser(
        description="Reset runtime data and sync fresh news/weather",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full reset and sync all enabled cities (default behavior)
  python scripts/reset_and_sync.py

  # Scoped reset and sync for a region
  python scripts/reset_and_sync.py --region europe

  # Reset and sync with city context refresh (slower, use weekly)
  python scripts/reset_and_sync.py --with-context

This script performs two operations:
  1. Clears runtime data (feed_items, weather_forecasts) - NOTE: city_reports are preserved
  2. Syncs fresh data from all sources (news, GDELT, NWS, weather)

Preserved data:
  - cities & sources (config)
  - curated_places (manually curated)
  - city_context_snapshots (hospitals, pharmacies, stores)
  - transit_snapshots (transit infrastructure)
        """,
    )

    parser.add_argument(
        "--with-context",
        action="store_true",
        help="Include city context sync (hospitals, etc.) - slower",
    )
    parser.add_argument(
        "--region",
        type=str,
        default=None,
        help="Optional sync scope: americas, europe, asia (alias: us)",
    )

    args = parser.parse_args()

    print("\nRESET AND SYNC WORKFLOW")
    print("=" * 60)

    try:
        normalized_region = normalize_region(args.region)
    except ValueError as exc:
        parser.error(str(exc))

    selected_city_keys = select_city_keys_for_region(config.CITIES, normalized_region)
    if normalized_region:
        print(f"Running scoped sync for region: {normalized_region}")
        print(f"Cities selected: {', '.join(selected_city_keys) if selected_city_keys else '(none)'}")
        if not selected_city_keys:
            print("No enabled cities found for requested region; aborting.")
            return 1
    else:
        print("Running full sync for all enabled cities")

    # Step 1: Reset runtime data
    reset_scope = selected_city_keys if normalized_region else None
    if not reset_runtime_news(city_keys=reset_scope):
        print("\nERROR Reset failed, aborting")
        return 1

    # Step 2: Sync fresh data
    skip_context = not args.with_context
    if not sync_all_cities(
        skip_context=skip_context,
        city_keys=selected_city_keys,
        region=normalized_region,
    ):
        print("\nERROR Sync failed")
        return 1

    print("\n" + "=" * 60)
    print("OK RESET AND SYNC COMPLETE")
    print("=" * 60)
    print("\nYou can now generate reports with:")
    print("  python run_report.py <city-key>")
    print("  python scripts/generate_all_reports.py")

    return 0


if __name__ == "__main__":
    sys.exit(main())
