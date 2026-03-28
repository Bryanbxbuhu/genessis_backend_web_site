#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Reset Runtime News Data

Clears feed items and weather forecasts (runtime data)
while preserving city reports, city context, curated places, and config.

This is useful when you want to refresh news/weather data without
re-syncing hospital/pharmacy/store infrastructure.

Equivalent to what gets updated by:
  python scripts/sync_supabase.py --all --skip-context --force

Preserves:
- cities (config)
- sources (config)
- curated_places (manually curated)
- city_reports (AI-generated reports)
- city_context_snapshots (hospital/pharmacy/store data)
- transit_snapshots (transit infrastructure)

Clears:
- feed_items & feed_item_cities (news, GDELT, NWS alerts, advisories)
- weather_forecasts (7-day forecasts)
"""

import argparse
import sys
from pathlib import Path

# Fix Windows console encoding issues
if sys.platform == 'win32' and hasattr(sys.stdout, 'buffer'):
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from storage import get_datastore


def reset_runtime_news() -> bool:
    """
    Reset only runtime news/weather data via direct SQL TRUNCATE.
    
    This is more targeted than reset_osint_runtime_data() RPC which also
    clears city_context_snapshots. We only want to clear feed/weather data.
    
    Returns:
        True if successful
    """
    try:
        datastore = get_datastore()
        
        if not hasattr(datastore, 'client'):
            print("✗ Datastore does not support Supabase operations")
            return False
        
        print("\n" + "="*60)
        print("RESET RUNTIME NEWS DATA")
        print("="*60)
        print("\nThis will clear:")
        print("  ? feed_items & feed_item_cities (news, alerts, advisories)")
        print("  ? weather_forecasts (7-day forecasts)")
        print("\nThis will PRESERVE:")
        print("  ? cities & sources (config)")
        print("  ? curated_places (manually curated)")
        print("  ? city_reports (AI-generated reports)")
        print("  ? city_context_snapshots (hospital/pharmacy/store data)")
        print("  ? transit_snapshots (transit infrastructure)")
        print("\n" + "="*60)
        
        # Execute TRUNCATE via raw SQL for precise control
        print("\n🗑️  Truncating runtime news tables...")
        
        # Note: CASCADE ensures feed_item_cities is also cleared
        datastore.client.postgrest.rpc(
            "exec_sql",
            {"sql": "TRUNCATE TABLE public.feed_items CASCADE;"}
        ).execute()
        print("   OK feed_items & feed_item_cities cleared")
        
        datastore.client.postgrest.rpc(
            "exec_sql", 
            {"sql": "TRUNCATE TABLE public.weather_forecasts;"}
        ).execute()
        print("   OK weather_forecasts cleared")
        
        
        print("\n✅ Runtime news data reset complete")
        print("\nNext step: Sync data for all cities:")
        print("  python scripts/sync_supabase.py --all --skip-context --force")
        
        return True
        
    except Exception as e:
        # If exec_sql RPC doesn't exist, fall back to direct table operations
        print(f"\n⚠️  RPC method failed, using direct table truncate: {e}")
        
        try:
            print("\n🗑️  Truncating via direct table access...")
            
            # Delete all records (CASCADE should handle feed_item_cities)
            datastore.client.table("feed_items").delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
            print("   ✓ feed_items cleared")
            
            datastore.client.table("weather_forecasts").delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
            print("   ✓ weather_forecasts cleared")
            
            
            print("\n✅ Runtime news data reset complete")
            print("\nNext step: Sync data for all cities:")
            print("  python scripts/sync_supabase.py --all --skip-context --force")
            
            return True
            
        except Exception as e2:
            print(f"\n✗ Failed to reset runtime news data: {e2}")
            import traceback
            traceback.print_exc()
            return False


def main():
    parser = argparse.ArgumentParser(
        description="Reset runtime news/weather data (preserves city context & config)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Reset runtime news data
  python scripts/reset_runtime_news.py
  
  # Reset and immediately sync fresh data
  python scripts/reset_runtime_news.py && python scripts/sync_supabase.py --all --skip-context --force

What gets cleared:
  - feed_items (news, GDELT, NWS alerts, advisories)
  - weather_forecasts (7-day forecasts)

What gets preserved:
  - cities & sources (config)
  - curated_places (manually curated)
  - city_reports (AI-generated reports)
  - city_context_snapshots (hospitals, pharmacies, stores)
  - transit_snapshots (transit infrastructure)
        """
    )
    
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Skip confirmation prompt",
    )
    
    args = parser.parse_args()
    
    # Confirmation prompt
    if not args.yes:
        print("\n⚠️  WARNING: This will delete all feed items and weather forecasts!")
        print("City context (hospitals, etc.) and curated places will be preserved.")
        response = input("\nContinue? [y/N]: ").strip().lower()
        if response not in ('y', 'yes'):
            print("Cancelled.")
            return 0
    
    # Execute reset
    success = reset_runtime_news()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
