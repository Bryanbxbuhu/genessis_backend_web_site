#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Universal Intelligence Brief Generator

Generates travel intelligence briefs for any configured city.
Replaces city-specific scripts (run_miami_report.py, run_new_york_report.py).

Usage:
    python run_report.py miami
    python run_report.py new-york --with-driving
    python run_report.py --list-cities
"""

import argparse
import os
import sys
from pathlib import Path

import config


def _is_ci_environment() -> bool:
    return os.getenv("GITHUB_ACTIONS") == "true" or os.getenv("CI") == "true"


def list_available_cities():
    """Display all enabled cities from config."""
    print("\n" + "=" * 60)
    print("Available Cities")
    print("=" * 60)
    
    cities = getattr(config, "CITIES", {})
    if not cities:
        print("No cities configured in config.py")
        return
    
    for city_key, city_config in cities.items():
        if city_config.get("enabled", True):
            name = city_config.get("name", city_key)
            population = city_config.get("population", "Unknown")
            print(f"\n  {city_key:15s} → {name}")
            print(f"  {'':15s}   Population: {population:,}")
    
    print("\n" + "=" * 60)
    print("Usage: python run_report.py <city-key>")
    print("=" * 60 + "\n")


def main():
    """Run the travel agent for any configured city."""
    parser = argparse.ArgumentParser(
        description="Generate travel intelligence brief for any configured city",
        epilog="Examples:\n"
               "  python run_report.py miami\n"
               "  python run_report.py new-york --with-driving --needs-idp\n"
               "  python run_report.py --all\n"
               "  python run_report.py --list-cities",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "city",
        nargs="?",
        help="City key from config.CITIES (e.g., 'miami', 'new-york')"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Generate reports for all enabled cities"
    )
    parser.add_argument(
        "--list-cities",
        action="store_true",
        help="List all available cities and exit"
    )
    parser.add_argument(
        "--with-driving",
        action="store_true",
        help="Include Driving Pack section (trip includes driving/rental)"
    )
    parser.add_argument(
        "--needs-idp",
        action="store_true",
        help="Flag that International Driving Permit is recommended"
    )
    parser.add_argument(
        "--rental-provider",
        type=str,
        default=None,
        help="Car rental company name (optional)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Number of events to keep (passes through to travel_agent.py)"
    )
    parser.add_argument(
        "--news-min-score",
        type=float,
        default=None,
        help="Minimum travel relevance score to keep a news item (passes through)"
    )
    parser.add_argument(
        "--news-min-keywords",
        type=int,
        default=None,
        help="Minimum regular keyword matches to keep a news item (passes through)"
    )
    parser.add_argument(
        "--news-min-strong",
        type=int,
        default=None,
        help="Minimum strong keyword matches to keep a news item (passes through)"
    )
    parser.add_argument(
        "--news-relax-keywords",
        action="store_true",
        help="Relax keyword filter (passes through)"
    )
    parser.add_argument(
        "--news-lookback-hours",
        type=int,
        default=None,
        help="Lookback window in hours for news items (passes through)"
    )
    parser.add_argument(
        "--news-ignore-location-filter",
        action="store_true",
        help="Ignore location filter for news items (passes through)"
    )
    parser.add_argument(
        "--news-include-global-critical",
        action="store_true",
        help="Include strong-keyword events even if the city is not mentioned (passes through)"
    )
    parser.add_argument(
        "--skip-news-preflight",
        action="store_true",
        help="Skip feed_items preflight check (passes through)"
    )
    parser.add_argument(
        "--skip-pdf",
        action="store_true",
        help="Skip generating PDF; upsert JSON/trends only"
    )
    
    args = parser.parse_args()
    
    # List cities if requested
    if args.list_cities:
        list_available_cities()
        return
    
    # Generate reports for all cities if requested
    if args.all:
        if args.skip_news_preflight and _is_ci_environment():
            print("Error: --skip-news-preflight is not allowed with --all in CI.")
            sys.exit(2)

        cities = getattr(config, "CITIES", {})
        enabled_cities = [key for key, cfg in cities.items() if cfg.get("enabled", True)]
        
        if not enabled_cities:
            print("Error: No enabled cities found in config.CITIES")
            sys.exit(1)
        
        print("=" * 60)
        print(f"Generating Reports for All Cities ({len(enabled_cities)} total)")
        print("=" * 60)
        print()
        
        failed_cities = []
        successful_cities = []
        preflight_fallback_cities = []
        import subprocess
        
        for city_key in enabled_cities:
            city_name = cities[city_key].get("name", city_key)
            print(f"\n{'─' * 60}")
            print(f"📍 Processing: {city_name}")
            print(f"{'─' * 60}\n")
            
            # Build command for travel_agent.py
            script_dir = Path(__file__).parent
            travel_agent = script_dir / "travel_agent.py"
            
            cmd = [sys.executable, str(travel_agent), city_key]
            
            if args.with_driving:
                cmd.append("--will-drive")
                if args.needs_idp:
                    cmd.append("--needs-idp")
                if args.rental_provider:
                    cmd.extend(["--rental-provider", args.rental_provider])
            if args.limit is not None:
                cmd.extend(["--limit", str(args.limit)])
            if args.news_min_score is not None:
                cmd.extend(["--news-min-score", str(args.news_min_score)])
            if args.news_min_keywords is not None:
                cmd.extend(["--news-min-keywords", str(args.news_min_keywords)])
            if args.news_min_strong is not None:
                cmd.extend(["--news-min-strong", str(args.news_min_strong)])
            if args.news_relax_keywords:
                cmd.append("--news-relax-keywords")
            if args.news_lookback_hours is not None:
                cmd.extend(["--news-lookback-hours", str(args.news_lookback_hours)])
            if args.news_ignore_location_filter:
                cmd.append("--news-ignore-location-filter")
            if args.news_include_global_critical:
                cmd.append("--news-include-global-critical")
            if args.skip_news_preflight:
                cmd.append("--skip-news-preflight")
            if args.skip_pdf:
                cmd.append("--skip-pdf")
            
            # Execute travel_agent.py
            result = subprocess.run(cmd)

            # In local batch mode only, auto-retry once when preflight reports missing recent feed items.
            if result.returncode == 2 and not args.skip_news_preflight and not _is_ci_environment():
                retry_cmd = list(cmd)
                retry_cmd.append("--skip-news-preflight")
                print(f"\n⚠ {city_name} - No recent feed items; retrying with --skip-news-preflight")
                retry_result = subprocess.run(retry_cmd)
                if retry_result.returncode == 0:
                    preflight_fallback_cities.append(city_name)
                result = retry_result
            elif result.returncode == 2 and not args.skip_news_preflight and _is_ci_environment():
                print(f"\n❌ {city_name} - Preflight failed in CI; no fallback retry is allowed.")
            
            if result.returncode == 0:
                successful_cities.append(city_name)
                if city_name in preflight_fallback_cities:
                    print(f"\n✅ {city_name} - SUCCESS (preflight skipped)")
                else:
                    print(f"\n✅ {city_name} - SUCCESS")
            else:
                failed_cities.append(city_name)
                print(f"\n❌ {city_name} - FAILED (exit code: {result.returncode})")
        
        # Summary
        print("\n" + "=" * 60)
        print("Summary")
        print("=" * 60)
        print(f"✅ Successful: {len(successful_cities)}/{len(enabled_cities)}")
        if successful_cities:
            for city in successful_cities:
                print(f"   • {city}")
        if preflight_fallback_cities:
            print(f"\n⚠ Used preflight fallback: {len(preflight_fallback_cities)}")
            for city in preflight_fallback_cities:
                print(f"   • {city}")
        
        if failed_cities:
            print(f"\n❌ Failed: {len(failed_cities)}/{len(enabled_cities)}")
            for city in failed_cities:
                print(f"   • {city}")
            sys.exit(1)
        else:
            print("\n🎉 All reports generated successfully!")
            if args.skip_pdf:
                print("Check the reports/ folder for HTML snapshots.")
            else:
                print("Check the reports/ folder for your PDFs.")
            sys.exit(0)
    
    # Validate city argument
    if not args.city:
        print("Error: City argument required (or use --all for all cities)")
        print("Use --list-cities to see available cities\n")
        parser.print_help()
        sys.exit(1)
    
    # Validate city exists in config
    cities = getattr(config, "CITIES", {})
    if args.city not in cities:
        print(f"Error: City '{args.city}' not found in config.CITIES")
        print("Use --list-cities to see available cities\n")
        sys.exit(1)
    
    city_config = cities[args.city]
    if not city_config.get("enabled", True):
        print(f"Error: City '{args.city}' is disabled in config")
        sys.exit(1)
    
    city_name = city_config.get("name", args.city)
    
    # Build command for travel_agent.py
    script_dir = Path(__file__).parent
    travel_agent = script_dir / "travel_agent.py"
    
    print("=" * 60)
    print(f"{city_name} Intelligence Brief Generator")
    print("=" * 60)
    print()
    
    cmd = [sys.executable, str(travel_agent), args.city]
    
    if args.with_driving:
        cmd.append("--will-drive")
        if args.needs_idp:
            cmd.append("--needs-idp")
        if args.rental_provider:
            cmd.extend(["--rental-provider", args.rental_provider])
        print(f"  ✓ Driving Pack enabled (needs_idp={args.needs_idp})")

    if args.limit is not None:
        cmd.extend(["--limit", str(args.limit)])
    if args.news_min_score is not None:
        cmd.extend(["--news-min-score", str(args.news_min_score)])
    if args.news_min_keywords is not None:
        cmd.extend(["--news-min-keywords", str(args.news_min_keywords)])
    if args.news_min_strong is not None:
        cmd.extend(["--news-min-strong", str(args.news_min_strong)])
    if args.news_relax_keywords:
        cmd.append("--news-relax-keywords")
    if args.news_lookback_hours is not None:
        cmd.extend(["--news-lookback-hours", str(args.news_lookback_hours)])
    if args.news_ignore_location_filter:
        cmd.append("--news-ignore-location-filter")
    if args.news_include_global_critical:
        cmd.append("--news-include-global-critical")
    if args.skip_news_preflight:
        cmd.append("--skip-news-preflight")
    if args.skip_pdf:
        cmd.append("--skip-pdf")
    
    # Execute travel_agent.py
    import subprocess
    result = subprocess.run(cmd)
    
    print()
    print("=" * 60)
    if result.returncode == 0:
        print("Report generated successfully!")
        if args.skip_pdf:
            print("Check the reports/ folder for the HTML snapshot.")
        else:
            print("Check the reports/ folder for your PDF.")
    else:
        print("Report generation failed!")
        print(f"Exit code: {result.returncode}")
    print("=" * 60)
    print()
    
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
