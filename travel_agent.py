import argparse
import json
import os
import re
import sys
import unicodedata
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, List, Sequence, Optional
from urllib.parse import urlparse

from openai import OpenAI
from pydantic import ValidationError

import config
from news_relevance import (
    STRONG_TRAVEL_KEYWORDS,
    TRAVEL_KEYWORDS,
    compute_travel_relevance,
    is_critical_event,
    normalize_text,
)
from city_context import CityContextTool
from report_schema import ReportContext
from report_renderer import render_html, render_report_pdf
from chart_utils import create_combined_trends_chart
from storage import get_datastore
from emergency_numbers import get_emergency_contacts, normalize_emergency_contacts
from format_advisory import format_advisory_text
from report_loader import ReportDataLoader
from coverage import CoverageStatus
from helpers.country_metadata import get_country_name_en
from helpers.rental_car_links import dedupe_rental_car_entries, resolve_rental_car_links_unvalidated
from report_data_contract import normalize_advisory_risk


# Fix Windows console encoding issues (prevents UnicodeEncodeError on non-ASCII city names)
if sys.platform == "win32" and hasattr(sys.stdout, 'buffer'):
    import codecs

    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, "strict")
    sys.stderr = codecs.getwriter("utf-8")(sys.stderr.buffer, "strict")

DEFAULT_EVENT_LIMIT = 10
# Default news/event window tightened to 48h to keep reports fresh
DEFAULT_LOOKBACK_HOURS = 48
DEFAULT_NEWS_MIN_SCORE = 0.5
DEFAULT_NEWS_MIN_KEYWORDS = 2
DEFAULT_NEWS_MIN_STRONG = 1
RELAXED_NEWS_MIN_SCORE = 0.3

"""
Keyword lists and relevance scoring live in news_relevance.py to keep ingestion
and report selection consistent.
"""


def _important_services_debug_enabled() -> bool:
    raw = str(os.getenv("REPORT_DEBUG_IMPORTANT_SERVICES", "")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def strip_html(s: str) -> str:
    """Remove HTML tags from string."""
    s = s or ""
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.I)
    s = re.sub(r"<[^>]+>", "", s)
    return re.sub(r"\s+", " ", s).strip()


_WEBSITE_SPLIT_RE = re.compile(r"[;\s]+")


def _normalize_website(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""

    candidates = []
    for token in _WEBSITE_SPLIT_RE.split(text):
        token = (token or "").strip()
        if not token:
            continue
        if token.lower().startswith("www."):
            token = f"https://{token}"
        if not re.match(r"^https?://", token, re.IGNORECASE):
            token = f"https://{token}"
        parsed = urlparse(token)
        if not parsed.netloc:
            continue
        normalized = parsed._replace(scheme=parsed.scheme.lower(), netloc=parsed.netloc.lower()).geturl()
        candidates.append(normalized)

    if not candidates:
        return ""

    deduped: dict[tuple[str, str, str], str] = {}
    order: list[tuple[str, str, str]] = []
    for url in candidates:
        parsed = urlparse(url)
        key = (parsed.netloc.lower(), parsed.path or "", parsed.query or "")
        existing = deduped.get(key)
        if existing:
            if existing.startswith("http://") and url.startswith("https://"):
                deduped[key] = url
            continue
        deduped[key] = url
        order.append(key)

    first_key = order[0]
    return deduped[first_key]


def _is_valid_http_url(value: str) -> bool:
    if not value:
        return False
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _should_fail_on_db_error() -> bool:
    return os.getenv("GITHUB_ACTIONS") == "true" or os.getenv("CI") == "true"


def _normalize_place_list(items: List[Dict]) -> List[Dict]:
    for item in items:
        if not isinstance(item, dict):
            continue
        if "website" in item:
            item["website"] = _normalize_website(item.get("website"))
    items.sort(key=lambda entry: 0 if entry.get("website") else 1)
    return items


def _normalize_places_context(context: Dict[str, object]) -> None:
    def _normalize_entries(entries: List[Dict], require_website: bool = True) -> List[Dict]:
        normalized = _normalize_place_list(entries)
        if not require_website:
            return normalized
        return [item for item in normalized if isinstance(item, dict) and item.get("website")]

    supplies = context.get("supplies_services")
    if isinstance(supplies, dict):
        for key in ["pharmacies", "supermarkets", "convenience_stores", "rental_cars"]:
            entries = supplies.get(key)
            if isinstance(entries, list):
                supplies[key] = _normalize_entries(entries, require_website=True)
        context["supplies_services"] = supplies

    for key in ["hospitals", "pharmacies", "supermarkets", "convenience_stores", "rental_cars"]:
        entries = context.get(key)
        if isinstance(entries, list):
            require_website = key != "hospitals"
            context[key] = _normalize_entries(entries, require_website=require_website)

    airports = context.get("airports")
    if isinstance(airports, list):
        context["airports"] = _normalize_place_list(airports)

    essentials = context.get("city_essentials")
    if isinstance(essentials, dict):
        hospitals = essentials.get("hospitals")
        if isinstance(hospitals, list):
            essentials["hospitals"] = _normalize_entries(hospitals, require_website=False)
            context["hospitals"] = essentials["hospitals"]
        airports = essentials.get("airports")
        if isinstance(airports, list):
            essentials["airports"] = _normalize_place_list(airports)
            context["airports"] = essentials["airports"]
        context["city_essentials"] = essentials

    if isinstance(supplies, dict):
        for key in ["pharmacies", "supermarkets", "convenience_stores", "rental_cars"]:
            context[key] = list(supplies.get(key) or [])


def _derive_overall_risk_from_advisory(advisory_data: Dict[str, object], is_us_destination: bool) -> str | None:
    if not advisory_data:
        return None
    advisory_text = " ".join(
        [
            advisory_data.get("summary") or "",
            advisory_data.get("title") or "",
        ]
    ).strip()
    advisory_source = advisory_data.get("advisory_source") or advisory_data.get("source")
    risk = normalize_advisory_risk(
        advisory_data.get("advisory_level"),
        advisory_text,
        advisory_source=advisory_source,
        is_us_destination=is_us_destination,
    )
    if not risk:
        return None
    if risk == "critical":
        return "high"
    if risk not in {"low", "med", "high"}:
        return "high"
    return risk


class Local10NewsClient:
    """Client for Local 10 Miami news using Supabase datastore."""
    
    def __init__(self, datastore=None) -> None:
        self.feed_config = getattr(config, "RSS_FEEDS", {}).get("local10_miami", {})
        self.datastore = datastore or get_datastore()
    
    def news_for_location(self, location: str, max_items: int = 10, lookback_hours: int = 1440) -> List[Dict]:
        """Get news articles from datastore for Miami, converted to event format."""
        # Get articles from datastore (synced separately via scripts/sync_supabase.py)
        feed_items = self.datastore.get_feed_items(source_key="local10_miami", since_hours=lookback_hours)
        
        if not feed_items:
            print("No Local10 news in datastore")
            print("  Run: python scripts/sync_supabase.py --city miami")
            return []
        
        print(f"Using {len(feed_items)} cached Local10 articles")
        
        # Convert to event shape (no hard filtering; relevance scored downstream)
        events = []
        for item in feed_items:
            title = item.title or ""
            summary = item.summary or ""
            link = item.url or ""
            event_time = item.published_at or item.fetched_at
            pub_date = event_time.isoformat() if event_time else ""
            source_name = item.raw.get("source", "Local10") if item.raw else "Local10"

            content = ""
            categories = []
            if item.raw and isinstance(item.raw, dict):
                content = item.raw.get("content_html") or item.raw.get("content") or ""
                categories = item.raw.get("categories") or item.raw.get("tags") or []

            relevance = compute_travel_relevance(
                title=title,
                summary=summary,
                content=content,
                categories=categories,
            )
            keyword_matches = len(relevance["regular_matches"])
            strong_matches = len(relevance["strong_matches"])
            magnitude = _news_magnitude(keyword_matches, strong_matches, relevance["is_critical"])

            eid = f"local10:{_norm(title)}:{pub_date or link}"
            events.append({
                "id": eid,
                "title": title,
                "summary": summary,
                "category": "news",
                "magnitude": magnitude,
                "confidence": {"score": 0.85},  # Higher confidence for local news
                "location": {"query": "Miami", "country": "US"},
                "timestamp": pub_date,
                "source": "local10_rss",
                "url": link,
                "provider": source_name,
                "keyword_matches": keyword_matches,
                "strong_matches": strong_matches,
                "travel_relevance_score": relevance["score"],
                "travel_keywords_matched": relevance["matched_keywords"],
                "travel_relevance_reason": relevance["reason"],
                "is_critical": relevance["is_critical"],
                "content": content,
                "categories": categories,
            })
        
        # Sort by strong matches first, then keyword matches
        events.sort(key=lambda e: (e.get("strong_matches", 0), e.get("keyword_matches", 0)), reverse=True)
        
        result = events[:max_items]
        if result:
            print(f"Prepared {len(result)} Local10 articles")

        return result


class TravelAdvisoryClient:
    """Client for travel advisories (Canada for US cities, US State Dept for international) using Supabase datastore."""
    
    def __init__(self, datastore=None) -> None:
        import config
        self.datastore = datastore or get_datastore()
        self.filter_country_advisories = getattr(config, "FILTER_COUNTRY_ADVISORIES", False)
    
    def get_advisory(self, country_code: str = "US", country_name: str = None) -> Dict:
        """Get travel advisory from datastore based on destination country.
        
        For US destinations: Uses Government of Canada Travel Advice
        URL: https://travel.gc.ca/destinations/united-states
        
        For non-US destinations: Uses U.S. Department of State Travel Advisories
        URL pattern: https://travel.state.gov/en/international-travel/travel-advisories/{country_slug}.html
        
        Args:
            country_code: ISO country code (e.g., 'US', 'FR', 'GB')
            country_name: Full country name for matching (e.g., 'France', 'United Kingdom')
        
        Returns:
            Advisory dict with:
            - advisory_source: 'STATE_DEPT' or 'CANADA'
            - advisory_level: int (1-4 for State Dept, 0-3 for Canada)
            - travel_advisory_url: official URL
            - title, summary, published fields
        """
        from helpers.country_metadata import get_country_name_en

        def _extract_summary(item) -> str:
            summary = (item.summary or "").strip()
            raw = item.raw or {}
            if not summary and isinstance(raw, dict):
                for key in ("summary", "content_html", "content"):
                    value = raw.get(key)
                    if isinstance(value, str) and value.strip():
                        summary = value.strip()
                        break
            return summary

        def _pick_best(items: List[object]) -> Optional[object]:
            if not items:
                return None
            return max(items, key=lambda it: (_effective_timestamp(it), len(_extract_summary(it))))

        def _effective_timestamp(item: object) -> float:
            candidate = getattr(item, "published_at", None) or getattr(item, "fetched_at", None)
            if isinstance(candidate, datetime):
                if candidate.tzinfo is None:
                    candidate = candidate.replace(tzinfo=timezone.utc)
                return candidate.timestamp()
            return 0.0

        country_code = (country_code or "").strip().upper()
        if not country_name:
            resolved_name = get_country_name_en(country_code)
            country_name = resolved_name or country_name

        # Mapping of country names to official State Department URL slugs
        # Source: https://travel.state.gov/en/international-travel/travel-advisories/
        COUNTRY_SLUG_MAP = {
            "czech republic": "czechia",
            "czechia": "czechia",
            "germany": "germany",
            "deutschland": "germany",
            "italy": "italy",
            "portugal": "portugal",
            "france": "france",
            "spain": "spain",
            "russia": "russia",
            "russian federation": "russia",
            "united arab emirates": "united-arab-emirates",
            "uae": "united-arab-emirates",
            "united kingdom": "united-kingdom",
            "united states": "united-states",
        }

        COUNTRY_INFO_URL_MAP = {
            "united-arab-emirates": "https://travel.state.gov/content/travel/en/traveladvisories/traveladvisories/united-arab-emirates-travel-advisory.html",
        }

        def _slug_from_name(value: str) -> str:
            country_name_key = normalize_text(value)
            expected_slug = COUNTRY_SLUG_MAP.get(country_name_key)
            if not expected_slug and country_name_key:
                expected_slug = country_name_key.replace(" ", "-")
            return expected_slug

        def _slug_from_url(value: str) -> str:
            if not value:
                return ""
            try:
                parsed = urlparse(value)
            except Exception:
                return ""
            path = parsed.path or ""
            marker = "/international-travel/travel-advisories/"
            if marker not in path:
                return ""
            slug_part = path.split(marker, 1)[-1]
            if slug_part.endswith(".html"):
                slug_part = slug_part[: -len(".html")]
            return slug_part.strip("/")
        
        # For US destinations, use Government of Canada travel advisories
        if country_code == "US":
            feed_items = self.datastore.get_feed_items(source_key="canada_travel_advisories")
            
            if not feed_items:
                print("No Canada travel advisory in datastore")
                print("  Run: python scripts/sync_supabase.py --all")
                return {}
            
            # Filter by country when enabled
            if self.filter_country_advisories:
                # Look for U.S. advisory from Canada feed
                matches = []
                for item in feed_items:
                    item_country = item.raw.get("country_code", "") if item.raw else ""
                    if item_country.upper() == "US":
                        matches.append(item)
                best = _pick_best(matches)
                if best:
                    return {
                        "advisory_source": "CANADA",
                        "advisory_level": best.raw.get("advisory_state", 0) if best.raw else 0,
                        "travel_advisory_url": "https://travel.gc.ca/destinations/united-states",
                        "title": best.title,
                        "link": best.url or "https://travel.gc.ca/destinations/united-states",
                        "summary": _extract_summary(best),
                        "published": (best.published_at or best.fetched_at).isoformat() if (best.published_at or best.fetched_at) else "",
                        "source": "Government of Canada Travel Advisories",
                    }
                # No U.S.-specific advisory found; fall back to best item
                best = _pick_best(feed_items)
                if best:
                    return {
                        "advisory_source": "CANADA",
                        "advisory_level": best.raw.get("advisory_state", 0) if best.raw else 0,
                        "travel_advisory_url": "https://travel.gc.ca/destinations/united-states",
                        "title": best.title,
                        "link": best.url or "https://travel.gc.ca/destinations/united-states",
                        "summary": _extract_summary(best),
                        "published": (best.published_at or best.fetched_at).isoformat() if (best.published_at or best.fetched_at) else "",
                        "source": "Government of Canada Travel Advisories",
                    }
                return {}
            else:
                # Fallback to best item when filtering is disabled
                best = _pick_best(feed_items)
                if best:
                    return {
                        "advisory_source": "CANADA",
                        "advisory_level": best.raw.get("advisory_state", 0) if best.raw else 0,
                        "travel_advisory_url": "https://travel.gc.ca/destinations/united-states",
                        "title": best.title,
                        "link": best.url or "https://travel.gc.ca/destinations/united-states",
                        "summary": _extract_summary(best),
                        "published": (best.published_at or best.fetched_at).isoformat() if (best.published_at or best.fetched_at) else "",
                        "source": "Government of Canada Travel Advisories",
                    }
        
        # For all non-Canada destinations, use US State Department travel advisories (global feed)
        else:
            # Don't filter by time - advisories can be old but still valid
            feed_items = self.datastore.get_feed_items(source_key="us_travel_advisories")
            
            if not feed_items:
                print("No US State Department travel advisory in datastore")
                print("  Run: python scripts/sync_supabase.py --all")
                return {}

            def build_state_dept_payload(item) -> Dict:
                # Build canonical State Dept URL from country_slug
                advisory_url = ""
                slug = ""
                
                # Priority 1: item.raw["country_slug"]
                if item.raw and item.raw.get("country_slug"):
                    slug = item.raw.get("country_slug")
                # Priority 2: derive from item.raw["country_name"] using COUNTRY_SLUG_MAP
                elif item.raw and item.raw.get("country_name"):
                    slug = _slug_from_name(item.raw.get("country_name", ""))
                # Priority 3: derive from resolved country_name using COUNTRY_SLUG_MAP
                elif country_name:
                    slug = _slug_from_name(country_name)
                # Priority 4: derive from item.url when it is a State Dept advisory
                if not slug and item.url:
                    slug = _slug_from_url(item.url)
                
                # Build canonical URL from slug
                if slug:
                    advisory_url = COUNTRY_INFO_URL_MAP.get(
                        slug,
                        f"https://travel.state.gov/en/international-travel/travel-advisories/{slug}.html",
                    )
                elif item.url and "travel.state.gov/en/international-travel/travel-advisories/" in item.url:
                    advisory_url = item.url
                
                return {
                    "advisory_source": "STATE_DEPT",
                    "advisory_level": item.raw.get("advisory_level", 0) if item.raw else 0,
                    "travel_advisory_url": advisory_url,
                    "title": item.title,
                    "link": item.url,
                    "summary": _extract_summary(item) or "No summary available",
                    "published": (item.published_at or item.fetched_at).isoformat() if (item.published_at or item.fetched_at) else "",
                    "source": "U.S. Department of State Travel Advisories",
                }
            
            # Try to find advisory for the specific country
            if country_name:
                # Build alias list for country matching
                country_aliases = [normalize_text(country_name)]
                
                # Add known aliases for specific countries for title matching
                if (country_code or "").upper() == "CZ":
                    country_aliases.extend(["czech republic", "czechia"])
                elif (country_code or "").upper() == "PT":
                    country_aliases.extend(["portugal"])
                elif (country_code or "").upper() == "FR":
                    country_aliases.extend(["france"])
                elif (country_code or "").upper() == "DE":
                    country_aliases.extend(["germany", "deutschland", "federal republic of germany"])
                normalized_aliases = {normalize_text(alias) for alias in country_aliases if alias}
                scored_matches = []
                for item in feed_items:
                    raw = item.raw if isinstance(getattr(item, "raw", None), dict) else {}
                    raw_country = normalize_text(str(raw.get("country_name") or ""))
                    title_norm = normalize_text(item.title or "")

                    score = 0
                    if raw_country and raw_country in normalized_aliases:
                        score += 2
                    if normalized_aliases and any(alias and alias in title_norm for alias in normalized_aliases):
                        score += 1

                    if score > 0:
                        scored_matches.append((score, _effective_timestamp(item), len(_extract_summary(item)), item))

                if scored_matches:
                    scored_matches.sort(key=lambda row: (row[0], row[1], row[2]), reverse=True)
                    return build_state_dept_payload(scored_matches[0][3])

                if self.filter_country_advisories:
                    return {}
            
            # Return first advisory if no country match and filtering is disabled
            if not self.filter_country_advisories and feed_items:
                best = _pick_best(feed_items)
                if best:
                    return build_state_dept_payload(best)
        
        return {}


class GDELTNewsClient:
    """Client for GDELT geolocation-based news using Supabase datastore."""
    
    def __init__(self, datastore=None) -> None:
        self.datastore = datastore or get_datastore()
    
    def news_for_location(self, location: str, city_key: str, max_items: int = 20, lookback_hours: int = 72) -> List[Dict]:
        """Get GDELT news articles from datastore for a city, converted to event format."""
        # Get articles from datastore (synced separately via scripts/sync_supabase.py)
        feed_items = self.datastore.get_feed_items(
            source_key="gdelt_geo",
            city_key=city_key,
            since_hours=lookback_hours
        )
        
        if not feed_items:
            print(f"No GDELT news in datastore for {city_key}")
            return []
        
        print(f"Using {len(feed_items)} cached GDELT articles")
        
        # Convert to event shape (no hard filtering; relevance scored downstream)
        events = []
        for item in feed_items:
            title = item.title or ""
            summary = item.summary or ""
            link = item.url or ""
            event_time = item.published_at or item.fetched_at
            pub_date = event_time.isoformat() if event_time else ""
            domain = item.raw.get("domain", "") if item.raw else ""

            content = ""
            categories = []
            if item.raw and isinstance(item.raw, dict):
                content = item.raw.get("content_html") or item.raw.get("content") or ""
                categories = item.raw.get("categories") or item.raw.get("tags") or []

            relevance = compute_travel_relevance(
                title=title,
                summary=summary,
                content=content,
                categories=categories,
            )
            keyword_matches = len(relevance["regular_matches"])
            strong_matches = len(relevance["strong_matches"])
            magnitude = _news_magnitude(keyword_matches, strong_matches, relevance["is_critical"])

            eid = f"gdelt:{city_key}:{_norm(title)}:{pub_date or link}"
            events.append({
                "id": eid,
                "title": title,
                "summary": f"{summary} (Source: {domain})" if domain else summary,
                "category": "news",
                "magnitude": magnitude,
                "confidence": {"score": 0.75},  # Slightly lower than local news
                "location": {"query": location, "country": ""},
                "timestamp": pub_date,
                "source": "gdelt_geo",
                "url": link,
                "provider": f"GDELT ({domain})" if domain else "GDELT",
                "keyword_matches": keyword_matches,
                "strong_matches": strong_matches,
                "travel_relevance_score": relevance["score"],
                "travel_keywords_matched": relevance["matched_keywords"],
                "travel_relevance_reason": relevance["reason"],
                "is_critical": relevance["is_critical"],
                "content": content,
                "categories": categories,
            })
        
        # Sort by strong matches first, then keyword matches
        events.sort(key=lambda e: (e.get("strong_matches", 0), e.get("keyword_matches", 0)), reverse=True)
        
        result = events[:max_items]
        if result:
            print(f"Prepared {len(result)} GDELT articles")

        return result


class WeatherAlertsClient:
    """Client for National Weather Service alerts using Supabase datastore."""
    
    def __init__(self, datastore=None) -> None:
        self.datastore = datastore or get_datastore()
    
    def alerts_for_location(self, location: str, city_key: str, lookback_hours: int = 48) -> List[Dict]:
        """Get NWS weather alerts from datastore for a city, converted to event format."""
        # Get alerts from datastore (synced separately via scripts/sync_supabase.py)
        feed_items = self.datastore.get_feed_items(
            source_key="nws_alerts",
            city_key=city_key,
            since_hours=lookback_hours
        )
        
        if not feed_items:
            # No alerts is actually good news
            return []
        
        print(f"Using {len(feed_items)} active NWS weather alerts")
        
        # Convert to event shape
        events = []
        for item in feed_items:
            title = item.title or ""
            summary = item.summary or ""
            link = item.url or ""
            event_time = item.published_at or item.fetched_at
            pub_date = event_time.isoformat() if event_time else ""
            
            # Extract severity and magnitude from raw data
            raw = item.raw or {}
            severity = raw.get("severity", "Unknown")
            urgency = raw.get("urgency", "Unknown")
            magnitude = raw.get("magnitude", 3.0)  # Pre-calculated in source fetcher
            event_type = raw.get("event", "Weather Alert")
            
            eid = f"nws:{city_key}:{_norm(title)}:{pub_date or link}"
            
            events.append({
                "id": eid,
                "title": title,
                "summary": summary,
                "category": "weather",
                "magnitude": magnitude,
                "confidence": {"score": 0.95},  # High confidence - official government alerts
                "location": {"query": location, "country": "US"},
                "timestamp": pub_date,
                "source": "nws_alerts",
                "url": link,
                "provider": f"NWS ({event_type})",
                "severity": severity,
                "urgency": urgency,
            })
        
        if events:
            print(f"Loaded {len(events)} NWS weather alerts")
        
        return events


class WeatherForecastClient:
    """Client for Open-Meteo weather forecasts using Supabase datastore (schema v2)."""
    
    def __init__(self, datastore=None) -> None:
        self.datastore = datastore or get_datastore()
    
    def get_forecast(self, location: str, city_key: str) -> Dict:
        """
        Get weather forecast from datastore for a city (schema v2).
        
        Schema v2 changes:
        - Reads from weather_forecasts table instead of feed_items
        - Uses get_latest_weather_forecast() method
        """
        # Get latest forecast from weather_forecasts table (schema v2)
        forecast_record = self.datastore.get_latest_weather_forecast(city_key, provider="open_meteo")
        
        if not forecast_record:
            print(f"No weather forecast in datastore for {city_key}")
            return {}
        
        # Extract raw forecast data
        raw_data = forecast_record.get("raw", {})
        
        print(f"Loaded weather forecast for {location}")
        
        # Extract current conditions
        current = raw_data.get("current", {})
        current_summary = {
            "temperature": current.get("temperature_2m"),
            "apparent_temperature": current.get("apparent_temperature"),
            "wind_speed": current.get("wind_speed_10m"),
            "wind_gusts": current.get("wind_gusts_10m"),
            "precipitation": current.get("precipitation"),
            "weather_code": current.get("weather_code"),
            "condition": self._decode_weather_code(current.get("weather_code", 0)),
        }
        
        # Extract daily forecast (7 days)
        daily = raw_data.get("daily", {})
        daily_forecast = []
        
        if daily and "time" in daily:
            for i in range(len(daily["time"])):
                day_data = {
                    "date": daily["time"][i],
                    "max_temp": daily.get("temperature_2m_max", [])[i] if i < len(daily.get("temperature_2m_max", [])) else None,
                    "min_temp": daily.get("temperature_2m_min", [])[i] if i < len(daily.get("temperature_2m_min", [])) else None,
                    "precipitation_sum": daily.get("precipitation_sum", [])[i] if i < len(daily.get("precipitation_sum", [])) else None,
                    "precipitation_probability_max": daily.get("precipitation_probability_max", [])[i] if i < len(daily.get("precipitation_probability_max", [])) else None,
                    "weather_code": daily.get("weather_code", [])[i] if i < len(daily.get("weather_code", [])) else None,
                }
                day_data["condition"] = self._decode_weather_code(day_data.get("weather_code", 0))
                daily_forecast.append(day_data)
        
        return {
            "current": current_summary,
            "daily": daily_forecast,
            "units": raw_data.get("daily_units", {}),
            "timezone": raw_data.get("timezone", "UTC"),
        }
    
    @staticmethod
    def _decode_weather_code(code: int) -> str:
        """Convert WMO weather code to human-readable description."""
        # WMO Weather interpretation codes (WW)
        weather_codes = {
            0: "Clear sky",
            1: "Mainly clear",
            2: "Partly cloudy",
            3: "Overcast",
            45: "Foggy",
            48: "Depositing rime fog",
            51: "Light drizzle",
            53: "Moderate drizzle",
            55: "Dense drizzle",
            56: "Light freezing drizzle",
            57: "Dense freezing drizzle",
            61: "Slight rain",
            63: "Moderate rain",
            65: "Heavy rain",
            66: "Light freezing rain",
            67: "Heavy freezing rain",
            71: "Slight snow",
            73: "Moderate snow",
            75: "Heavy snow",
            77: "Snow grains",
            80: "Slight rain showers",
            81: "Moderate rain showers",
            82: "Violent rain showers",
            85: "Slight snow showers",
            86: "Heavy snow showers",
            95: "Thunderstorm",
            96: "Thunderstorm with slight hail",
            99: "Thunderstorm with heavy hail",
        }
        return weather_codes.get(code, f"Unknown ({code})")


# Removed search_miami_transit() - transit data now comes exclusively from transit_snapshots table


def sanitize_text(text: str) -> str:
    """Replaces special unicode characters with standard ASCII equivalents."""
    replacements = {
        "’": "'",
        "‘": "'",
        "“": '"',
        "”": '"',
        "—": "--",
        "–": "-",
        "‑": "-",  # non-breaking hyphen
        "•": "*",
        "…": "...",
        "€": "EUR",
        "£": "GBP",
        "¢": "c",
        "™": "(TM)",
        "®": "(R)",
        "©": "(c)",
        "°": " degrees",
        "×": "x",
        "÷": "/",
    }
    for uni_char, ascii_char in replacements.items():
        text = text.replace(uni_char, ascii_char)

    # Preserve Unicode characters (do not strip non-latin-1); return sanitized text
    return text


def _norm(s: str) -> str:
    """Normalize text for matching: strip accents, lowercase, clean punctuation."""
    s = s or ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    # Keep Unicode letters/digits, convert underscores to spaces, remove remaining non-word chars
    s = s.replace("_", " ")
    s = re.sub(r"[^\w]+", " ", s, flags=re.UNICODE)
    return re.sub(r"\s+", " ", s).strip()


def _keyword_match_counts(text_blob: str) -> tuple[int, int]:
    keyword_matches = sum(1 for kw in TRAVEL_KEYWORDS if kw in text_blob)
    strong_matches = sum(1 for kw in STRONG_TRAVEL_KEYWORDS if kw in text_blob)
    return keyword_matches, strong_matches


def _event_keyword_counts(event: Dict) -> tuple[int, int]:
    keyword_matches = event.get("keyword_matches")
    strong_matches = event.get("strong_matches")
    if isinstance(keyword_matches, int) and isinstance(strong_matches, int):
        return keyword_matches, strong_matches
    matched = event.get("travel_keywords_matched")
    if isinstance(matched, list) and matched:
        keyword_matches = sum(1 for kw in matched if kw in TRAVEL_KEYWORDS)
        strong_matches = sum(1 for kw in matched if kw in STRONG_TRAVEL_KEYWORDS)
        return keyword_matches, strong_matches
    text_blob = _norm(f"{event.get('title', '')} {event.get('summary', '')}")
    return _keyword_match_counts(text_blob)

def _parse_event_timestamp(value: Optional[object]) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except Exception:
            try:
                dt = parsedate_to_datetime(text)
            except Exception:
                return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _format_event_timestamp(value: Optional[object]) -> str:
    dt = _parse_event_timestamp(value)
    if dt:
        return dt.isoformat()
    if value is None:
        return ""
    return str(value).strip()


def location_aliases(location: str, city_config: Optional[Dict] = None) -> List[str]:
    """
    Generate location aliases for matching (e.g., 'Paris, France' -> ['paris', 'france']).
    If city_config is provided, includes aliases from config (handles Portuguese names).
    
    Returns both accent-normalized and accent-folded versions for Portuguese support.
    """
    from news_relevance import fold_accents
    
    # Prefer city-level alias only (e.g., 'Paris' not 'France') to avoid broad country matches.
    parts = [p.strip() for p in (location or "").split(",") if p.strip()]
    city_part = parts[0] if parts else (location or "")
    aliases = {_norm(city_part)}

    # Lisbon-specific alias expansion (Portuguese naming + country references).
    # Keep scoped to Lisbon to avoid broad country matches for other cities.
    is_lisbon = False
    if city_config:
        city_name_norm = _norm(str(city_config.get("name") or ""))
        cfg_aliases = city_config.get("aliases", [])
        cfg_aliases_norm = [_norm(a) for a in (cfg_aliases or [])]
        if (
            "lisbon" in city_name_norm
            or "lisboa" in city_name_norm
            or "lisbon" in cfg_aliases_norm
            or "lisboa" in cfg_aliases_norm
        ):
            is_lisbon = True
    if not is_lisbon:
        loc_norm = _norm(location)
        if "lisbon" in loc_norm or "lisboa" in loc_norm:
            is_lisbon = True
    if is_lisbon:
        for alias in ("Lisboa", "Grande Lisboa", "Portugal", "PT"):
            aliases.add(_norm(alias))
            aliases.add(fold_accents(alias))
    
    # Add config aliases if provided (for Portuguese and other multilingual city names)
    if city_config and "aliases" in city_config:
        for alias in city_config["aliases"]:
            aliases.add(_norm(alias))
            # Also add accent-folded version for Portuguese matching (São -> Sao, Lisboa -> lisboa)
            aliases.add(fold_accents(alias))

    # Drop empty/duplicates
    return [a for a in aliases if a]


def _location_blob_for_matching(loc_value: object) -> str:
    """
    Builds a location text blob safe for matching.
    IMPORTANT: excludes 'query' because many events store the requested query there,
    which would make every event look like it matches the city.
    """
    if isinstance(loc_value, dict):
        parts = []
        for k, v in loc_value.items():
            if not v:
                continue
            key = str(k).strip().lower()
            if key in ("query", "requested_location", "city_query"):
                continue
            parts.append(str(v))
        return " ".join(parts).strip()
    if loc_value:
        return str(loc_value).strip()
    return ""


def filter_events_by_location(
    events: Sequence[Dict],
    location: str,
    *,
    include_global_critical: bool = False,
    min_strong_matches: int = DEFAULT_NEWS_MIN_STRONG,
    city_config: Optional[Dict] = None,
) -> List[Dict]:
    """
    Filter events that mention the location or its aliases. No fallback to all events.
    
    Supports Portuguese text with accent folding (Lisboa/lisboa, São/Sao match).
    """
    from news_relevance import fold_accents
    
    if not events:
        return []

    aliases = location_aliases(location, city_config)
    filtered: List[Dict] = []

    for event in events:
        loc_blob = _location_blob_for_matching(event.get("location"))

        haystack = _norm(
            " ".join(
                [
                    str(event.get("title", "")),
                    str(event.get("summary", "")),
                    str(event.get("content", "")),
                    loc_blob,
                ]
            )
        )

        haystack_folded = fold_accents(haystack)

        matched = False
        for alias in aliases:
            pattern = rf"\b{re.escape(alias)}\b"
            if re.search(pattern, haystack) or re.search(pattern, haystack_folded):
                matched = True
                break

        if matched:
            filtered.append(event)
            continue

        if include_global_critical:
            _, strong_matches = _event_keyword_counts(event)
            if strong_matches >= min_strong_matches:
                filtered.append(event)

    return filtered


def _incident_recency_label(event_time: Optional[datetime]) -> Optional[str]:
    if not event_time or event_time.year <= 1900:
        return None
    today = datetime.now(timezone.utc).date()
    delta = (event_time.date() - today).days
    if delta == 0:
        return "today"
    if delta > 0:
        return "in 1 day" if delta == 1 else f"in {delta} days"
    days = abs(delta)
    return "1 day ago" if days == 1 else f"{days} days ago"


def _format_incidents_from_events(
    events: Sequence[Dict],
    *,
    limit: int = 5,
    mark_fallback: bool = False,
) -> List[Dict]:
    if not events:
        return []
    sorted_events = sorted(
        events,
        key=lambda event: _parse_event_timestamp(event.get("timestamp"))
        or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    incidents: List[Dict] = []
    for event in sorted_events[:limit]:
        event_time = _parse_event_timestamp(event.get("timestamp"))
        date_str = event_time.date().isoformat() if event_time else ""
        incident = {
            "date": date_str,
            "recency": _incident_recency_label(event_time),
            "type": "News",
            "impact": event.get("title") or event.get("summary") or "",
            "where": "",
            "source": event.get("provider") or event.get("source") or "",
        }
        if mark_fallback:
            incident["is_fallback"] = True
        incidents.append(incident)
    return incidents


def _apply_incident_fallback(
    parsed_data: Dict,
    *,
    local_events: Sequence[Dict],
    selected_events: Sequence[Dict],
) -> bool:
    incidents = parsed_data.get("incidents")
    if incidents:
        return False
    if not local_events:
        return False
    fallback_events = selected_events if selected_events else local_events
    fallback_incidents = _format_incidents_from_events(
        fallback_events,
        limit=5,
        mark_fallback=True,
    )
    if not fallback_incidents:
        return False
    parsed_data["incidents"] = fallback_incidents
    return True


def _to_float(x) -> float:
    """Safe float conversion."""
    try:
        return float(x)
    except Exception:
        return 0.0


def _news_magnitude(keyword_matches: int, strong_matches: int, is_critical: bool) -> float:
    if keyword_matches <= 0 and strong_matches <= 0:
        base = 1.0
    else:
        base = 2.0 + strong_matches * 1.0 + keyword_matches * 0.3
    if is_critical:
        base = max(base, 3.0)
    return min(base, 5.0)


def travel_relevance_score(event: Dict, location: str, city_config: Optional[Dict] = None) -> float:
    """Score how relevant an event is for travelers to a specific location."""
    title = str(event.get("title", ""))
    summary = str(event.get("summary", ""))
    content = str(event.get("content", ""))
    categories = event.get("categories")
    raw = event.get("raw")
    if categories is None and isinstance(raw, dict):
        categories = raw.get("categories") or raw.get("tags")

    relevance_score = event.get("travel_relevance_score")
    is_critical = event.get("is_critical")
    if relevance_score is None or is_critical is None:
        relevance = compute_travel_relevance(
            title=title,
            summary=summary,
            content=content,
            categories=categories,
        )
        if relevance_score is None:
            relevance_score = relevance["score"]
        if is_critical is None:
            is_critical = relevance["is_critical"]

    score = _to_float(relevance_score)

    if score > 0 or is_critical:
        score += 0.5 * _to_float((event.get("confidence") or {}).get("score"))
        score += 0.3 * _to_float(event.get("magnitude"))

        aliases = location_aliases(location, city_config)
        if aliases:
            loc_blob = _location_blob_for_matching(event.get("location"))
            text = _norm(" ".join([title, summary, loc_blob]))

            if any(re.search(rf"\b{re.escape(alias)}\b", text) for alias in aliases):
                score += 1.5

    return score


def select_top_travel_events(
    events: Sequence[Dict],
    location: str,
    *,
    max_events: int = DEFAULT_EVENT_LIMIT,
    min_score: float = DEFAULT_NEWS_MIN_SCORE,
    relax_keywords: bool = False,
    min_keyword_matches: int = DEFAULT_NEWS_MIN_KEYWORDS,
    min_strong_matches: int = DEFAULT_NEWS_MIN_STRONG,
    city_config: Optional[Dict] = None,
) -> tuple[List[Dict], Dict[str, int]]:
    """Select the most travel-relevant events, filtering out low-value items."""
    stats = {
        "eligible": 0,
        "kept": 0,
        "recovered_by_fallback": 0,
        "dropped_below_score": 0,
        "dropped_over_limit": 0,
        "kept_relaxed_keywords": 0,
        "kept_relaxed_score": 0,
        "dropped_keyword_mismatch": 0,
    }

    kept_candidates: list[tuple[float, Dict]] = []
    remaining_scored: list[tuple[float, Dict]] = []
    for event in events:
        category = (event.get("category") or "news").lower()
        keyword_matches, strong_matches = _event_keyword_counts(event)
        high_confidence = (
            strong_matches >= min_strong_matches
            or keyword_matches >= min_keyword_matches
        )

        is_critical = event.get("is_critical")
        if is_critical is None:
            is_critical = is_critical_event(
                title=event.get("title", ""),
                summary=event.get("summary", ""),
                content=event.get("content", ""),
                categories=event.get("categories"),
            )

        score = travel_relevance_score(event, location, city_config=city_config)
        keep = score >= min_score or bool(is_critical)
        keep_reason = None

        if category == "news" and high_confidence and not keep:
            keep = True
            keep_reason = "high_confidence"

        if not keep and relax_keywords:
            if strong_matches >= 1 or keyword_matches >= 1:
                keep = True
                keep_reason = "keywords"
            elif score >= min(RELAXED_NEWS_MIN_SCORE, min_score):
                keep = True
                keep_reason = "relaxed_score"

        if keep:
            kept_candidates.append((score, event))
            if keep_reason == "keywords":
                stats["kept_relaxed_keywords"] += 1
            elif keep_reason == "relaxed_score":
                stats["kept_relaxed_score"] += 1
        else:
            remaining_scored.append((score, event))

    def _score_sort_key(item: tuple[float, Dict]) -> tuple[float, datetime]:
        score, event = item
        ts = _parse_event_timestamp(event.get("timestamp"))
        if ts is None:
            ts = datetime.min.replace(tzinfo=timezone.utc)
        return (score, ts)

    candidates_count = len(kept_candidates) + len(remaining_scored)
    stats["eligible"] = candidates_count

    if len(kept_candidates) < max_events and remaining_scored:
        remaining_scored.sort(key=_score_sort_key, reverse=True)
        fallback_threshold = min_score * 0.5
        idx = 0
        while len(kept_candidates) < max_events and idx < len(remaining_scored):
            added = 0
            while (
                idx < len(remaining_scored)
                and remaining_scored[idx][0] >= fallback_threshold
                and len(kept_candidates) < max_events
            ):
                kept_candidates.append(remaining_scored[idx])
                stats["recovered_by_fallback"] += 1
                idx += 1
                added += 1
            if len(kept_candidates) >= max_events or idx >= len(remaining_scored):
                break
            if added == 0:
                fallback_threshold *= 0.5
                if fallback_threshold <= 0:
                    while len(kept_candidates) < max_events and idx < len(remaining_scored):
                        kept_candidates.append(remaining_scored[idx])
                        stats["recovered_by_fallback"] += 1
                        idx += 1
                    break

    stats["dropped_below_score"] = candidates_count - len(kept_candidates)

    kept_candidates.sort(key=_score_sort_key, reverse=True)
    if len(kept_candidates) > max_events:
        stats["dropped_over_limit"] = len(kept_candidates) - max_events
        kept_candidates = kept_candidates[:max_events]

    stats["kept"] = len(kept_candidates)
    return [e for _, e in kept_candidates], stats


def format_events_for_prompt(events: Sequence[Dict], limit: int = 15) -> List[Dict]:
    trimmed = []
    for event in events[:limit]:
        timestamp = _format_event_timestamp(event.get("timestamp"))
        trimmed.append(
            {
                "id": event.get("id"),
                "title": event.get("title"),
                "summary": event.get("summary"),
                "category": event.get("category"),
                "magnitude": event.get("magnitude"),
                "confidence": (event.get("confidence") or {}).get("score"),
                "location": event.get("location"),
                "timestamp": timestamp,
                "source": event.get("source", "news"),
                "url": event.get("url"),
            }
        )
    return trimmed


def ensure_city_news_ready(
    datastore,
    city_key: str,
    *,
    min_items: int = 1,
    lookback_days: int = 30,
) -> bool:
    """Check for recent feed_items joined to a city before report generation."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    try:
        client = getattr(datastore, "client", None)
        if client is None:
            raise RuntimeError("Datastore client unavailable")

        response = (
            client.table("feed_items")
            .select("id, published_at, fetched_at, feed_item_cities!inner(city_key)")
            .eq("feed_item_cities.city_key", city_key)
            .gte("published_at", cutoff.isoformat())
            .limit(min_items)
            .execute()
        )
        items = {row.get("id"): row for row in (response.data or []) if row.get("id")}

        if len(items) < min_items:
            fallback_response = (
                client.table("feed_items")
                .select("id, published_at, fetched_at, feed_item_cities!inner(city_key)")
                .eq("feed_item_cities.city_key", city_key)
                .is_("published_at", "null")
                .gte("fetched_at", cutoff.isoformat())
                .limit(min_items)
                .execute()
            )
            for row in (fallback_response.data or []):
                item_id = row.get("id")
                if item_id and item_id not in items:
                    items[item_id] = row

        if len(items) < min_items:
            print(f"No recent feed items found for {city_key} in the last {lookback_days} days.")
            print(f"Run: python scripts/sync_supabase.py --city {city_key}")
            return False
        return True
    except Exception as e:
        print(f"News preflight failed for {city_key}: {e}")
        print(f"Run: python scripts/sync_supabase.py --city {city_key}")
        return False


class TravelIntelAgent:
    def __init__(self, api_key: str, datastore=None) -> None:
        self.api_key = api_key
        # Increase timeout to 5 minutes to handle network issues
        self.client = OpenAI(api_key=api_key, timeout=300.0, max_retries=3)
        self.datastore = datastore or get_datastore()
        self.local10_client = Local10NewsClient(datastore=self.datastore)
        self.travel_advisory = TravelAdvisoryClient(datastore=self.datastore)
        self.gdelt_client = GDELTNewsClient(datastore=self.datastore)
        self.weather_client = WeatherAlertsClient(datastore=self.datastore)
        self.forecast_client = WeatherForecastClient(datastore=self.datastore)
        self.city_context = CityContextTool(openai_api_key=api_key, datastore=self.datastore)
    
    def _location_to_city_key(self, location: str) -> str:
        """
        Convert location string to city_key for datastore lookups.
        
        Tries to match against configured cities, falls back to normalized location.
        """
        location_lower = location.lower()
        
        # Try to match against configured cities
        for key, city_config in getattr(config, "CITIES", {}).items():
            city_name = city_config.get("name", "").lower()
            aliases = [a.lower() for a in city_config.get("aliases", [])]

            if location_lower in aliases or location_lower in city_name:
                return key

        # DB-first fallback: match against cities table (display_name + aliases)
        try:
            if hasattr(self.datastore, "get_cities"):
                for row in (self.datastore.get_cities() or []):
                    if not isinstance(row, dict):
                        continue
                    display = (row.get("display_name") or "").lower()
                    aliases = row.get("aliases") if isinstance(row.get("aliases"), list) else []
                    aliases_lower = [str(a).lower() for a in aliases]
                    if location_lower in aliases_lower or (display and location_lower in display):
                        ck = row.get("city_key")
                        if ck:
                            return ck
        except Exception:
            pass
        
        # Fallback: normalize location to key
        safe_name = location.lower().replace(" ", "_").replace(",", "")
        safe_name = "".join(c for c in safe_name if c.isalnum() or c == "_")
        return safe_name

    def get_transit_info(self, location: str) -> Optional[Dict]:
        """Get transit information with caching (30-day TTL) using datastore."""
        # Convert location to city_key
        city_key = self._location_to_city_key(location)
        
        # Try to get from datastore
        snapshot = self.datastore.get_transit_snapshot(city_key)
        
        if snapshot:
            age_days = (datetime.now(timezone.utc) - snapshot.fetched_at).total_seconds() / (24 * 60 * 60)
            if age_days < 30:
                print(f"  ✓ Using cached transit info from datastore ({int(age_days)}d old)")
                return snapshot.transit
        
        # Fallback: Transit data not in datastore
        print(f"  ℹ No transit data in datastore for {city_key}")
        print(f"  ℹ Run 'python scripts/sync_supabase.py --city {city_key}' to populate transit data")
        
        # Generic fallback
        city_name = location.split(',')[0].strip()
        generic = {
            "name": f"{city_name} Public Transportation",
            "modes": ["Bus"],
            "website": "",
            "description": "Check local transit authority for routes and schedules",
            "taxis": "Local taxi services available",
            "rideshare": "Uber and Lyft typically available in major cities"
        }
        print(f"  ℹ Using generic transit fallback for {city_name}")
        return generic

    def collect_events(
        self,
        location: str,
        limit: int = DEFAULT_EVENT_LIMIT,
        lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
        city_key: Optional[str] = None,
        min_keyword_matches: int = DEFAULT_NEWS_MIN_KEYWORDS,
        min_strong_matches: int = DEFAULT_NEWS_MIN_STRONG,
        return_stats: bool = False,
    ) -> List[Dict] | tuple[List[Dict], Dict[str, int]]:
        """
        Collect travel-impacting events from database using feed_item_cities join.
        
        No early filtering based on keywords - all items are collected, scored,
        and filtered happens only in select_top_travel_events. Critical items
        (is_critical=True) are preserved regardless of keyword counts.
        """
        # Determine city_key for datastore queries
        city_key = (city_key or "").strip() or self._location_to_city_key(location)
        
        all_events = []
        
        # Use DB join to get all feed items for this city
        cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
        
        try:
            print(f"Collecting feed items for city: {city_key}")
            
            # Get feed item IDs for this city via join table
            mappings = self.datastore.client.table("feed_item_cities") \
                .select("feed_item_id") \
                .eq("city_key", city_key) \
                .execute()
            
            if not mappings.data:
                print(f"  ⚠ No feed item mappings found for {city_key}")
                return ([], {"scanned_events": 0, "dropped_keyword_mismatch": 0}) if return_stats else []
            
            item_ids = [m['feed_item_id'] for m in mappings.data]
            
            # Get feed items using published_at with fetched_at fallback
            items_by_id: Dict[str, Dict] = {}
            
            # Query 1: Items with published_at >= cutoff
            response = (
                self.datastore.client.table("feed_items")
                .select("*")
                .in_("id", item_ids)
                .gte("published_at", cutoff.isoformat())
                .order("published_at", desc=True)
                .limit(200)
                .execute()
            )
            for row in (response.data or []):
                if row.get("id"):
                    items_by_id[row["id"]] = row
            
            # Query 2: Items without published_at, using fetched_at >= cutoff
            if len(items_by_id) < 200:
                fallback = (
                    self.datastore.client.table("feed_items")
                    .select("*")
                    .in_("id", item_ids)
                    .is_("published_at", "null")
                    .gte("fetched_at", cutoff.isoformat())
                    .order("fetched_at", desc=True)
                    .limit(200 - len(items_by_id))
                    .execute()
                )
                for row in (fallback.data or []):
                    if row.get("id") and row["id"] not in items_by_id:
                        items_by_id[row["id"]] = row
            
            items = list(items_by_id.values())
            total_items = len(items)
            
            print(f"  ✓ Found {total_items} feed items from database")
            
            # Convert feed items to event format
            # NO early filtering - all items are converted and scored downstream
            for item in items:
                title = item.get('title', '')
                summary = item.get('summary', '')
                source_key = item.get('source_key', '')
                
                # Determine category from source or content
                category = "news"
                if "nws" in source_key.lower():
                    category = "weather"
                
                content = ""
                categories = []
                raw = item.get('raw', {})
                if raw and isinstance(raw, dict):
                    content = raw.get("content_html") or raw.get("content") or ""
                    categories = raw.get("categories") or raw.get("tags") or []
                
                # Compute or extract travel relevance (used for scoring, not filtering)
                if category == "news":
                    # Check if travel relevance data already exists in raw (from GDELT sync)
                    if raw and isinstance(raw, dict) and "travel_relevance_score" in raw:
                        # Use pre-computed relevance data
                        travel_relevance_score = raw.get("travel_relevance_score")
                        travel_keywords_matched = raw.get("travel_keywords_matched", [])
                        travel_relevance_reason = raw.get("travel_relevance_reason", "")
                        
                        # Extract keyword counts
                        if isinstance(travel_keywords_matched, list):
                            keyword_matches = len(travel_keywords_matched)
                            # Count strong keywords
                            from news_relevance import STRONG_TRAVEL_KEYWORDS
                            strong_matches = sum(1 for kw in travel_keywords_matched if kw.lower() in [s.lower() for s in STRONG_TRAVEL_KEYWORDS])
                        else:
                            keyword_matches = 0
                            strong_matches = 0
                        
                        is_critical = travel_relevance_score >= 0.7 if travel_relevance_score else False
                    else:
                        # Compute relevance on-the-fly
                        relevance = compute_travel_relevance(
                            title=title,
                            summary=summary,
                            content=content,
                            categories=categories,
                        )
                        keyword_matches = len(relevance["regular_matches"])
                        strong_matches = len(relevance["strong_matches"])
                        travel_relevance_score = relevance["score"]
                        travel_keywords_matched = relevance["matched_keywords"]
                        travel_relevance_reason = relevance["reason"]
                        is_critical = relevance["is_critical"]
                    
                    # Calculate magnitude for downstream scoring
                    magnitude = _news_magnitude(
                        keyword_matches,
                        strong_matches,
                        is_critical,
                    )
                else:
                    # Weather alerts and other categories pass through
                    magnitude = raw.get("magnitude", 2.0) if raw else 2.0
                    keyword_matches = 0
                    strong_matches = 0
                    travel_relevance_score = None
                    travel_keywords_matched = None
                    travel_relevance_reason = None
                    is_critical = False
                
                # Generate event ID
                published_at = item.get('published_at')
                fetched_at = item.get('fetched_at')
                
                def _parse_ts(value):
                    if not value:
                        return None
                    try:
                        if isinstance(value, str):
                            if value.endswith("Z"):
                                value = value[:-1] + "+00:00"
                            return datetime.fromisoformat(value)
                        return value
                    except:
                        return None
                
                event_time = _parse_ts(published_at) or _parse_ts(fetched_at)
                time_str = event_time.isoformat() if event_time else ""
                
                eid = f"{source_key}:{city_key}:{_norm(title)}:{time_str or item.get('url', '')}"
                
                all_events.append({
                    "id": eid,
                    "title": title,
                    "summary": summary,
                    "category": category,
                    "magnitude": magnitude,
                    "confidence": {"score": 0.85 if category == "news" else 0.75},
                    "location": {"query": location},
                    "timestamp": time_str,
                    "source": source_key,
                    "url": item.get('url', ''),
                    "provider": source_key,
                    "keyword_matches": keyword_matches,
                    "strong_matches": strong_matches,
                    "travel_relevance_score": travel_relevance_score,
                    "travel_keywords_matched": travel_keywords_matched,
                    "travel_relevance_reason": travel_relevance_reason,
                    "is_critical": is_critical,
                    "content": content,
                    "categories": categories,
                    "scoped_city_key": city_key,
                    "scoped_via": "feed_item_cities",
                })
        
        except Exception as e:
            print(f"  ⚠ Warning: Could not fetch feed items from database ({type(e).__name__}: {e})")
            total_items = 0
        
        # SECONDARY: Also collect NWS weather alerts directly (they may not be in feed_items)
        extra_events = 0
        try:
            weather_events = self.weather_client.alerts_for_location(location, city_key, lookback_hours=48)
            if weather_events:
                extra_events = len(weather_events)
                all_events.extend(weather_events)
        except Exception as e:
            print(f"  ⚠ Warning: NWS alerts unavailable ({type(e).__name__})")
        
        # Sort by timestamp
        if all_events:
            all_events.sort(
                key=lambda event: _parse_event_timestamp(event.get("timestamp"))
                or datetime.min.replace(tzinfo=timezone.utc),
                reverse=True,
            )
        
        print(f"Total events collected: {len(all_events)} (no early filtering applied)")
        
        collection_stats = {
            "scanned_events": total_items + extra_events,
            "dropped_keyword_mismatch": 0,  # No early dropping based on keywords
        }
        
        if return_stats:
            return all_events, collection_stats
        return all_events
    
    def build_report_data(
        self,
        location: str,
        events: Sequence[Dict],
        city_ctx: Dict,
        trip_profile: Optional[Dict] = None,
        news_limit: int = DEFAULT_EVENT_LIMIT,
        news_min_score: float = DEFAULT_NEWS_MIN_SCORE,
        news_relax_keywords: bool = False,
        news_ignore_location_filter: bool = False,
        news_min_keyword_matches: int = DEFAULT_NEWS_MIN_KEYWORDS,
        news_min_strong_matches: int = DEFAULT_NEWS_MIN_STRONG,
        news_include_global_critical: bool = False,
        collection_stats: Optional[Dict[str, int]] = None,
        return_news_briefs: bool = False,
        city_config: Optional[Dict] = None,
        lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
    ) -> Dict | tuple[Dict, List[Dict]]:
        """Generate structured report data as JSON (not free text)."""
        # Determine city_key for DB queries
        city_key = self._location_to_city_key(location)
        scoped_keys = {
            event.get("scoped_city_key")
            for event in events
            if event.get("scoped_city_key")
        }
        if scoped_keys and city_key not in scoped_keys and len(scoped_keys) == 1:
            inferred_key = next(iter(scoped_keys))
            print(
                "News filter: inferred city_key from scoped events "
                f"({city_key} -> {inferred_key})"
            )
            city_key = inferred_key
        
        scan_stats = collection_stats or {}
        sorted_event_count = len(events)
        scanned_count = scan_stats.get("scanned_events", sorted_event_count)
        parsed_dates = [
            dt for dt in (_parse_event_timestamp(e.get("timestamp")) for e in events) if dt
        ]
        if parsed_dates:
            earliest = min(parsed_dates).date().isoformat()
            latest = max(parsed_dates).date().isoformat()
            print(
                f"News scan: sorted_events={sorted_event_count}, "
                f"published_range={earliest} to {latest}"
            )
        else:
            print(f"News scan: sorted_events={sorted_event_count}, published_range=unknown")

        eligible_for_location_count = len(events)
        scoped = [event for event in events if event.get("scoped_city_key") == city_key]
        unscoped = [event for event in events if event.get("scoped_city_key") != city_key]
        if news_ignore_location_filter:
            local_events = list(events)
            filtered_unscoped = list(unscoped)
            dropped_location_mismatch = 0
            print("News filter: location filter disabled")
        else:
            filtered_unscoped = filter_events_by_location(
                unscoped,
                location,
                include_global_critical=news_include_global_critical,
                min_strong_matches=news_min_strong_matches,
                city_config=city_config,
            )
            merged = scoped + filtered_unscoped
            seen = set()
            local_events = []
            for event in merged:
                eid = event.get("id")
                if eid and eid in seen:
                    continue
                if eid:
                    seen.add(eid)
                local_events.append(event)
            dropped_location_mismatch = max(len(unscoped) - len(filtered_unscoped), 0)
            if not local_events and events:
                # Do not fall back to all events when location filtering removes everything.
                # This prevents unrelated global items from appearing in a city's 'What's happening now'.
                print("News filter: location filter removed all events; no fallback to global items")

        print(
            "News filter scoped counts: "
            f"events_total={len(events)}, "
            f"events_scoped={len(scoped)}, "
            f"events_unscoped={len(unscoped)}, "
            f"events_unscoped_kept_after_location_filter={len(filtered_unscoped)}, "
            f"events_local_final={len(local_events)}"
        )
        selected, selection_stats = select_top_travel_events(
            local_events,
            location,
            max_events=news_limit,
            min_score=news_min_score,
            relax_keywords=news_relax_keywords,
            min_keyword_matches=news_min_keyword_matches,
            min_strong_matches=news_min_strong_matches,
            city_config=city_config,
        )

        dropped_keyword_mismatch = scan_stats.get(
            "dropped_keyword_mismatch", selection_stats.get("dropped_keyword_mismatch", 0)
        )
        dropped_total = (
            dropped_keyword_mismatch
            + dropped_location_mismatch
            + selection_stats.get("dropped_below_score", 0)
            + selection_stats.get("dropped_over_limit", 0)
        )
        print(
            "News filter counts: "
            f"scanned feed items={scanned_count}, kept={selection_stats.get('kept', 0)}, "
            f"dropped={dropped_total} (dropped_keyword_mismatch={dropped_keyword_mismatch}, "
            f"dropped_location_mismatch={dropped_location_mismatch}, "
            f"below_score={selection_stats.get('dropped_below_score', 0)}, "
            f"over_limit={selection_stats.get('dropped_over_limit', 0)}, "
            f"recovered_by_fallback={selection_stats.get('recovered_by_fallback', 0)})"
        )
        if news_relax_keywords:
            print(
                "News filter relax: "
                f"kept_by_keywords={selection_stats.get('kept_relaxed_keywords', 0)}, "
                f"kept_by_relaxed_score={selection_stats.get('kept_relaxed_score', 0)}"
            )

        selected = sorted(
            selected,
            key=lambda event: _parse_event_timestamp(event.get("timestamp"))
            or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        news_briefs = format_events_for_prompt(selected, limit=news_limit)
        
        # Default trip profile if not provided
        if trip_profile is None:
            trip_profile = {
                "will_drive": False,
                "needs_idp": False,
                "rental_provider": None,
                "notes": None
            }
        
        # Get appropriate travel advisory based on destination country
        city_config = get_city_config(city_key)
        country_code = city_config.get("country_code", "US")
        country_name = location.split(",")[-1].strip() if "," in location else None
        
        travel_advisory_data = {}
        try:
            travel_advisory_data = self.travel_advisory.get_advisory(country_code=country_code, country_name=country_name)
        except Exception as e:
            print(f"Warning: Failed to get travel advisory: {e}")
        
        # Get weather forecast
        weather_forecast = {}
        try:
            weather_forecast = self.forecast_client.get_forecast(location, city_key)
        except Exception as e:
            print(f"Warning: Failed to get weather forecast: {e}")

        system_prompt = (
            "You are a travel risk analyst. Produce ONLY valid JSON for a traveler intelligence report.\n\n"

            "Goal: actionable, decision grade traveler safety and planning guidance based strictly on provided inputs.\n"
            "Scope: crimes, severe weather, terrorism, public safety incidents, major transit disruptions, health emergencies, civil unrest.\n\n"

            "Hard constraints:\n"
            "1) Use ONLY the provided news events and provided weather forecast. Do not add facts not present in inputs.\n"
            "2) Do NOT output or invent any database owned fields: hospitals, airports, transit, pharmacies, supermarkets, convenience_stores, rental_cars, emergency_contacts.\n"
            "3) Output MUST contain exactly the allowed keys defined in the Output Schema below. No extra keys.\n"
            "4) Max items: top_actions max 5, whats_happening max 3, incidents max 5, cues max 5, baseline_tips 3 to 5, contextual_tips 0 to 4.\n"
            "5) Evidence rule: Every incident and every contextual_tip MUST reference at least one item from sources. If there is no evidence, omit the incident or contextual tip.\n"
            "6) If there are no destination relevant incidents, return empty arrays for whats_happening, incidents, and contextual_tips.\n\n"

            "Risk badge rules:\n"
            "You MUST output all 4 risk_badges in this order: Crime, Terrorism, Traffic, Health.\n"
            "Levels are low, med, high.\n"
            "If the provided inputs contain no evidence for a category, set level to low and note to 'No relevant items in provided feed'. Do not guess.\n\n"

            "Weather formatting rules:\n"
            "All temperatures MUST be formatted exactly like 68°F (20°C) or 20°C (68°F).\n"
            "All wind speeds MUST be formatted exactly like 10 mph (16 km/h).\n"
            "If forecast data is missing, weather_summary fields must be short and say forecast unavailable.\n\n"

            "Driving pack rules:\n"
            "You will receive trip_profile in the user prompt.\n"
            "Include driving_pack ONLY if trip_profile.will_drive is true. If will_drive is false or missing, OMIT the driving_pack key entirely.\n"
            "Do not claim country specific legal requirements unless explicitly present in inputs.\n\n"

            "Output Schema (return JSON only, no commentary, no markdown):\n"
            "{\n"
            '  "top_actions": ["..."],\n'
            '  "whats_happening": ["..."],\n'
            '  "risk_badges": [\n'
            '    {"name":"Crime","level":"low|med|high","note":"..."},\n'
            '    {"name":"Terrorism","level":"low|med|high","note":"..."},\n'
            '    {"name":"Traffic","level":"low|med|high","note":"..."},\n'
            '    {"name":"Health","level":"low|med|high","note":"..."}\n'
            "  ],\n"
            '  "cues": [{"cue":"...","action":"...","avoid":"..."}],\n'
            '  "incidents": [{"date":"YYYY-MM-DD","type":"Crime|Weather|Transit|Health|CivilUnrest","impact":"...","where":"...","recency":"...","source":"..."}],\n'
            '  "weather_summary": {"current":"...","outlook":"...","traveler_impact":"..."},\n'
            '  "opsec": {\n'
            '    "baseline_tips": [{"category":"...","tip":"..."}],\n'
            '    "contextual_tips": [{"category":"...","tip":"...","why_now":"...","evidence":["..."],"confidence":0.0}]\n'
            "  },\n"
            '  "trip_profile": {"will_drive":false,"needs_idp":false,"rental_provider":null,"notes":null},\n'
            '  "data_coverage": "...",\n'
            '  "sources": ["Publisher — Title (YYYY-MM-DD)", "..."]\n'
            "}\n"
        )

        
        # Format travel advisory data (source depends on destination)
        advisory_text = "None available"
        if travel_advisory_data:
            source = travel_advisory_data.get("source", "Travel Advisory")
            
            # Canada uses different level names than US State Department
            if "Canada" in source:
                advisory_level_map = {
                    0: "Exercise normal security precautions",
                    1: "Exercise a high degree of caution",
                    2: "Avoid non-essential travel",
                    3: "Avoid all travel"
                }
            else:  # US State Department
                advisory_level_map = {
                    0: "Exercise normal precautions",
                    1: "Exercise normal precautions",
                    2: "Exercise increased caution",
                    3: "Reconsider travel",
                    4: "Do not travel"
                }
            
            level = travel_advisory_data.get("advisory_level", 0)
            advisory_text = (
                f"{source}: {advisory_level_map.get(level, 'Unknown')}. "
                f"{travel_advisory_data.get('summary', '')}"
            )
        
        # Format weather forecast data
        weather_text = "No weather forecast available"
        if weather_forecast and weather_forecast.get("current"):
            current = weather_forecast["current"]
            daily = weather_forecast.get("daily", [])
            
            current_temp = current.get("temperature")
            feels_like = current.get("apparent_temperature")
            condition = current.get("condition", "Unknown")
            wind = current.get("wind_speed")
            
            # Convert to Celsius (handle None values)
            current_temp_c = (current_temp - 32) * 5/9 if current_temp is not None else None
            feels_like_c = (feels_like - 32) * 5/9 if feels_like is not None else None
            
            if current_temp is not None and feels_like is not None:
                weather_text = f"Current: {current_temp}°F ({current_temp_c:.1f}°C) (feels like {feels_like}°F/{feels_like_c:.1f}°C), {condition}"
            elif current_temp is not None:
                weather_text = f"Current: {current_temp}°F ({current_temp_c:.1f}°C), {condition}"
            else:
                weather_text = f"Current: {condition}"
                
            if wind:
                wind_kph = wind * 1.60934
                weather_text += f", Wind: {wind} mph ({wind_kph:.1f} km/h)"
            
            if daily:
                weather_text += "\n\n7-Day Forecast:\n"
                for day in daily[:7]:
                    date = day.get("date", "Unknown")
                    max_temp = day.get("max_temp")
                    min_temp = day.get("min_temp")
                    day_condition = day.get("condition", "Unknown")
                    precip_prob = day.get("precipitation_probability_max")
                    
                    # Convert to Celsius (handle None values)
                    max_temp_c = (max_temp - 32) * 5/9 if max_temp is not None else None
                    min_temp_c = (min_temp - 32) * 5/9 if min_temp is not None else None
                    
                    if max_temp is not None and min_temp is not None:
                        weather_text += f"  {date}: {max_temp}°F ({max_temp_c:.1f}°C) / {min_temp}°F ({min_temp_c:.1f}°C) - {day_condition}"
                    else:
                        weather_text += f"  {date}: {day_condition}"
                        
                    if precip_prob and precip_prob > 30:
                        weather_text += f" ({precip_prob}% rain)"
                    weather_text += "\n"

        user_prompt = (
            f"Destination: {location}\\n\\n"
            f"Travel-Impact News:\\n{json.dumps(news_briefs, ensure_ascii=False)}\\n\\n"
            f"Travel Advisory: {advisory_text}\\n\\n"
            f"Weather Forecast:\\n{weather_text}\\n\\n"
            f"Trip Profile:\\n{json.dumps(trip_profile, ensure_ascii=False, indent=2)}\\n\\n"
            "Generate structured JSON report data. Output ONLY valid JSON, no other text."
        )

        # No tools needed - all data comes from database
        
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        model_name = getattr(config, "OPENAI_MODEL", "gpt-5-mini")
        print(f"\n=== Using OpenAI model: {model_name} ===")
        
        # Make API call without tools
        response = self.client.chat.completions.create(
            model=model_name,
            messages=messages,
            response_format={"type": "json_object"}  # Force JSON output
        )
        
        response_message = response.choices[0].message
        json_text = response_message.content.strip()
        
        # Parse and return JSON
        try:
            parsed_data = json.loads(json_text)
            
            # GUARDRAILS: Remove any DB-owned fields from LLM output
            DB_OWNED_FIELDS = [
                "hospitals",
                "airports",
                "transit",
                "pharmacies",
                "supermarkets",
                "convenience_stores",
                "rental_cars",
                "emergency_contacts",
            ]
            
            for field in DB_OWNED_FIELDS:
                if field in parsed_data:
                    print(f"  ⚠ WARNING: LLM returned DB-owned field '{field}', removing from output")
                    del parsed_data[field]

            _apply_incident_fallback(
                parsed_data,
                local_events=local_events,
                selected_events=selected,
            )
            
            # Load DB bundle for deterministic data
            from report_loader import ReportDataLoader
            loader = ReportDataLoader(datastore=self.datastore)
            db_bundle = loader.load_db_bundle(
                city_key=city_key,
                window_hours=lookback_hours,
            )
            
            # Merge DB-owned data into final report
            curated = db_bundle.get('curated_places', {})
            
            # Hospitals (combine hospital + urgent_care)
            hospitals = curated.get('hospital', []) + curated.get('urgent_care', [])
            parsed_data["hospitals"] = [
                {
                    "name": h.get("name", ""),
                    "address": h.get("address", ""),
                    "website": h.get("website", ""),
                    "notes": h.get("notes", ""),
                }
                for h in hospitals
            ]
            
            # Transit
            parsed_data["transit"] = db_bundle.get('transit', [])

            def _extract_iata(value: Optional[str]) -> str:
                text = (value or "").strip()
                if not text:
                    return ""
                match = re.search(r"\biata[:\s]+([a-z0-9]{3})\b", text, re.IGNORECASE)
                if match:
                    return match.group(1).upper()
                if re.fullmatch(r"[a-z0-9]{3}", text, re.IGNORECASE):
                    return text.upper()
                return ""

            # Airports
            parsed_data["airports"] = [
                {
                    "name": a.get("name", ""),
                    "iata": _extract_iata(a.get("notes")) or (a.get("iata") or "").strip().upper(),
                    "website": a.get("website", ""),
                }
                for a in curated.get('airport', [])
            ]
            
            # Pharmacies
            parsed_data["pharmacies"] = [
                {
                    "name": p.get("name", ""),
                    "address": p.get("address", ""),
                    "website": p.get("website", ""),
                }
                for p in curated.get('pharmacy', [])
            ]
            
            # Supermarkets
            parsed_data["supermarkets"] = [
                {
                    "name": s.get("name", ""),
                    "address": s.get("address", ""),
                    "website": s.get("website", ""),
                }
                for s in curated.get('supermarket', [])
            ]
            
            # Convenience stores
            convenience = curated.get('convenience', []) or curated.get('convenience_store', [])
            parsed_data["convenience_stores"] = [
                {
                    "name": c.get("name", ""),
                    "address": c.get("address", ""),
                    "website": c.get("website", ""),
                }
                for c in convenience
            ]
            
            # Rental cars
            parsed_data["rental_cars"] = [
                {
                    "name": r.get("name", ""),
                    "address": r.get("address", ""),
                    "website": r.get("website", ""),
                }
                for r in curated.get('rental_car', [])
            ]
            
            # Emergency contacts
            parsed_data["emergency_contacts"] = db_bundle.get('emergency_contacts', {})

            # Wikimedia enrichment (optional, gated by env + credentials)
            try:
                from wikimedia_enrichment import enrich_places_with_wikimedia, is_wikimedia_enrichment_enabled

                if is_wikimedia_enrichment_enabled(city_key):
                    place_sections = {
                        "hospitals": parsed_data.get("hospitals", []),
                        "airports": parsed_data.get("airports", []),
                        "pharmacies": parsed_data.get("pharmacies", []),
                        "supermarkets": parsed_data.get("supermarkets", []),
                        "convenience_stores": parsed_data.get("convenience_stores", []),
                        "rental_cars": parsed_data.get("rental_cars", []),
                    }

                    for key, items in place_sections.items():
                        place_sections[key] = enrich_places_with_wikimedia(city_key, items)
                        parsed_data[key] = place_sections[key]

                    summary_parts = []
                    for key in (
                        "hospitals",
                        "airports",
                        "pharmacies",
                        "supermarkets",
                        "convenience_stores",
                        "rental_cars",
                    ):
                        items = parsed_data.get(key, [])
                        total = len(items)
                        with_website = sum(1 for p in items if p.get("website"))
                        conflicts = sum(1 for p in items if p.get("wikimedia_conflict"))
                        candidates = sum(1 for p in items if p.get("wikimedia_candidate_website"))
                        summary_parts.append(
                            f"{key}={total} (website={with_website}, conflicts={conflicts}, candidates={candidates})"
                        )

                    if summary_parts:
                        print("  INFO Wikimedia enrichment:", "; ".join(summary_parts))

                    # Log only: remove candidate fields before report payload is stored.
                    for items in place_sections.values():
                        for entry in items:
                            if not isinstance(entry, dict):
                                continue
                            entry.pop("wikimedia_candidate_website", None)
                            entry.pop("wikimedia_candidate_website_final", None)
                            entry.pop("wikimedia_confidence", None)
                            entry.pop("wikimedia_conflict", None)
            except Exception as e:
                print(f"  WARN Wikimedia enrichment failed: {e}")
            
            # Add coverage metadata
            coverage = db_bundle.get('coverage', {})
            if not coverage.get('transit'):
                parsed_data["data_coverage"] = (parsed_data.get("data_coverage", "") + " | Transit data unavailable").strip()
            if not coverage.get('hospitals'):
                parsed_data["data_coverage"] = (parsed_data.get("data_coverage", "") + " | Hospital data unavailable").strip()
            
            # Add trip profile
            if trip_profile:
                parsed_data["trip_profile"] = trip_profile
                if trip_profile.get("will_drive"):
                    driving_pack = parsed_data.get("driving_pack")
                    if not isinstance(driving_pack, dict):
                        driving_pack = {}
                    driving_pack.setdefault("checklist", [])
                    driving_pack.setdefault("rental_notes", [])
                    driving_pack.setdefault("accident_steps", [])
                    parsed_data["driving_pack"] = driving_pack
            
            print(f"  ✓ Final report: {len(parsed_data.get('hospitals', []))} hospitals, "
                  f"{len(parsed_data.get('transit', []))} transit providers, "
                  f"{len(parsed_data.get('pharmacies', []))} pharmacies from DB")
            
            if return_news_briefs:
                return parsed_data, news_briefs
            return parsed_data
        except json.JSONDecodeError as e:
            print(f"Warning: Failed to parse AI JSON response: {e}")
            # Return minimal fallback structure
            fallback = {
                "top_actions": ["Review local conditions", "Monitor news", "Keep emergency contacts handy"],
                "whats_happening": ["Data collection in progress"],
                "risk_badges": [{"name": "Overall", "level": "low"}],
                "cues": [{"cue": "Any concerns", "action": "Contact local authorities"}],
                "incidents": [],
                "hospitals": [],
                "airports": [],
                "transit": [],
                "pharmacies": [],
                "supermarkets": [],
                "convenience_stores": [],
                "rental_cars": [],
                "emergency_contacts": {},
                "data_coverage": "Partial data available",
                "sources": []
            }
            
            # Load DB bundle for fallback too
            try:
                from report_loader import ReportDataLoader
                loader = ReportDataLoader(datastore=self.datastore)
                db_bundle = loader.load_db_bundle(city_key=city_key, window_hours=lookback_hours)
                
                curated = db_bundle.get('curated_places', {})
                hospitals = curated.get('hospital', []) + curated.get('urgent_care', [])
                fallback["hospitals"] = [{"name": h.get("name", ""), "address": h.get("address", ""), "website": h.get("website", "")} for h in hospitals]
                fallback["airports"] = [
                    {
                        "name": a.get("name", ""),
                        "iata": "",
                        "website": a.get("website", ""),
                    }
                    for a in curated.get('airport', [])
                ]
                fallback["transit"] = db_bundle.get('transit', [])
                fallback["pharmacies"] = [{"name": p.get("name", ""), "address": p.get("address", ""), "website": p.get("website", "")} for p in curated.get('pharmacy', [])]
                fallback["supermarkets"] = [{"name": s.get("name", ""), "address": s.get("address", ""), "website": s.get("website", "")} for s in curated.get('supermarket', [])]
                convenience = curated.get('convenience', []) or curated.get('convenience_store', [])
                fallback["convenience_stores"] = [{"name": c.get("name", ""), "address": c.get("address", ""), "website": c.get("website", "")} for c in convenience]
                fallback["rental_cars"] = [{"name": r.get("name", ""), "address": r.get("address", ""), "website": r.get("website", "")} for r in curated.get('rental_car', [])]
                fallback["emergency_contacts"] = db_bundle.get('emergency_contacts', {})
            except Exception as fallback_err:
                print(f"  ⚠ Could not load DB bundle for fallback: {fallback_err}")
                # Use config-based emergency contacts
                city_config = config.CITIES.get(city_key, {})
                fallback["emergency_contacts"] = get_emergency_contacts(city_config.get("country_code"))

            if trip_profile:
                fallback["trip_profile"] = trip_profile
                if trip_profile.get("will_drive"):
                    fallback["driving_pack"] = {
                        "checklist": [],
                        "rental_notes": [],
                        "accident_steps": [],
                    }
            
            if return_news_briefs:
                return fallback, news_briefs
            return fallback

    def _build_final_context(
        self,
        report_data: Dict,
        location: str,
        city_key: str,
        lookback_hours: int,
        news_briefs: Optional[List[Dict]] = None,
    ) -> tuple[Dict, Optional[Dict]]:
        """
        Build final merged context from canonical DB data + AI overrides.
        
        Returns:
            (context_dict, db_trend_data) - merged context for PDF/DB and trend metadata
        """
        # DB-first: Emergency numbers and transit are owned by snapshot tables.
        missing_snapshot_reasons: list[str] = []

        emergency_contacts = report_data.get("emergency_contacts")
        if not emergency_contacts:
            try:
                ctx_obj = self.datastore.get_city_context(city_key)
                ctx = ctx_obj.context if ctx_obj and isinstance(ctx_obj.context, dict) else {}
                if isinstance(ctx, dict):
                    if isinstance(ctx.get("emergency_numbers"), dict):
                        emergency_contacts = ctx.get("emergency_numbers")
                    elif isinstance(ctx.get("emergency_contacts"), dict):
                        emergency_contacts = ctx.get("emergency_contacts")
            except Exception:
                emergency_contacts = None

        if not emergency_contacts:
            missing_snapshot_reasons.append("missing snapshot: city_context")
            city_config = getattr(config, "CITIES", {}).get(city_key, {})
            emergency_contacts = get_emergency_contacts(city_config.get("country_code"))
        emergency_contacts = normalize_emergency_contacts(emergency_contacts)

        # DB-first transit providers
        transit_list = None
        try:
            tr_obj = self.datastore.get_transit_snapshot(city_key)
            tr = tr_obj.transit if tr_obj and isinstance(tr_obj.transit, dict) else {}
            providers = tr.get("providers") if isinstance(tr, dict) else None
            if isinstance(providers, list):
                transit_list = []
                for p in providers:
                    if not isinstance(p, dict) or not p.get("name"):
                        continue
                    transit_list.append(
                        {
                            "name": p.get("name"),
                            "modes": p.get("modes", []) or [],
                            "website": p.get("website", ""),
                            "description": p.get("description") or "",
                            "taxis": p.get("taxis"),
                            "rideshare": p.get("rideshare"),
                        }
                    )
        except Exception:
            transit_list = None

        if transit_list is None:
            missing_snapshot_reasons.append("missing snapshot: transit")
            city_config = getattr(config, "CITIES", {}).get(city_key, {})
            transit_list = []
            for system in (city_config.get("transit_systems") or []):
                if not isinstance(system, dict) or not system.get("name"):
                    continue
                transit_list.append(
                    {
                        "name": system.get("name"),
                        "modes": system.get("modes", []) or [],
                        "website": system.get("website", ""),
                        "description": "",
                    }
                )

        report_data["transit"] = transit_list

        if missing_snapshot_reasons:
            base = (report_data.get("data_coverage") or "").strip()
            note = "Partial: " + ", ".join(missing_snapshot_reasons)
            report_data["data_coverage"] = (base + (" " if base else "") + note).strip()
        
        # Deduplicate establishments by name (case-insensitive), prefer actionable entries.
        def deduplicate_list(items: List[Dict], key: str = "name") -> List[Dict]:
            """Remove duplicates, preferring entries with usable websites while preserving order."""
            seen: Dict[str, tuple[int, Dict]] = {}
            unique_items: List[Dict] = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                identifier = (item.get(key, "") or "").lower().strip()
                if not identifier:
                    continue

                if "website" in item:
                    item["website"] = _normalize_website(item.get("website"))

                if identifier not in seen:
                    unique_items.append(item)
                    seen[identifier] = (len(unique_items) - 1, item)
                    continue

                idx, existing = seen[identifier]
                existing_website = existing.get("website") or ""
                current_website = item.get("website") or ""

                choose_current = False
                if current_website and not existing_website:
                    choose_current = True
                elif current_website and existing_website:
                    if _is_valid_http_url(current_website) and not _is_valid_http_url(existing_website):
                        choose_current = True

                if choose_current:
                    unique_items[idx] = item
                    seen[identifier] = (idx, item)

            return unique_items

        city_config = get_city_config(city_key)
        country_code = (city_config.get("country_code") or "US").strip().upper()
        country_name_en = get_country_name_en(country_code)
        
        # Deduplicate and prioritize POIs with usable websites.
        if "pharmacies" in report_data:
            report_data["pharmacies"] = _normalize_place_list(deduplicate_list(report_data["pharmacies"]))
        if "supermarkets" in report_data:
            report_data["supermarkets"] = _normalize_place_list(deduplicate_list(report_data["supermarkets"]))
        if "convenience_stores" in report_data:
            report_data["convenience_stores"] = _normalize_place_list(deduplicate_list(report_data["convenience_stores"]))
        if "hospitals" in report_data:
            report_data["hospitals"] = _normalize_place_list(deduplicate_list(report_data["hospitals"]))
        if "transit" in report_data:
            report_data["transit"] = _normalize_place_list(deduplicate_list(report_data["transit"]))
        if "airports" in report_data:
            report_data["airports"] = _normalize_place_list(deduplicate_list(report_data["airports"]))
        report_data["rental_cars"] = dedupe_rental_car_entries(
            report_data.get("rental_cars") or [],
            country_code,
            country_name_en,
        )
        report_data["rental_cars"] = _normalize_place_list(report_data.get("rental_cars") or [])
        if not report_data["rental_cars"]:
            canonical_links = resolve_rental_car_links_unvalidated(country_code, country_name_en)
            report_data["rental_cars"] = _normalize_place_list(
                [
                    {"name": item.get("label", ""), "address": "", "website": item.get("url", "")}
                    for item in canonical_links
                    if item.get("label") and item.get("url")
                ]
            )

        trip_profile = report_data.get("trip_profile")
        if isinstance(trip_profile, dict) and trip_profile.get("will_drive"):
            driving_pack = report_data.get("driving_pack")
            if not isinstance(driving_pack, dict):
                driving_pack = {}
            driving_pack.setdefault("checklist", [])
            driving_pack.setdefault("rental_notes", [])
            driving_pack.setdefault("accident_steps", [])
            report_data["driving_pack"] = driving_pack
        
        # Remove emergency_contacts from report_data since we pass it explicitly
        report_data.pop("emergency_contacts", None)
        
        # Build complete template context
        now_utc = datetime.now(timezone.utc)
        generated_utc = now_utc.strftime('%Y-%m-%d %H:%M')
        report_id = f"{location.lower().replace(' ', '-')}-{now_utc.strftime('%Y%m%d-%H%M')}"
        
        # Compute at-a-glance metrics
        risk_badges = report_data.get("risk_badges", [])
        allowed_risk_levels = {"low", "med", "high"}
        for badge in risk_badges:
            if not isinstance(badge, dict):
                continue
            level = str(badge.get("level") or "").strip().lower()
            if level in allowed_risk_levels:
                badge["level"] = level
            else:
                badge["level"] = "high"
        
        # Overall Risk: Derive STRICTLY from official travel advisory level
        # Do NOT use risk_badges for overall_risk calculation
        overall_risk = "Unknown"  # Default if advisory unavailable
        
        # Get city metadata to determine country
        city_config = city_config or get_city_config(city_key)
        country_code = (city_config.get("country_code") or "US").strip().upper()
        is_us_destination = (country_code == "US")
        
        # Fetch travel advisory for this destination's country
        advisory_data = {}
        try:
            # Extract country_name from city_config name, not raw input location
            config_name = city_config.get("name", "")
            country_name = config_name.split(",")[-1].strip() if "," in config_name else None
            # Fallback to parsing input location if config name is missing
            if not country_name:
                country_name = location.split(",")[-1].strip() if "," in location else None
            advisory_data = self.travel_advisory.get_advisory(country_code=country_code, country_name=country_name)
        except Exception as e:
            print(f"  ⚠ Warning: Could not fetch travel advisory: {e}")
        
        # Map advisory level or advisory phrase to Overall Risk
        if advisory_data:
            normalized_risk = _derive_overall_risk_from_advisory(advisory_data, is_us_destination)
            if normalized_risk:
                overall_risk = normalized_risk

        overall_risk = str(overall_risk or "").strip().lower()
        if overall_risk not in allowed_risk_levels:
            overall_risk = "high"
        
        # Top risk driver: badge with highest level (this remains for context)
        top_risk_driver = "None"
        for priority in ["high", "med", "low"]:
            high_risks = [b.get("name", "Unknown") for b in risk_badges if b.get("level") == priority]
            if high_risks:
                top_risk_driver = high_risks[0]
                break

        exec_summary = report_data.get("executive_summary")
        if not isinstance(exec_summary, dict):
            exec_summary = {}
            report_data["executive_summary"] = exec_summary
        exec_summary["overall_risk"] = overall_risk
        exec_summary["top_risk_driver"] = top_risk_driver
        if not str(exec_summary.get("summary") or "").strip():
            advisory_label = (report_data.get("advisory_level") or "").strip()
            if advisory_label:
                exec_summary["summary"] = f"Official travel advisory: {advisory_label}."
        
        # Coverage status
        incidents_count = len(report_data.get("incidents", []))
        coverage_status = "All feeds OK" if incidents_count > 0 else "Limited data"
        
        context_payload = dict(report_data)
        context_payload.pop("executive_summary", None)
        context = ReportContext(
            destination=location,
            generated_utc=generated_utc,
            lookback_label="Past 48 hours",
            coverage_label="Local news + Verified infrastructure data",
            emergency_contacts=emergency_contacts,
            report_id=report_id,
            overall_risk=overall_risk,
            top_risk_driver=top_risk_driver,
            coverage_status=coverage_status,
            **context_payload  # Spread AI-generated data
        )
        
        # Generate 30-day trend analysis for all categories in one chart
        combined_trend_chart_path = None
        trend_note = None
        
        city_config = getattr(config, "CITIES", {}).get(city_key, {})
        population = city_config.get("population", 500000)  # Fallback to 500k if not configured
        
        if population > 0:
            # Query trend data (30 days from datastore)
            trend_config = getattr(config, "TREND_CONFIG", {})
            lookback_days = trend_config.get("lookback_days", 30)
            
            try:
                # Get raw trend buckets from datastore
                datastore = get_datastore()
                trend_buckets = datastore.get_incident_trends(
                    city_key=city_key,
                    lookback_days=lookback_days,
                    categories=None,
                )

                total_incidents = 0
                try:
                    total_incidents = sum((b.get('count', 0) or 0) for b in (trend_buckets or {}).get('total', []))
                except Exception:
                    total_incidents = 0

                days_in_window = len((trend_buckets or {}).get('total', []))
                category_keys = ['crime', 'terrorism', 'traffic', 'health', 'weather', 'other']
                category_totals = {}
                for key in category_keys:
                    try:
                        category_totals[key] = sum((b.get('count', 0) or 0) for b in (trend_buckets or {}).get(key, []))
                    except Exception:
                        category_totals[key] = 0

                print(f"  Trend scan: days={days_in_window}, total_incidents={total_incidents}")
                totals_line = ', '.join(f"{key}={category_totals.get(key, 0)}" for key in category_keys)
                print(f"  Trend totals: {totals_line}")

                # Generate combined trend chart for all 4 categories
                if trend_buckets:
                    combined_chart_file = Path('charts') / f"{report_id}-combined-trends.png"
                    create_combined_trends_chart(trend_buckets, combined_chart_file)
                    combined_trend_chart_path = str(combined_chart_file)

                if total_incidents == 0:
                    trend_note = (
                        f"Low incident volume: {total_incidents} incidents in the last {lookback_days} days."
                    )
            except Exception as e:
                print(f"Warning: Failed to generate trend analysis: {e}")
                import traceback
                traceback.print_exc()
                trend_note = "Trend analysis unavailable due to an internal error."
        
        # =============================================================================
        # CANONICAL-FIRST ARCHITECTURE
        # Build canonical report_data from database FIRST, then merge AI overrides
        # with allowlist to ensure curated facilities take precedence
        # =============================================================================
        print("\n=== Building canonical report data from database ===")
        from report_data_contract import (
            apply_overrides_with_allowlist,
            build_important_services,
            build_report_data,
            check_website_regression,
        )

        city_cfg = getattr(config, "CITIES", {}).get(city_key, {}) or {}
        
        canonical = build_report_data(
            city_key=city_key,
            window_hours=lookback_hours,
            datastore=self.datastore,
            generated_at=datetime.now(timezone.utc).isoformat(),
        )
        print(f"  ✓ Canonical data built:")
        print(f"    • Hospitals: {len(canonical.get('hospitals', []))}")
        print(f"    • Transit: {len(canonical.get('transit', []))}")
        print(f"    • Pharmacies: {len(canonical.get('pharmacies', []))}")
        print(f"    • Supermarkets: {len(canonical.get('supermarkets', []))}")
        print(f"    • Convenience stores: {len(canonical.get('convenience_stores', []))}")
        print(f"    • Rental cars: {len(canonical.get('rental_cars', []))}")

        # Prepare AI overrides from context
        ai_context_dict = context.model_dump()
        if isinstance(report_data.get("executive_summary"), dict):
            ai_context_dict["executive_summary"] = report_data["executive_summary"]
        for key in ("travel_advisory", "advisory_level", "advisory_source", "travel_advisory_url"):
            if report_data.get(key) is not None:
                ai_context_dict[key] = report_data[key]
        if news_briefs is not None:
            ai_context_dict["news_briefs"] = news_briefs
        ai_context_dict["combined_trend_chart_path"] = combined_trend_chart_path
        ai_context_dict["trend_note"] = trend_note
        ai_context_dict["city_key"] = city_key
        
        print(f"\n=== AI-generated context ===")
        print(f"  ℹ AI hospitals: {len(ai_context_dict.get('hospitals', []))}")
        print(f"  ℹ AI transit: {len(ai_context_dict.get('transit', []))}")
        weather_preview = ai_context_dict.get('weather_summary', 'None')
        if weather_preview and weather_preview != 'None':
            weather_preview = str(weather_preview)[:50] + ('...' if len(str(weather_preview)) > 50 else '')
        print(f"  ℹ AI weather: {weather_preview}")
        
        # Merge with canonical-first strategy (DB-backed lists use canonical, narrative uses AI)
        print(f"\n=== Merging canonical + AI with allowlist ===")
        context_dict = apply_overrides_with_allowlist(
            canonical=canonical,
            ai_overrides=ai_context_dict,
            verbose=True,
        )

        expected_important_services = build_important_services(city_cfg, [])
        if expected_important_services:
            merged_services = context_dict.get("important_services")
            merged_count = len(merged_services) if isinstance(merged_services, list) else 0
            expected_count = len(expected_important_services)
            if _important_services_debug_enabled():
                print(f"Important services configured, expected {expected_count}, merged {merged_count}")
            if merged_count == 0:
                raise RuntimeError(
                    f"Important services verification failed for {city_key}: "
                    f"configured {expected_count}, merged {merged_count}"
                )

        # Ensure advisory fields are not wiped by AI nulls
        if canonical.get("travel_advisory_url") and not context_dict.get("travel_advisory_url"):
            context_dict["travel_advisory_url"] = canonical["travel_advisory_url"]
        if canonical.get("advisory_level") and not context_dict.get("advisory_level"):
            context_dict["advisory_level"] = canonical["advisory_level"]
        if canonical.get("advisory_source") and not context_dict.get("advisory_source"):
            context_dict["advisory_source"] = canonical["advisory_source"]
        if not context_dict.get("medical_facilities"):
            context_dict["medical_facilities"] = list(context_dict.get("hospitals") or [])
        
        # Check for website regressions (guardrail)
        print(f"\n=== Checking for website regressions ===")
        website_warnings = check_website_regression(canonical, context_dict, verbose=True)
        if not website_warnings:
            print(f"  ✓ No website regressions detected")
        
        print(f"\n=== Final merged context ===")
        print(f"  ℹ Hospitals: {len(context_dict.get('hospitals', []))}")
        print(f"  ℹ Transit: {len(context_dict.get('transit', []))}")
        print(f"  ℹ Pharmacies: {len(context_dict.get('pharmacies', []))}")
        final_weather = context_dict.get('weather_summary', 'None')
        if final_weather and final_weather != 'None':
            final_weather = str(final_weather)[:50] + ('...' if len(str(final_weather)) > 50 else '')
        print(f"  ℹ Weather: {final_weather}")
        
        # =============================================================================
        # Prepare trend_data and meta before finalizing context_dict
        # =============================================================================
        # Prepare trend data for database storage
        db_trend_data = None
        if trend_buckets:
            db_trend_data = {
                "lookback_days": lookback_days,
                "categories": trend_buckets,
                "chart_generated": combined_trend_chart_path is not None,
            }
        
        # Add trend_data to context if not already present
        if "trend_data" not in context_dict:
            context_dict["trend_data"] = db_trend_data or {}
        
        # Ensure meta section is populated
        if "meta" not in context_dict or not isinstance(context_dict.get("meta"), dict):
            context_dict["meta"] = {}
        
        meta = context_dict["meta"]
        meta.update({
            "city_key": city_key,
            "window_hours": lookback_hours,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "coverage_status": context_dict.get("coverage_status", ""),
        })
        if not context_dict.get("country_code"):
            try:
                city_cfg = getattr(config, "CITIES", {}).get(city_key, {}) or {}
                context_dict["country_code"] = (city_cfg.get("country_code") or "").strip().upper()
            except Exception:
                context_dict["country_code"] = context_dict.get("country_code", "")
        if context_dict.get("country_code"):
            meta["country_code"] = context_dict.get("country_code")
        
        return context_dict, db_trend_data

    def render_pdf(
        self,
        report_data: Dict,
        location: str,
        output_path: Path,
        travel_advisory_data: Dict = None,
        events: List[Dict] = None,
        lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
        city_key: Optional[str] = None,
        news_briefs: Optional[List[Dict]] = None,
        skip_pdf: bool = False,
    ) -> Path:
        """Render professional PDF from structured data using HTML/CSS template."""
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Resolve destination metadata early for advisory labeling.
        city_key = (city_key or "").strip() or self._location_to_city_key(location)
        city_config = get_city_config(city_key)
        from report_data_contract import build_important_services

        expected_important_services = build_important_services(city_config, [])
        country_code = (city_config.get("country_code") or "US").strip().upper()
        
        # Format travel advisory (source depends on destination)
        travel_advisory = None
        advisory_level = None
        advisory_source = None
        if travel_advisory_data:
            # Choose label mapping strictly based on destination country code:
            # - US destinations use Government of Canada Travel Advisories
            # - All other destinations use U.S. Department of State Travel Advisories
            if country_code == "US":
                advisory_level_map = {
                    0: "Exercise normal security precautions",
                    1: "Exercise a high degree of caution",
                    2: "Avoid non-essential travel",
                    3: "Avoid all travel"
                }
                advisory_source = "Government of Canada Travel Advisories"
            else:
                advisory_level_map = {
                    0: "Exercise normal precautions",
                    1: "Exercise normal precautions",
                    2: "Exercise increased caution",
                    3: "Reconsider travel",
                    4: "Do not travel"
                }
                advisory_source = "U.S. Department of State Travel Advisories"

            level = travel_advisory_data.get("advisory_level", 0)
            advisory_level = advisory_level_map.get(level, "Unknown")
            # Strip HTML tags and format as clean text
            raw_summary = travel_advisory_data.get('summary', '')
            travel_advisory = format_advisory_text(raw_summary)
        
        # Add advisory to report_data
        if travel_advisory:
            report_data["travel_advisory"] = travel_advisory
        if travel_advisory_data and country_code == "US":
            report_data["advisory_level"] = advisory_level
            report_data["advisory_source"] = advisory_source
            if travel_advisory_data.get("travel_advisory_url"):
                report_data["travel_advisory_url"] = travel_advisory_data.get("travel_advisory_url")
        elif travel_advisory:
            report_data["advisory_level"] = advisory_level
            report_data["advisory_source"] = advisory_source
            if travel_advisory_data and travel_advisory_data.get("travel_advisory_url"):
                report_data["travel_advisory_url"] = travel_advisory_data.get("travel_advisory_url")
        
        # Build final merged context (canonical + AI)
        try:
            context_dict, db_trend_data = self._build_final_context(
                report_data=report_data,
                location=location,
                city_key=city_key,
                lookback_hours=lookback_hours,
                news_briefs=news_briefs,
            )
            _normalize_places_context(context_dict)
            
            # =============================================================================
            # Enrich context with Google Maps search URLs BEFORE hashing
            # =============================================================================
            from helpers.enrich_google_maps_urls import enrich_report_with_google_maps_urls, _looks_like_place_list
            
            # Get city config for city label building
            city_config = get_city_config(city_key)
            
            # Enrich context_dict with Google Maps URLs (do not override existing values)
            # This MUST happen before hashing to ensure PDF and DB have identical data
            context_dict = enrich_report_with_google_maps_urls(
                context_dict,
                meta=context_dict.get("meta"),
                city_config=city_config,
            )

            from helpers.link_health import sanitize_government_service_links

            # Sanitize Government service links before hash/PDF/DB so all outputs remain identical.
            gov_link_stats = sanitize_government_service_links(
                context_dict,
                timeout=5,
                logger=print,
            )
            if gov_link_stats.get("rewritten") or gov_link_stats.get("removed"):
                print(
                    "Government link sanitizer: "
                    f"checked={gov_link_stats.get('checked', 0)} "
                    f"rewritten={gov_link_stats.get('rewritten', 0)} "
                    f"removed={gov_link_stats.get('removed', 0)}"
                )

            def _log_missing_google_maps_urls(report: Dict[str, Any]) -> None:
                missing: Dict[str, Dict[str, int]] = {}

                def walk(obj: Any) -> None:
                    if isinstance(obj, dict):
                        for k, v in obj.items():
                            if isinstance(v, list) and _looks_like_place_list(str(k), v):
                                total = len(v)
                                if total:
                                    missing_count = sum(
                                        1
                                        for item in v
                                        if isinstance(item, dict)
                                        and not str(item.get("google_maps_url") or "").strip()
                                    )
                                    missing[str(k)] = {"missing": missing_count, "total": total}
                            else:
                                walk(v)
                    elif isinstance(obj, list):
                        for item in obj:
                            walk(item)

                walk(report)
                if missing:
                    print("\n=== Google Maps URL Coverage (post-enrichment) ===")
                    for key in sorted(missing.keys()):
                        stats = missing[key]
                        print(f"  • {key}: {stats['missing']}/{stats['total']} missing")

            _log_missing_google_maps_urls(context_dict)
            
            # Save JSON for inspection (now includes google_maps_url fields)
            json_path = output_path.with_suffix('.json')
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(context_dict, f, ensure_ascii=False, indent=2)
            
            # =============================================================================
            # Hash and save the canonical payload BEFORE any rendering
            # =============================================================================
            import hashlib
            import copy
            
            # Create immutable snapshot for hashing (now includes google_maps_url)
            final_context_json_str = json.dumps(context_dict, ensure_ascii=False, sort_keys=True)
            final_context_hash = hashlib.sha256(final_context_json_str.encode('utf-8')).hexdigest()
            
            # Deep copy for PDF rendering (in case renderer mutates)
            pdf_context_dict = copy.deepcopy(context_dict)
            
            # === INCIDENT VERIFICATION ===
            incidents_list = context_dict.get("incidents", [])
            print(f"\n=== Incident Data Verification ===")
            print(f"  • Total incidents in context: {len(incidents_list)}")
            if incidents_list:
                for idx, inc in enumerate(incidents_list[:3], 1):
                    impact = inc.get("impact", "")
                    impact_preview = impact[:50] + "..." if len(impact) > 50 else impact
                    print(f"    [{idx}] {inc.get('date', 'No date')} - {inc.get('type', 'Unknown')}: {impact_preview}")
                if len(incidents_list) > 3:
                    print(f"    ... and {len(incidents_list) - 3} more")
            else:
                print(f"    (No incidents in this reporting period)")
            
            # Render HTML/CSS template to PDF (using copy of context_dict)
            if not skip_pdf:
                print(f"\n=== Rendering PDF ===")
                if _important_services_debug_enabled():
                    top_services = context_dict.get("important_services")
                    top_categories = context_dict.get("important_services_categories")
                    city_essentials = (
                        context_dict.get("city_essentials")
                        if isinstance(context_dict.get("city_essentials"), dict)
                        else {}
                    )
                    nested_services = city_essentials.get("important_services") if isinstance(city_essentials, dict) else []
                    nested_categories = (
                        city_essentials.get("important_services_categories")
                        if isinstance(city_essentials, dict)
                        else []
                    )
                    print("  - template: templates/report_new.html")
                    print(f"  - important_services (top): {len(top_services) if isinstance(top_services, list) else 0}")
                    print(
                        f"  - important_services_categories (top): "
                        f"{len(top_categories) if isinstance(top_categories, list) else 0}"
                    )
                    print(
                        f"  - city_essentials.important_services: "
                        f"{len(nested_services) if isinstance(nested_services, list) else 0}"
                    )
                    print(
                        f"  - city_essentials.important_services_categories: "
                        f"{len(nested_categories) if isinstance(nested_categories, list) else 0}"
                    )
                render_report_pdf(pdf_context_dict, output_path)
            else:
                print(f"\n=== Skipping PDF generation (--skip-pdf) ===")
                try:
                    html_output_path = output_path.with_suffix(".html")
                    html = render_html(
                        template_dir=Path("templates"),
                        template_name="report_new.html",
                        context=pdf_context_dict,
                    )
                    with open(html_output_path, "w", encoding="utf-8") as html_file:
                        html_file.write(html)
                    print(f"  ✓ Wrote HTML snapshot: {html_output_path}")
                except Exception as html_err:
                    print(f"  ⚠ Warning: Could not write HTML snapshot: {html_err}")
            
            # =============================================================================
            # Save report data to database (using SAME merged context_dict as PDF)
            # =============================================================================
            print(f"\n=== Saving to database ===")
            try:
                
                # Use the EXACT same merged context_dict for DB storage (already enriched with google_maps_url)
                model_name = getattr(config, "OPENAI_MODEL", "gpt-5-mini")
                
                # Hash should match the one we computed before PDF render
                stored_json_str = json.dumps(context_dict, ensure_ascii=False, sort_keys=True)
                stored_hash = hashlib.sha256(stored_json_str.encode('utf-8')).hexdigest()

                success = self.datastore.upsert_city_report(
                    city_key=city_key,
                    report_data=context_dict,  # Use original context_dict (not the PDF copy)
                    trend_data=db_trend_data,
                    model=model_name,
                    window_hours=lookback_hours,
                    pdf_file_url=(None if skip_pdf else str(output_path.absolute())),
                    updated_by="travel_agent.py",
                )
                
                if success:
                    print(f"  ✓ Report saved to database (city_reports table)")
                    print(f"    • Final context hash: {final_context_hash[:16]}...")
                    print(f"    • Stored hash: {stored_hash[:16]}...")
                    print(f"    • Incidents saved to DB: {len(context_dict.get('incidents', []))}")
                    
                    # VERIFICATION: Hashes should match since we use same dict
                    if stored_hash == final_context_hash:
                        print(f"    • Verification: PASSED (PDF and DB use identical data)")
                    else:
                        print(f"    ⚠ Verification: HASH MISMATCH (pre-render != stored)!")
                        print(f"      Pre-render hash: {final_context_hash}")
                        print(f"      Stored hash:     {stored_hash}")
                        raise RuntimeError("PDF/DB parity failure: pre-render hash != stored hash")
                    
                    # Read back and verify incidents count
                    db_record = self.datastore.get_city_report(city_key)
                    if db_record and db_record.get('report_data'):
                        db_json_str = json.dumps(db_record['report_data'], ensure_ascii=False, sort_keys=True)
                        db_hash = hashlib.sha256(db_json_str.encode('utf-8')).hexdigest()
                        db_incidents = db_record['report_data'].get('incidents', [])
                        
                        if db_hash == stored_hash:
                            print(f"    • DB read-back: PASSED (hashes match)")
                        else:
                            print(f"    ⚠ DB read-back: FAILED (hash mismatch!)")
                            print(f"      Expected: {stored_hash}")
                            print(f"      From DB:  {db_hash}")
                            raise RuntimeError("PDF/DB parity failure: DB read-back hash mismatch")
                        
                        # Verify incident count
                        if len(db_incidents) == len(incidents_list):
                            print(f"    • Incidents verification: PASSED ({len(db_incidents)} incidents match)")
                        else:
                            print(f"    ⚠ Incidents verification: COUNT MISMATCH!")
                            print(f"      Sent to DB: {len(incidents_list)}")
                            print(f"      Retrieved:  {len(db_incidents)}")
                            raise RuntimeError("PDF/DB parity failure: incident count mismatch")

                        if expected_important_services:
                            db_services = db_record["report_data"].get("important_services", [])
                            db_services_count = len(db_services) if isinstance(db_services, list) else 0
                            if db_services_count == 0:
                                raise RuntimeError(
                                    f"Important services persistence verification failed for {city_key}: "
                                    f"configured {len(expected_important_services)}, stored {db_services_count}"
                                )
                    else:
                        print(f"    ? DB read-back: Could not retrieve record")
                        raise RuntimeError(f"DB read-back failed for city_reports: {city_key}")
                else:
                    print(f"  ? Failed to save report to database")
                    raise RuntimeError(f"city_reports upsert failed for {city_key}")
                    
            except Exception as e:
                print(f"  ⚠ Warning: Could not save report to database: {e}")
                import traceback
                traceback.print_exc()
                if isinstance(e, RuntimeError) and (
                    "PDF/DB parity failure" in str(e)
                    or "city_reports upsert failed" in str(e)
                    or "DB read-back failed" in str(e)
                ):
                    raise
                if _should_fail_on_db_error():
                    raise
                # Continue anyway - PDF generation succeeded
            
        except ValidationError as e:
            print(f"Warning: Data validation failed: {e}")
            print("Falling back to minimal report structure")
            
            # Create minimal valid context
            city_key = self._location_to_city_key(location)
            now_utc = datetime.now(timezone.utc)
            generated_utc = now_utc.strftime('%Y-%m-%d %H:%M')
            report_id = f"{location.lower().replace(' ', '-')}-{now_utc.strftime('%Y%m%d-%H%M')}"
            
            # Get emergency contacts
            emergency_contacts = None
            try:
                ctx_obj = self.datastore.get_city_context(city_key)
                ctx = ctx_obj.context if ctx_obj and isinstance(ctx_obj.context, dict) else {}
                if isinstance(ctx, dict):
                    if isinstance(ctx.get("emergency_numbers"), dict):
                        emergency_contacts = ctx.get("emergency_numbers")
                    elif isinstance(ctx.get("emergency_contacts"), dict):
                        emergency_contacts = ctx.get("emergency_contacts")
            except Exception:
                emergency_contacts = None

            if not emergency_contacts:
                city_config = getattr(config, "CITIES", {}).get(city_key, {})
                emergency_contacts = get_emergency_contacts(city_config.get("country_code"))
            emergency_contacts = normalize_emergency_contacts(emergency_contacts)
            
            # Derive overall_risk from travel advisory (even in fallback)
            overall_risk = "unknown"
            allowed_risk_levels = {"low", "med", "high"}
            city_config = get_city_config(city_key)
            country_code = city_config.get("country_code", "US")
            is_us_destination = (country_code == "US")
            
            try:
                # Extract country_name from city_config name, not raw input location
                config_name = city_config.get("name", "")
                country_name = config_name.split(",")[-1].strip() if "," in config_name else None
                # Fallback to parsing input location if config name is missing
                if not country_name:
                    country_name = location.split(",")[-1].strip() if "," in location else None
                advisory_data = self.travel_advisory.get_advisory(country_code=country_code, country_name=country_name)
                
                if advisory_data:
                    normalized_risk = _derive_overall_risk_from_advisory(advisory_data, is_us_destination)
                    if normalized_risk:
                        overall_risk = normalized_risk
            except Exception as adv_err:
                print(f"  ⚠ Could not fetch advisory for fallback: {adv_err}")

            overall_risk = str(overall_risk or "").strip().lower()
            if overall_risk not in allowed_risk_levels:
                overall_risk = "high"
            
            context = ReportContext(
                destination=location,
                generated_utc=generated_utc,
                lookback_label="Past 48 hours",
                coverage_label="Partial data",
                emergency_contacts=emergency_contacts,
                report_id=report_id,
                overall_risk=overall_risk,
                top_risk_driver="None",
                coverage_status="Limited data",
                top_actions=["Monitor local conditions", "Keep emergency contacts ready"],
                whats_happening=["Data collection in progress"],
                risk_badges=[{"name": "Overall", "level": overall_risk}],
                cues=[{"cue": "Any concerns", "action": "Contact authorities"}],
                data_coverage="Limited data available",
            )
            
            fallback_context_dict = context.model_dump()
            # Ensure travel_essentials present in fallback (deterministic, no AI)
            try:
                from report_data_contract import build_travel_essentials
                import config as _config

                city_cfg = getattr(_config, "CITIES", {}).get(city_key, {}) or {"country_code": country_code}
                # For fallback we may not have parking; ensure at least eSIM is present
                fallback_context_dict["travel_essentials"] = build_travel_essentials(city_cfg, None)
            except Exception:
                # Best-effort: minimal shape
                fallback_context_dict.setdefault("travel_essentials", {"title": "Travel Essentials", "actions": []})
            
            if not skip_pdf:
                render_report_pdf(fallback_context_dict, output_path)
            
            # Save fallback report to database (same as success path)
            try:
                from helpers.enrich_google_maps_urls import enrich_report_with_google_maps_urls, _looks_like_place_list

                fallback_context_dict = enrich_report_with_google_maps_urls(
                    fallback_context_dict,
                    meta=fallback_context_dict.get("meta"),
                    city_config=city_cfg,
                )

                def _log_missing_google_maps_urls_fallback(report: Dict[str, Any]) -> None:
                    missing: Dict[str, Dict[str, int]] = {}

                    def walk(obj: Any) -> None:
                        if isinstance(obj, dict):
                            for k, v in obj.items():
                                if isinstance(v, list) and _looks_like_place_list(str(k), v):
                                    total = len(v)
                                    if total:
                                        missing_count = sum(
                                            1
                                            for item in v
                                            if isinstance(item, dict)
                                            and not str(item.get("google_maps_url") or "").strip()
                                        )
                                        missing[str(k)] = {"missing": missing_count, "total": total}
                                else:
                                    walk(v)
                        elif isinstance(obj, list):
                            for item in obj:
                                walk(item)

                    walk(report)
                    if missing:
                        print("\n=== Google Maps URL Coverage (fallback, post-enrichment) ===")
                        for key in sorted(missing.keys()):
                            stats = missing[key]
                            print(f"  • {key}: {stats['missing']}/{stats['total']} missing")

                _log_missing_google_maps_urls_fallback(fallback_context_dict)

                model_name = getattr(config, "OPENAI_MODEL", "gpt-5-mini")
                success = self.datastore.upsert_city_report(
                    city_key=city_key,
                    report_data=fallback_context_dict,
                    trend_data=None,
                    model=model_name,
                    window_hours=lookback_hours,
                    pdf_file_url=(None if skip_pdf else str(output_path.absolute())),
                    updated_by="travel_agent.py (fallback)",
                )
                if not success and _should_fail_on_db_error():
                    raise RuntimeError(f"city_reports upsert failed for {city_key} (fallback)")
            except Exception as db_error:
                print(f"  ??? Warning: Could not save fallback report to database: {db_error}")
                if _should_fail_on_db_error():
                    raise

        return output_path

    def generate_location_brief_db_first(
        self,
        location: str,
        output_dir: Path = Path("reports"),
        trip_profile: Optional[Dict] = None,
        skip_pdf: bool = False,
    ) -> Path:
        """
        Generate intelligence brief using ONLY database-stored data (database-first architecture).
        
        This method loads ALL data from Supabase tables using ReportDataLoader and tracks
        coverage to ensure data completeness. No direct API calls are made.
        
        PREREQUISITE: Run 'python scripts/sync_supabase.py --city <city_key>' first to populate data.
        """
        print("\n=== DATABASE-FIRST REPORT GENERATION ===")
        
        # Convert location to city_key
        city_key = self._location_to_city_key(location)
        print(f"City key: {city_key}")
        
        # Load all data from database using unified loader
        loader = ReportDataLoader(datastore=self.datastore)
        vm = loader.load_report_data(city_key, trip_profile=trip_profile)
        
        # Display coverage summary
        print(f"\n{vm.coverage.summary_message}")
        for section_name, section_cov in [
            ("Medical Facilities", vm.coverage.medical_facilities),
            ("Transit", vm.coverage.transit),
            ("Weather", vm.coverage.weather),
            ("Incidents", vm.coverage.incidents),
            ("Supplies", vm.coverage.supplies),
        ]:
            status_icon = "✓" if section_cov.status == CoverageStatus.OK else "⚠"
            print(f"  {status_icon} {section_name}: {section_cov.status.value}")
            if section_cov.reason:
                print(f"      Reason: {section_cov.reason}")
        
        # Get city config for metadata
        city_config = getattr(config, "CITIES", {}).get(city_key, {})
        country_code = city_config.get("country_code", "US")
        country_name = location.split(",")[-1].strip() if "," in location else None
        
        # Get travel advisory
        travel_advisory_data = {}
        try:
            travel_advisory_data = self.travel_advisory.get_advisory(country_code=country_code, country_name=country_name)
        except Exception as e:
            print(f"Warning: Failed to get travel advisory: {e}")
        
        # Build report_data dict from ReportViewModel for rendering
        report_data = {
            "top_actions": vm.top_actions or ["Monitor local conditions", "Keep emergency contacts ready"],
            "whats_happening": vm.whats_happening or ["Data collection in progress"],
            "risk_badges": vm.risk_badges or [{"name": "Overall", "level": "low"}],
            "cues": vm.cues or [{"cue": "Any concerns", "action": "Contact authorities"}],
            "incidents": vm.incidents or [],
            "weather_summary": vm.weather_summary or {},
            "opsec": vm.opsec or {"baseline_tips": [], "contextual_tips": []},
            "driving_pack": vm.driving_pack,
            "parking": vm.parking,  # SpotHero parking link for US cities
            "hospitals": vm.hospitals or [],
            "airports": vm.airports or [],
            "transit": vm.transit or [],
            "pharmacies": vm.pharmacies or [],
            "supermarkets": vm.supermarkets or [],
            "convenience_stores": vm.convenience_stores or [],
            "rental_cars": vm.rental_cars or [],
            "emergency_contacts": vm.emergency_contacts,
            "data_coverage": vm.coverage.summary_message,
        }
        if trip_profile:
            report_data["trip_profile"] = trip_profile
            if trip_profile.get("will_drive"):
                driving_pack = report_data.get("driving_pack")
                if not isinstance(driving_pack, dict):
                    driving_pack = {}
                driving_pack.setdefault("checklist", [])
                driving_pack.setdefault("rental_notes", [])
                driving_pack.setdefault("accident_steps", [])
                report_data["driving_pack"] = driving_pack
        
        # Generate PDF
        safe_location = location.lower().replace(" ", "-")
        output_path = output_dir / f"{safe_location}-intel-brief.pdf"
        
        # Use existing render_pdf method
        return self.render_pdf(
            report_data=report_data,
            location=location,
            output_path=output_path,
            travel_advisory_data=travel_advisory_data,
            events=vm.incidents,  # Pass incidents as events for metadata
            lookback_hours=48,  # Default lookback
            city_key=city_key,
            skip_pdf=skip_pdf,
        )

    def generate_location_brief(
        self,
        location: str,
        limit: int = DEFAULT_EVENT_LIMIT,
        lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
        output_dir: Path = Path("reports"),
        trip_profile: Optional[Dict] = None,
        city_key: Optional[str] = None,
        news_min_score: float = DEFAULT_NEWS_MIN_SCORE,
        news_relax_keywords: bool = False,
        news_ignore_location_filter: bool = False,
        news_min_keyword_matches: int = DEFAULT_NEWS_MIN_KEYWORDS,
        news_min_strong_matches: int = DEFAULT_NEWS_MIN_STRONG,
        news_include_global_critical: bool = False,
        skip_pdf: bool = False,
    ) -> Path:
        # Fetch city context (hospitals, transit)
        print()
        effective_city_key = (city_key or "").strip() or self._location_to_city_key(location)
        city_ctx = self.city_context.get_city_context(location, city_key=effective_city_key)
        print()
        
        # Collect incident/news events
        events, collection_stats = self.collect_events(
            location,
            limit=limit,
            lookback_hours=lookback_hours,
            city_key=effective_city_key,
            min_keyword_matches=news_min_keyword_matches,
            min_strong_matches=news_min_strong_matches,
            return_stats=True,
        )
        
        # Get appropriate travel advisory based on destination country
        city_key = effective_city_key
        city_config = get_city_config(city_key)
        country_code = city_config.get("country_code", "US")
        country_name = location.split(",")[-1].strip() if "," in location else None
        
        travel_advisory_data = {}
        try:
            travel_advisory_data = self.travel_advisory.get_advisory(country_code=country_code, country_name=country_name)
        except Exception as e:
            print(f"Warning: Failed to get travel advisory: {e}")
        
        # Generate structured report data (JSON)
        report_data, news_briefs = self.build_report_data(
            location,
            events,
            city_ctx,
            trip_profile,
            news_limit=limit,
            news_min_score=news_min_score,
            news_relax_keywords=news_relax_keywords,
            news_ignore_location_filter=news_ignore_location_filter,
            news_min_keyword_matches=news_min_keyword_matches,
            news_min_strong_matches=news_min_strong_matches,
            news_include_global_critical=news_include_global_critical,
            collection_stats=collection_stats,
            return_news_briefs=True,
            city_config=city_config,
        )
        if trip_profile:
            report_data["trip_profile"] = trip_profile
        
        safe_location = location.lower().replace(" ", "-")
        output_path = output_dir / f"{safe_location}-intel-brief.pdf"
        return self.render_pdf(
            report_data,
            location,
            output_path,
            travel_advisory_data,
            events,
            lookback_hours,
            city_key=city_key,
            news_briefs=news_briefs,
            skip_pdf=skip_pdf,
        )


def get_city_config(city_key: str) -> Dict:
    """Get city configuration DB-first.

    In production, cities are DB-owned. config.CITIES remains an optional bootstrap fallback.
    """

    # 1) DB-first: cities table
    try:
        datastore = get_datastore()
        client = getattr(datastore, "client", None)
        if client is not None:
            resp = (
                client.table("cities")
                .select("city_key,display_name,country_code,timezone,latitude,longitude,aliases,enabled")
                .eq("city_key", city_key)
                .limit(1)
                .execute()
            )
            if resp.data:
                row = resp.data[0]
                if row.get("enabled") is False:
                    raise ValueError(f"City '{city_key}' is disabled")
                return {
                    "name": row.get("display_name") or city_key,
                    "country_code": row.get("country_code"),
                    "timezone": row.get("timezone"),
                    "latitude": row.get("latitude"),
                    "longitude": row.get("longitude"),
                    "aliases": row.get("aliases") or [],
                    "enabled": True,
                }
    except Exception:
        pass

    # 2) Fallback: config
    cities = getattr(config, "CITIES", {})
    if city_key not in cities:
        available = ", ".join(cities.keys())
        raise ValueError(
            f"City '{city_key}' not found in database or config. Available config cities: {available}"
        )

    city_config = cities[city_key]
    if not city_config.get("enabled", True):
        raise ValueError(f"City '{city_key}' is disabled in configuration")

    return city_config


def list_available_cities() -> None:
    """Print available cities from config."""
    cities = getattr(config, "CITIES", {})
    if not cities:
        print("No cities configured.")
        return
    
    print("Available cities:")
    for key, city in cities.items():
        status = "enabled" if city.get("enabled", True) else "disabled"
        print(f"  {key}: {city.get('name', key)} [{status}]")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a one-page traveler intelligence brief for configured cities.",
        epilog="Currently focused on Miami. Use --list-cities to see available cities."
    )
    parser.add_argument(
        "city",
        nargs="?",
        default=getattr(config, "DEFAULT_CITY", "miami"),
        help="City key from config (default: miami). Use --list-cities to see options."
    )
    parser.add_argument("--limit", type=int, default=DEFAULT_EVENT_LIMIT, help="Number of events to keep (default: 10)")
    parser.add_argument("--hours", type=int, default=DEFAULT_LOOKBACK_HOURS, help="Lookback window in hours")
    parser.add_argument(
        "--news-lookback-hours",
        type=int,
        default=DEFAULT_LOOKBACK_HOURS,
        help="Lookback window in hours for news items (default: 168)",
    )
    parser.add_argument(
        "--output-dir", default="reports", help="Directory to store the generated PDF (will be created if missing)"
    )
    parser.add_argument("--list-cities", action="store_true", help="List available cities and exit")
    parser.add_argument(
        "--news-min-score",
        type=float,
        default=DEFAULT_NEWS_MIN_SCORE,
        help="Minimum travel relevance score to keep a news item (default: 0.5)",
    )
    parser.add_argument(
        "--news-min-keywords",
        type=int,
        default=DEFAULT_NEWS_MIN_KEYWORDS,
        help="Minimum regular keyword matches to keep a news item (default: 2)",
    )
    parser.add_argument(
        "--news-min-strong",
        type=int,
        default=DEFAULT_NEWS_MIN_STRONG,
        help="Minimum strong keyword matches to keep a news item (default: 1)",
    )
    parser.add_argument(
        "--news-relax-keywords",
        action="store_true",
        help="Relax keyword filter: allow 1 keyword or 0.3+ score when set",
    )
    parser.add_argument(
        "--news-ignore-location-filter",
        action="store_true",
        help="Ignore location filter and consider all events in the lookback window",
    )
    parser.add_argument(
        "--news-include-global-critical",
        action="store_true",
        help="Include strong-keyword events even if the city is not mentioned",
    )
    parser.add_argument(
        "--skip-news-preflight",
        action="store_true",
        help="Skip feed_items preflight check (not recommended)",
    )
    parser.add_argument(
        "--skip-pdf",
        action="store_true",
        help="Skip generating PDF; upsert JSON/trends only",
    )
    
    # Trip profile options
    parser.add_argument("--will-drive", action="store_true", help="Enable Driving Pack section (trip includes driving/rental)")
    parser.add_argument("--needs-idp", action="store_true", help="Flag that International Driving Permit is recommended")
    parser.add_argument("--rental-provider", type=str, default=None, help="Name of car rental company (optional)")
    parser.add_argument("--trip-notes", type=str, default=None, help="Additional trip context or notes")
    
    args = parser.parse_args()

    # List cities if requested
    if args.list_cities:
        list_available_cities()
        return

    # Get API keys
    api_key = os.getenv("OPENAI_API_KEY") or getattr(config, "OPENAI_API_KEY", "")
    if not api_key or "sk-" not in api_key:
        raise RuntimeError("OPENAI_API_KEY is missing. Set env var or config.OPENAI_API_KEY.")

    # Get city configuration
    city_key = args.city
    city_config = get_city_config(city_key)
    location = city_config["name"]
    
    print(f"Generating intelligence brief for: {location}")
    
    # Build trip profile if driving options specified
    trip_profile = None
    if args.will_drive:
        trip_profile = {
            "will_drive": True,
            "needs_idp": args.needs_idp,
            "rental_provider": args.rental_provider,
            "notes": args.trip_notes
        }
        print(f"  ✓ Driving Pack enabled (needs_idp={args.needs_idp})")
    
    # Generate brief
    agent = TravelIntelAgent(api_key=api_key)
    if not args.skip_news_preflight:
        if not ensure_city_news_ready(agent.datastore, city_key):
            sys.exit(2)

    news_lookback_hours = args.news_lookback_hours
    if not any(arg.startswith("--news-lookback-hours") for arg in sys.argv):
        news_lookback_hours = args.hours

    output_path = agent.generate_location_brief(
        location=location,
        limit=args.limit,
        lookback_hours=news_lookback_hours,
        output_dir=Path(args.output_dir),
        trip_profile=trip_profile,
        city_key=city_key,
        news_min_score=args.news_min_score,
        news_relax_keywords=args.news_relax_keywords,
        news_ignore_location_filter=args.news_ignore_location_filter,
        news_min_keyword_matches=args.news_min_keywords,
        news_min_strong_matches=args.news_min_strong,
        news_include_global_critical=args.news_include_global_critical,
        skip_pdf=args.skip_pdf,
    )
    print(f"Wrote intelligence brief to {output_path}")


if __name__ == "__main__":
    main()
