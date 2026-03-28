"""
GDELT DOC 2.0 API fetcher optimized for geographic + keyword searches.

GDELT (Global Database of Events, Language, and Tone) provides real-time global news.
This module fetches travel-relevant crime and safety articles using location names + keywords.
"""

import hashlib
import urllib.request
import urllib.parse
import json
from datetime import datetime, timezone
from typing import List, Optional

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from storage.base import FeedItem
from news_relevance import compute_travel_relevance, parse_published_at


def fetch_gdelt_geo(
    city_name: str,
    city_key: str,
    lat: float,
    lon: float,
    radius_km: int = 50,
    start_dt: Optional[datetime] = None,
    end_dt: Optional[datetime] = None,
    max_records: int = 100,
    timespan: str = "7d",
) -> List[FeedItem]:
    """
    Fetch GDELT news articles using DOC 2.0 API with location + keywords.
    
    Uses GDELT DOC 2.0 API to find travel-relevant crime and safety news
    mentioning the city name combined with crime/safety keywords.
    
    Args:
        city_name: Human-readable city name (e.g., "Miami, Florida")
        city_key: City identifier for database storage (e.g., "miami")
        lat: Latitude (for reference, not used by DOC API)
        lon: Longitude (for reference, not used by DOC API)
        radius_km: Radius (for reference, not used by DOC API)
        start_dt: Start datetime (not used by DOC API)
        end_dt: End datetime (not used by DOC API)
        max_records: Maximum number of records to return (default: 100)
        timespan: Time window to search (default: "7d" for 7 days)
                  Options: "24h", "3d", "7d", "1w", "2w", "1m"
        
    Returns:
        List of FeedItem objects normalized for storage
        
    Example:
        >>> items = fetch_gdelt_geo("Miami", "miami", 25.7617, -80.1918)
        >>> len(items)
        42
    """
    # Extract base city name and country (for better location specificity)
    base_city = city_name.split(',')[0].strip()
    
    # Extract country if available (e.g., "Paris, France" -> "France")
    parts = [p.strip() for p in city_name.split(',')]
    country = parts[-1] if len(parts) > 1 else None
    
    # ========================================================================
    # TOURISM-FOCUSED SAFETY KEYWORDS
    # Optimized set - too many keywords can overwhelm GDELT API
    # ========================================================================
    
    # Core crime and safety keywords - focused on traveler-impacting events
    # Add broader keywords for better coverage
    tourism_keywords = [
        "robbery", "mugging", "carjacking", "shooting", "assault",
        "attack", "stabbing", "murder", "theft", "burglary",
        "crime", "violence", "incident", "fire", "explosion",
        "riot", "protest", "unrest", "arrested", "police"
    ]
    
    keyword_query = " OR ".join(tourism_keywords)
    
    # For international cities, use ONLY the country name to get broader French news
    # then filter by city relevance in post-processing
    # This ensures we don't miss Paris-area incidents just because "Paris" isn't in the headline
    if country and country.lower() not in ["united states", "usa", "us"]:
        # International city - search country-wide then filter locally
        full_query = f"{country} ({keyword_query}) sourcelang:english"
    else:
        # US city - use city name only (US cities get good local coverage)
        full_query = f"{base_city} ({keyword_query}) sourcelang:english"
    
    # Build GDELT DOC 2.0 API query
    # Documentation: https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/
    base_url = "https://api.gdeltproject.org/api/v2/doc/doc"
    
    params = {
        "query": full_query,
        "mode": "ArtList",  # Article list mode
        "format": "json",
        "maxrecords": str(max_records),
        "sort": "DateDesc",  # Newest first
        "timespan": timespan,
    }
    
    url = f"{base_url}?{urllib.parse.urlencode(params)}"
    
    print(f"   Fetching GDELT DOC articles for {base_city}")
    print(f"   Timespan: {timespan}, max: {max_records} articles")
    
    try:
        # Create SSL context that doesn't verify certificates (GDELT cert issues)
        import ssl
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        with urllib.request.urlopen(url, timeout=30, context=ssl_context) as response:
            raw_data = response.read().decode('utf-8')
            if not raw_data.strip():
                print("   [X] GDELT API returned empty response")
                return []
            
            # Check if response is HTML (error page)
            if raw_data.strip().startswith('<'):
                print(f"   [X] GDELT API returned HTML (likely error): {raw_data[:200]}")
                return []
                
            data = json.loads(raw_data)
        
        articles = data.get("articles", [])
        print(f"   [OK] Retrieved {len(articles)} GDELT articles")
        
        # Filter out non-tourism articles (political, sports, entertainment)
        exclude_keywords = [
            # Political (key terms only)
            "trump", "biden", "president", "senate", "congress", 
            "governor", "election", "campaign", "zelensky", "zelenskyy",
            "putin", "diplomatic", "embassy", "legislation", "bill signed",
            # Sports (expanded with common patterns)
            "nfl", "nba", "mlb", "nhl", "heat", "dolphins", "marlins", 
            "panthers", "inter miami", "bulls", "pacers", "nuggets",
            "playoff", "championship", "game score", "season opener",
            "midfielder", "goalkeeper", "striker", "points to lead",
            "knicks", "yankees", "mets", "rangers", "islanders",
            # Entertainment/Celebrity
            "kardashian", "celebrity", "movie premiere", "film festival",
            "red carpet", "award show",
            # Business/Finance
            "stock market", "merger", "acquisition", "earnings report", "ipo",
            # Lifestyle noise
            "economia e finanza", "tips from super", "how to live a healthy",
        ]
        
        # Filter for location relevance
        base_city_lower = base_city.lower()
        country_lower = country.lower() if country else ""
        
        filtered_articles = []
        def _truncate_summary(text: str, limit: int = 400) -> str:
            cleaned = (text or "").strip()
            if len(cleaned) <= limit:
                return cleaned
            return cleaned[: max(limit - 3, 0)].rstrip() + "..."

        for article in articles:
            title = article.get("title", "")
            title_lower = title.lower()
            
            # Skip if title contains excluded keywords
            if any(keyword in title_lower for keyword in exclude_keywords):
                continue
            
            # For international cities: REQUIRE city or country mention in title
            # This prevents getting random global news when we search "France"
            if country and country_lower not in ["united states", "usa", "us"]:
                # Must mention the city name OR country name in title
                if base_city_lower not in title_lower and country_lower not in title_lower:
                    continue
            
            # For US cities: Keep all (already city-specific from query)
            filtered_articles.append(article)
        
        filtered_count = len(articles) - len(filtered_articles)
        if filtered_count > 0:
            print(f"   [INFO] Filtered out {filtered_count} non-tourism articles (political/sports/entertainment/non-local)")
            articles = filtered_articles
        
        # Convert to FeedItem objects
        items = []
        now = datetime.now(timezone.utc)
        
        for article in articles:
            # Parse article data
            url_str = article.get("url", "")
            title = article.get("title", "Untitled")
            
            domain = article.get("domain", "")
            summary = (
                article.get("summary")
                or article.get("snippet")
                or (f"Article from {domain}" if domain else "News article")
            )
            summary = _truncate_summary(summary)
            
            # Parse seendate if available (format: YYYYMMDDTHHMMSSZ)
            seendate_str = article.get("seendate", "")
            publishtime_str = article.get("publishtime", "")
            translingual_str = article.get("translingual", "")
            published_at = parse_published_at(
                {
                    "seendate": seendate_str,
                    "publishtime": publishtime_str,
                    "translingual": translingual_str,
                },
                now,
                preferred_fields=("seendate", "publishtime", "translingual"),
            )
            
            relevance = compute_travel_relevance(
                title=title,
                summary=summary,
                content="",
                categories=None,
            )
            
            # Create deterministic ID from URL + source + city
            id_hash = hashlib.md5(f"gdelt_geo:{city_key}:{url_str}".encode()).hexdigest()[:16]
            
            item = FeedItem(
                id=f"gdelt_{id_hash}",
                source_key="gdelt_geo",
                city_key=city_key,
                guid=url_str,  # Use URL as guid for deduplication
                url=url_str,
                title=title,
                summary=summary,
                published_at=published_at,
                fetched_at=now,
                travel_relevance_score=relevance["score"],
                travel_keywords_matched=relevance["matched_keywords"],
                travel_relevance_reason=relevance["reason"],
                raw={
                    "domain": article.get("domain", ""),
                    "language": article.get("language", ""),
                    "seendate": seendate_str,
                    "publishtime": publishtime_str,
                    "translingual": translingual_str,
                    "socialimage": article.get("socialimage", ""),
                    "source": "gdelt_geo",
                    "query_lat": lat,
                    "query_lon": lon,
                    "query_radius_km": radius_km,
                    "travel_relevance_score": relevance["score"],
                    "travel_keywords_matched": relevance["matched_keywords"],
                    "travel_relevance_reason": relevance["reason"],
                },
            )
            items.append(item)
        
        return items
        
    except urllib.error.HTTPError as e:
        print(f"   [X] GDELT API HTTP error: {e.code} {e.reason}")
        return []
    except urllib.error.URLError as e:
        print(f"   [X] GDELT API connection error: {e.reason}")
        return []
    except json.JSONDecodeError as e:
        print(f"   [X] GDELT API JSON decode error: {e}")
        return []
    except Exception as e:
        print(f"   [X] GDELT API unexpected error: {e}")
        return []
