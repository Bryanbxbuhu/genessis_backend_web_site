"""
National Weather Service (NWS) alerts fetcher for US locations.

NWS provides real-time weather alerts, warnings, and watches for US locations.
This module fetches active alerts for a specific geographic point.
"""

import hashlib
import urllib.request
import json
from datetime import datetime, timezone
from typing import List

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from storage.base import FeedItem
from news_relevance import compute_travel_relevance, parse_published_at


# Severity mapping for magnitude calculation
SEVERITY_MAGNITUDE = {
    "Extreme": 5.0,
    "Severe": 4.0,
    "Moderate": 3.0,
    "Minor": 2.0,
    "Unknown": 2.0,
}


def fetch_nws_alerts(
    city_name: str,
    city_key: str,
    lat: float,
    lon: float,
) -> List[FeedItem]:
    """
    Fetch active National Weather Service alerts for a US location.
    
    Args:
        city_name: Human-readable city name for logging
        city_key: City identifier for database storage (e.g., "miami")
        lat: Latitude of the location
        lon: Longitude of the location
        
    Returns:
        List of FeedItem objects normalized for storage
        
    Example:
        >>> items = fetch_nws_alerts("Miami", "miami", 25.7617, -80.1918)
        >>> len(items)
        2  # e.g., Heat Advisory, Rip Current Statement
    """
    # NWS API endpoint for active alerts at a point
    # Documentation: https://www.weather.gov/documentation/services-web-api
    base_url = "https://api.weather.gov/alerts/active"
    url = f"{base_url}?point={lat},{lon}"
    
    print(f"   Fetching NWS alerts for {city_name} ({lat}, {lon})")
    
    try:
        # NWS API requires a User-Agent header
        headers = {
            "User-Agent": "OSINT-Project/1.0 (Travel Intelligence Bot)",
            "Accept": "application/geo+json",
        }
        
        req = urllib.request.Request(url, headers=headers)
        
        with urllib.request.urlopen(req, timeout=20) as response:
            data = json.loads(response.read().decode('utf-8'))
        
        features = data.get("features", [])
        print(f"   ✓ Retrieved {len(features)} NWS alerts")
        
        # Convert to FeedItem objects
        items = []
        now = datetime.now(timezone.utc)
        
        for feature in features:
            props = feature.get("properties", {})
            
            # Extract alert data
            alert_id = props.get("id", "")
            event = props.get("event", "Weather Alert")
            headline = props.get("headline", event)
            description = props.get("description", "").strip()
            severity = props.get("severity", "Unknown")
            urgency = props.get("urgency", "Unknown")
            certainty = props.get("certainty", "Unknown")
            
            # Parse sent/effective time
            sent_str = props.get("sent", "")
            effective_str = props.get("effective", "")
            
            published_at = parse_published_at(
                {"sent": sent_str, "effective": effective_str},
                now,
                preferred_fields=("sent", "effective"),
            )
            
            # Trim description to reasonable length for summary
            summary = description[:500] + "..." if len(description) > 500 else description
            if not summary:
                summary = headline
            
            # Use alert ID or fallback to deterministic hash
            guid = alert_id if alert_id else hashlib.md5(
                f"nws:{city_key}:{headline}:{sent_str}".encode()
            ).hexdigest()
            
            relevance = compute_travel_relevance(
                title=headline,
                summary=summary,
                content=description,
                categories=[event, severity],
            )

            # Create deterministic ID
            id_hash = hashlib.md5(f"nws_alerts:{city_key}:{guid}".encode()).hexdigest()[:16]
            
            # Calculate magnitude from severity
            magnitude = SEVERITY_MAGNITUDE.get(severity, 2.0)
            
            item = FeedItem(
                id=f"nws_{id_hash}",
                source_key="nws_alerts",
                city_key=city_key,
                guid=guid,
                url=alert_id if alert_id.startswith("http") else f"https://api.weather.gov/alerts/{alert_id.split('/')[-1]}" if alert_id else "",
                title=headline,
                summary=summary,
                published_at=published_at,
                fetched_at=now,
                travel_relevance_score=relevance["score"],
                travel_keywords_matched=relevance["matched_keywords"],
                travel_relevance_reason=relevance["reason"],
                raw={
                    "event": event,
                    "severity": severity,
                    "urgency": urgency,
                    "certainty": certainty,
                    "magnitude": magnitude,
                    "source": "nws_alerts",
                    "category": "weather",
                    "query_lat": lat,
                    "query_lon": lon,
                    "instruction": props.get("instruction", ""),
                    "response": props.get("response", ""),
                    "onset": props.get("onset", ""),
                    "expires": props.get("expires", ""),
                    "travel_relevance_score": relevance["score"],
                    "travel_keywords_matched": relevance["matched_keywords"],
                    "travel_relevance_reason": relevance["reason"],
                },
            )
            items.append(item)
        
        return items
        
    except urllib.error.HTTPError as e:
        print(f"   ✗ NWS API HTTP error: {e.code} {e.reason}")
        return []
    except urllib.error.URLError as e:
        print(f"   ✗ NWS API connection error: {e.reason}")
        return []
    except json.JSONDecodeError as e:
        print(f"   ✗ NWS API JSON decode error: {e}")
        return []
    except Exception as e:
        print(f"   ✗ NWS API unexpected error: {e}")
        return []
