"""
Open-Meteo Weather Forecast Fetcher

Open-Meteo is a free, open-source weather API that provides:
- Current weather conditions
- Hourly forecasts (7 days)
- Daily forecasts (7 days)

No API key required. Data updated every hour.
Documentation: https://open-meteo.com/en/docs
"""

import hashlib
import urllib.request
import urllib.parse
import json
from datetime import datetime, timezone
from typing import List

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from storage.base import FeedItem
from news_relevance import parse_published_at


def fetch_open_meteo_forecast(
    city_name: str,
    city_key: str,
    lat: float,
    lon: float,
    timezone_str: str = "America/New_York",
) -> List[FeedItem]:
    """
    Fetch weather forecast from Open-Meteo API.
    
    Creates a single comprehensive FeedItem containing:
    - Current conditions (temp, precipitation, wind)
    - Hourly forecast (next 7 days)
    - Daily summary (highs, lows, precipitation)
    
    Args:
        city_name: Human-readable city name (e.g., "Miami, Florida")
        city_key: City identifier for database storage (e.g., "miami")
        lat: Latitude
        lon: Longitude
        timezone_str: IANA timezone (default: "America/New_York")
        
    Returns:
        List with single FeedItem containing complete weather forecast
        
    Example:
        >>> items = fetch_open_meteo_forecast("Miami", "miami", 25.7617, -80.1918)
        >>> len(items)
        1
    """
    base_url = "https://api.open-meteo.com/v1/forecast"
    
    params = {
        "latitude": str(lat),
        "longitude": str(lon),
        "current": "temperature_2m,precipitation,wind_speed_10m,relative_humidity_2m,weather_code",
        "hourly": "temperature_2m,precipitation_probability,precipitation,wind_speed_10m,weather_code",
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,precipitation_probability_max,weather_code",
        "timezone": timezone_str,
        "temperature_unit": "fahrenheit",  # Use Fahrenheit for US cities
        "wind_speed_unit": "mph",
        "precipitation_unit": "inch",
    }
    
    url = f"{base_url}?{urllib.parse.urlencode(params)}"
    
    print(f"   Fetching weather forecast for {city_name}")
    
    try:
        with urllib.request.urlopen(url, timeout=15) as response:
            data = json.loads(response.read().decode('utf-8'))
        
        # Parse current conditions
        current = data.get("current", {})
        current_temp = current.get("temperature_2m", 0)
        current_precip = current.get("precipitation", 0)
        current_wind = current.get("wind_speed_10m", 0)
        current_time = current.get("time", "")
        
        # Parse daily forecast (next 7 days)
        daily = data.get("daily", {})
        daily_times = daily.get("time", [])
        daily_max = daily.get("temperature_2m_max", [])
        daily_min = daily.get("temperature_2m_min", [])
        daily_precip = daily.get("precipitation_sum", [])
        
        # Create human-readable summary
        title = f"Weather Forecast for {city_name}"
        
        # Build summary text
        summary_parts = [
            f"Current: {current_temp:.1f}°F",
            f"Wind: {current_wind:.1f} mph",
        ]
        
        if current_precip > 0:
            summary_parts.append(f"Precipitation: {current_precip:.2f} in")
        
        # Add 7-day outlook
        if daily_times and daily_max and daily_min:
            outlook = []
            for i, date in enumerate(daily_times[:7]):
                if i < len(daily_max) and i < len(daily_min):
                    day_name = datetime.fromisoformat(date).strftime("%a %m/%d")
                    high = daily_max[i]
                    low = daily_min[i]
                    precip = daily_precip[i] if i < len(daily_precip) else 0
                    
                    day_summary = f"{day_name}: {high:.0f}°/{low:.0f}°F"
                    if precip > 0.01:
                        day_summary += f", {precip:.2f} in rain"
                    outlook.append(day_summary)
            
            if outlook:
                summary_parts.append(f"\n\n7-Day Forecast:\n" + "\n".join(outlook))
        
        summary = ". ".join(summary_parts[:2]) + ("." if len(summary_parts) <= 2 else summary_parts[2])
        
        # Create unique ID
        now = datetime.now(timezone.utc)
        id_hash = hashlib.md5(f"open_meteo:{city_key}:{current_time}".encode()).hexdigest()[:16]
        
        # Store full forecast data in raw field for report generation
        published_at = parse_published_at({"published": current_time}, now, preferred_fields=("published",))
        item = FeedItem(
            id=f"weather_{id_hash}",
            source_key="open_meteo_forecast",
            city_key=city_key,
            guid=f"open-meteo-{city_key}-{current_time}",
            url=f"https://open-meteo.com/en/docs#latitude={lat}&longitude={lon}",
            title=title,
            summary=summary,
            published_at=published_at,
            fetched_at=now,
            raw={
                "current": current,
                "hourly": data.get("hourly", {}),
                "daily": daily,
                "timezone": data.get("timezone", timezone_str),
                "latitude": data.get("latitude", lat),
                "longitude": data.get("longitude", lon),
                "source": "open_meteo_forecast",
            },
        )
        
        print(f"   ✓ Retrieved weather forecast: {current_temp:.1f}°F")
        return [item]
        
    except urllib.error.HTTPError as e:
        print(f"   ✗ Weather API HTTP error: {e.code} {e.reason}")
        return []
    except urllib.error.URLError as e:
        print(f"   ✗ Weather API connection error: {e.reason}")
        return []
    except json.JSONDecodeError as e:
        print(f"   ✗ Weather API JSON decode error: {e}")
        return []
    except Exception as e:
        print(f"   ✗ Weather API unexpected error: {e}")
        return []
