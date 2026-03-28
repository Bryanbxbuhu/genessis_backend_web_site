"""Google Places API helper for enriching facility websites.

Uses RapidAPI's Google Places API to look up official websites for facilities
when other sources (Nominatim, cross-city fallback) are unavailable.

⚠️ QUOTA: 100 calls per day - must be used sparingly.
"""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Tuple
from pathlib import Path

import requests
import config

logger = logging.getLogger(__name__)

# RapidAPI endpoints
NEARBYSEARCH_ENDPOINT = "https://google-maps-api3.p.rapidapi.com/maps/api/place/nearbysearch/json"
DETAILS_ENDPOINT = "https://google-map-places.p.rapidapi.com/maps/api/place/details/json"
DETAILS_HOST = "google-map-places.p.rapidapi.com"


class GooglePlacesLookup:
    """Rate-limited Google Places website lookup with local caching."""
    
    def __init__(self, rapidapi_key: str = None, rapidapi_host: str = None):
        """Initialize with RapidAPI credentials."""
        self.rapidapi_key = rapidapi_key or config.RAPIDAPI_KEY
        self.rapidapi_host = rapidapi_host or config.RAPIDAPI_HOST
        self.enabled = config.GOOGLE_PLACES_ENABLED and bool(self.rapidapi_key)
        self.max_daily_calls = config.GOOGLE_PLACES_MAX_CALLS_PER_DAY
        
        # In-memory cache: (normalized_name, category) -> {website, cached_at}
        self._cache: Dict[tuple, Dict] = {}
        
        # Rate limiting
        self._calls_today = 0
        self._last_call_time = 0.0
        self._date_of_last_call = None
        self._rate_limit_delay_seconds = 0.5  # Be conservative: 2 calls/sec max

    def _reset_daily_counter_if_needed(self):
        """Reset call counter at midnight UTC."""
        today = datetime.now(timezone.utc).date()
        if self._date_of_last_call != today:
            self._calls_today = 0
            self._date_of_last_call = today

    def _rate_limit(self):
        """Enforce minimum delay between API calls."""
        elapsed = time.time() - self._last_call_time
        if elapsed < self._rate_limit_delay_seconds:
            time.sleep(self._rate_limit_delay_seconds - elapsed)
        self._last_call_time = time.time()

    def _quota_exceeded(self) -> bool:
        """Check if daily quota is exhausted."""
        self._reset_daily_counter_if_needed()
        return self._calls_today >= self.max_daily_calls

    def _get_cache_key(self, name: str, category: str) -> tuple:
        """Normalize name and category for cache key."""
        normalized_name = name.lower().strip()
        return (normalized_name, category)

    def _get_cached(self, name: str, category: str) -> Optional[str]:
        """Retrieve cached website from in-memory store."""
        key = self._get_cache_key(name, category)
        if key in self._cache:
            entry = self._cache[key]
            cached_at = entry.get("cached_at")
            website = entry.get("website")
            # Cache valid for 24 hours
            if cached_at and (datetime.now(timezone.utc).timestamp() - cached_at) < 86400:
                logger.info(f"  INFO Google Places cache hit for '{name}' ({category})")
                return website
        return None

    def _set_cached(self, name: str, category: str, website: Optional[str]):
        """Store result in in-memory cache."""
        key = self._get_cache_key(name, category)
        self._cache[key] = {
            "website": website,
            "cached_at": datetime.now(timezone.utc).timestamp()
        }

    def _lookup_place_details(self, place_id: str) -> Optional[str]:
        """Look up place details by place_id to get website (supplemental method).
        
        Args:
            place_id: Google Places place_id from nearbysearch result
            
        Returns:
            Website URL from result.website if found, None otherwise
        """
        try:
            # Rate limit
            self._rate_limit()

            # Build request for details endpoint
            url = DETAILS_ENDPOINT
            
            params = {
                "place_id": place_id,
                "fields": "website,name",  # Only request what we need
                "language": "en",
            }

            headers = {
                "x-rapidapi-key": self.rapidapi_key,
                "x-rapidapi-host": DETAILS_HOST,
            }

            # Make request
            response = requests.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()

            data = response.json()
            self._calls_today += 1

            # Extract website from result.website
            result = data.get("result", {})
            website = result.get("website")
            
            if website:
                logger.info(f"  INFO Google Places Details API found website for place_id {place_id}: {website}")
                return website
            else:
                logger.debug(f"  INFO Google Places Details API returned no website for place_id {place_id}")
                return None

        except requests.exceptions.Timeout:
            logger.warning(f"  WARN Google Places Details timeout for place_id {place_id}")
            return None
        except requests.exceptions.RequestException as e:
            logger.warning(f"  WARN Google Places Details error for place_id {place_id}: {type(e).__name__}")
            return None
        except (KeyError, ValueError) as e:
            logger.warning(f"  WARN Google Places Details response parse error for place_id {place_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"  ERROR Google Places Details unexpected error for place_id {place_id}: {e}")
            return None

    def lookup_place_website(
        self,
        name: str,
        latitude: float,
        longitude: float,
        category: str = "hospital",
        radius: int = 10000,
    ) -> Optional[str]:
        """Look up official website for a facility using Google Places API.

        Args:
            name: Facility name (used for filtering results)
            latitude: Facility latitude
            longitude: Facility longitude
            category: Facility category (e.g., "hospital", "pharmacy", "rental_car")
            radius: Search radius in meters (default 10km)

        Returns:
            Official website URL (websiteUri) if found, None otherwise
        """
        if not self.enabled:
            return None

        # Check in-memory cache first
        cached = self._get_cached(name, category)
        if cached is not None:
            return cached

        # Check quota
        if self._quota_exceeded():
            logger.warning(
                f"  WARN Google Places daily quota ({self.max_daily_calls} calls) exhausted. "
                f"Skipping lookup for '{name}'."
            )
            return None

        try:
            # Rate limit
            self._rate_limit()

            # Build request
            url = NEARBYSEARCH_ENDPOINT
            
            payload = {
                "location": f"{latitude},{longitude}",
                "radius": radius,
                "keyword": name,  # Include name to filter to relevant place
                "type": "point_of_interest",
            }

            headers = {
                "X-RapidAPI-Key": self.rapidapi_key,
                "X-RapidAPI-Host": self.rapidapi_host,
            }

            # Make request
            response = requests.post(url, data=payload, headers=headers, timeout=10)
            response.raise_for_status()

            data = response.json()
            self._calls_today += 1

            # Extract website from first result
            results = data.get("results", [])
            if not results:
                logger.debug(f"  INFO No Google Places results for '{name}' at {latitude},{longitude}")
                self._set_cached(name, category, None)
                return None

            # Look for result matching name (best effort)
            best_result = results[0]  # Default to first result
            for result in results:
                result_name = result.get("name", "").lower()
                if name.lower() in result_name or result_name in name.lower():
                    best_result = result
                    break

            website = best_result.get("website")
            if website:
                logger.info(f"  INFO Google Places found website for '{name}': {website}")
                self._set_cached(name, category, website)
                return website
            
            # If no website in nearbysearch, try details API with place_id
            place_id = best_result.get("place_id")
            if place_id:
                logger.debug(f"  INFO No website in nearbysearch, trying Details API for '{name}'")
                # Check quota again before second call
                if self._quota_exceeded():
                    logger.warning(f"  WARN Quota exhausted, cannot query Details API for '{name}'")
                    self._set_cached(name, category, None)
                    return None
                
                details_website = self._lookup_place_details(place_id)
                if details_website:
                    self._set_cached(name, category, details_website)
                    return details_website
            
            logger.debug(f"  INFO Google Places result for '{name}' has no website field")
            self._set_cached(name, category, None)
            return None

        except requests.exceptions.Timeout:
            logger.warning(f"  WARN Google Places timeout for '{name}': request timed out")
            return None
        except requests.exceptions.RequestException as e:
            logger.warning(f"  WARN Google Places error for '{name}': {type(e).__name__} - {str(e)[:100]}")
            return None
        except (KeyError, ValueError) as e:
            logger.warning(f"  WARN Google Places response parse error for '{name}': {e}")
            return None
        except Exception as e:
            logger.error(f"  ERROR Google Places unexpected error for '{name}': {e}")
            return None


# Global singleton instance
_google_places_instance = None


def get_google_places() -> GooglePlacesLookup:
    """Get or create global GooglePlacesLookup instance."""
    global _google_places_instance
    if _google_places_instance is None:
        _google_places_instance = GooglePlacesLookup()
    return _google_places_instance
