"""
City Context Tool - Fetch real location data to prevent LLM hallucinations.

Provides:
- Geocoding (Nominatim)
- Hospitals and Urgent Care (OpenStreetMap Overpass API) - cached monthly

This ensures the LLM only uses real, verified data for medical facilities.
Caching prevents excessive API calls since infrastructure changes are infrequent.

Note: Transit information is fetched separately using DuckDuckGo search.

Storage: Uses DataStore abstraction (file-based or Supabase) via storage.get_datastore()
"""

import json
import re
import time
from math import atan2, cos, radians, sin, sqrt
from urllib.parse import urlparse
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
import requests
from ddgs import DDGS
from openai import OpenAI
import config

from helpers.store_chain_canonical import canonicalize_store_website, is_store_chain_name

from storage import get_datastore
from storage.base import CityContext
from storage.normalize import normalize_domain, is_aggregator_domain
from emergency_numbers import get_emergency_contacts


class CityContextTool:
    """Fetch verified city context data (hospitals, geocoding) to prevent hallucinations."""
    
    def __init__(self, user_agent: str = "OSINT-TravelAgent/1.0", openai_api_key: Optional[str] = None, datastore=None) -> None:
        self.user_agent = user_agent
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": user_agent})
        self._geocode_cache: Dict[str, Dict] = {}
        self._reverse_geocode_cache: Dict[Tuple[float, float], str] = {}  # Cache for reverse geocoding
        self._last_nominatim_call = 0.0
        self._wikidata_entity_cache: Dict[str, Dict] = {}
        self._wikidata_website_cache: Dict[str, Optional[str]] = {}
        self._wikidata_search_cache: Dict[Tuple[str, float, float], Optional[str]] = {}
        self.openai_client = OpenAI(api_key=openai_api_key) if openai_api_key else None
        self.cache_refresh_days = 30  # Refresh city context monthly
        
        # Use provided datastore or get default
        self.datastore = datastore or get_datastore()
    
    def _save_city_cache(self, location: str, context_data: Dict, city_key: Optional[str] = None) -> None:
        """Save city context via datastore.

        Args:
            location: Human-readable location string
            context_data: Snapshot data to persist
            city_key: Optional explicit city_key to use for DB writes
        """
        # Determine city_key from override or location
        city_key = (city_key or "").strip() or self._location_to_city_key(location)
        
        # Create CityContext object
        context = CityContext(
            city_key=city_key,
            context=context_data,
            fetched_at=datetime.now(timezone.utc),
        )
        
        # Save via datastore
        success = self.datastore.upsert_city_context(context)
        
        if not success:
            print(f"  WARN Failed to save city context")
    
    def _load_city_cache(self, location: str, city_key: Optional[str] = None) -> Optional[Dict]:
        """Load city context via datastore.

        Args:
            location: Human-readable location string
            city_key: Optional explicit city_key to use for DB reads
        """
        city_key = (city_key or "").strip() or self._location_to_city_key(location)
        
        # Try to load from datastore
        context = self.datastore.get_city_context(city_key)
        
        if context and self.datastore.is_city_context_fresh(city_key, self.cache_refresh_days):
            cached = context.context
            if isinstance(cached, dict):
                hospitals = cached.get("hospitals")
                pharmacies = cached.get("pharmacies")
                if isinstance(hospitals, list) and isinstance(pharmacies, list):
                    if not hospitals and not pharmacies:
                        return None
                # Only fill if DB snapshot is missing emergency data.
                if "emergency_contacts" not in cached and "emergency_numbers" not in cached:
                    resolved = cached.get("resolved_location") if isinstance(cached.get("resolved_location"), dict) else None
                    contacts = self._build_emergency_contacts(city_key, resolved)
                    cached["emergency_contacts"] = contacts
                    cached["emergency_numbers"] = contacts
                    self._save_city_cache(location, cached)
                if "airports" not in cached:
                    try:
                        import config

                        cached["airports"] = list((config.CITIES.get(city_key, {}) or {}).get("airports") or [])
                    except Exception:
                        cached["airports"] = []
                    self._save_city_cache(location, cached)
            return cached
        
        return None
    
    def _location_to_city_key(self, location: str) -> str:
        """
        Convert location string to city_key.
        
        Tries to match against configured cities, falls back to normalized location.
        """
        import config
        
        location_lower = location.lower()
        
        # Try to match against configured cities
        for key, city_config in getattr(config, "CITIES", {}).items():
            city_name = city_config.get("name", "").lower()
            aliases = [a.lower() for a in city_config.get("aliases", [])]
            
            if location_lower in aliases or location_lower in city_name:
                return key
        
        # Fallback: normalize location to key
        safe_name = location.lower().replace(" ", "_").replace(",", "")
        safe_name = "".join(c for c in safe_name if c.isalnum() or c == "_")
        return safe_name

    def _build_emergency_contacts(self, city_key: str, resolved_location: Optional[Dict]) -> Dict[str, object]:
        """Build emergency contacts using country defaults.

        Production source-of-truth is the database snapshot (city_context_snapshots.context).
        This helper is only used to fill missing values.
        """
        country_code = ""
        if resolved_location:
            country_code = resolved_location.get("country_code", "") or ""

        return get_emergency_contacts(country_code)
    
    def _rate_limit_nominatim(self) -> None:
        """Enforce Nominatim's 1 request per second policy."""
        elapsed = time.time() - self._last_nominatim_call
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)
        self._last_nominatim_call = time.time()
    
    def _ddg_search_with_timeout(self, query: str, max_results: int = 5, timeout_seconds: int = 10) -> Optional[List[Dict]]:
        """
        Perform a DuckDuckGo search with a timeout.
        
        The DDGS library has no explicit timeout and can hang indefinitely.
        This wrapper ensures searches complete within a reasonable time.
        
        Args:
            query: Search query
            max_results: Maximum number of results to return
            timeout_seconds: Timeout in seconds (default 10)
            
        Returns:
            List of search results or None if timeout/error occurs
        """
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(lambda: list(DDGS().text(query, max_results=max_results)))
                results = future.result(timeout=timeout_seconds)
                return results if results else None
        except FuturesTimeoutError:
            print(f"  WARN DDG search timed out after {timeout_seconds}s: {query[:50]}...")
            return None
        except Exception as e:
            print(f"  WARN DDG search failed: {str(e)[:100]}")
            return None
    
    def _lookup_osm_details(self, osm_id: int, osm_type: str) -> Optional[Dict]:
        """Lookup OSM element details via Nominatim with extratags.
        
        Args:
            osm_id: OSM element ID
            osm_type: OSM element type ("node", "way", or "relation")
            
        Returns:
            Dict with extratags and other details, or None if lookup fails
        """
        # Map type to Nominatim osmtype
        type_map = {"node": "N", "way": "W", "relation": "R"}
        osmtype = type_map.get(osm_type)
        if not osmtype:
            return None
        
        self._rate_limit_nominatim()
        
        try:
            resp = self._session.get(
                "https://nominatim.openstreetmap.org/details",
                params={
                    "osmtype": osmtype,
                    "osmid": osm_id,
                    "format": "json",
                    "extratags": 1,
                },
                timeout=10
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            return None
    
    def _reverse_geocode(self, lat: float, lon: float) -> str:
        """
        Reverse geocode coordinates to get a formatted address.
        Uses in-memory cache to avoid redundant API calls.
        
        Args:
            lat: Latitude
            lon: Longitude
            
        Returns:
            Formatted address string, or "Lat, Lon" fallback if geocoding fails
        """
        # Round coordinates to ~100m precision for cache key
        cache_key = (round(lat, 3), round(lon, 3))
        
        # Check cache first
        if cache_key in self._reverse_geocode_cache:
            return self._reverse_geocode_cache[cache_key]
        
        # Perform reverse geocoding
        try:
            self._rate_limit_nominatim()
            resp = self._session.get(
                "https://nominatim.openstreetmap.org/reverse",
                params={
                    "lat": lat,
                    "lon": lon,
                    "format": "json",
                    "addressdetails": 1,
                },
                timeout=10
            )
            resp.raise_for_status()
            data = resp.json()
            
            # Extract address components
            address_parts = data.get("address", {})
            
            # Build concise address (street, city, postcode)
            parts = []
            
            # Street address
            if "road" in address_parts:
                house_num = address_parts.get("house_number", "")
                road = address_parts["road"]
                parts.append(f"{house_num} {road}".strip() if house_num else road)
            
            # City
            city = (address_parts.get("city") or 
                   address_parts.get("town") or 
                   address_parts.get("village") or
                   address_parts.get("municipality"))
            if city:
                parts.append(city)
            
            # Postcode
            if "postcode" in address_parts:
                parts.append(address_parts["postcode"])
            
            if parts:
                address = ", ".join(parts)
            else:
                # Fallback to display_name if no structured address
                address = data.get("display_name", f"{lat}, {lon}")
            
            # Cache the result
            self._reverse_geocode_cache[cache_key] = address
            return address
            
        except Exception as e:
            # Fallback to lat, lon
            fallback = f"{lat:.4f}, {lon:.4f}"
            self._reverse_geocode_cache[cache_key] = fallback
            return fallback
    
    def geocode_location(self, location: str) -> Optional[Dict]:
        """
        Geocode a location to get lat, lon, bbox, country_code.
        Uses Nominatim with proper rate limiting and caching.
        
        Returns:
            {
                "lat": float,
                "lon": float,
                "bbox": [west, south, east, north],
                "country_code": str,
                "display_name": str
            }
        """
        # Check cache
        if location in self._geocode_cache:
            return self._geocode_cache[location]
        
        # Rate limit (Nominatim requires max 1 req/sec)
        self._rate_limit_nominatim()
        
        try:
            url = "https://nominatim.openstreetmap.org/search"
            params = {
                "q": location,
                "format": "json",
                "limit": 1,
                "addressdetails": 1,
            }
            
            resp = self._session.get(url, params=params, timeout=10)
            resp.raise_for_status()
            results = resp.json()
            
            if not results:
                return None
            
            result = results[0]
            bbox = result.get("boundingbox", [])
            if len(bbox) == 4:
                # Nominatim returns [south, north, west, east]
                # Convert to [west, south, east, north] for consistency
                bbox = [float(bbox[2]), float(bbox[0]), float(bbox[3]), float(bbox[1])]
            
            geocoded = {
                "lat": float(result.get("lat", 0)),
                "lon": float(result.get("lon", 0)),
                "bbox": bbox,
                "country_code": result.get("address", {}).get("country_code", "").upper(),
                "display_name": result.get("display_name", location),
                "address": result.get("address", {}) or {},  # Preserve address for location label building
            }
            
            # Cache result
            self._geocode_cache[location] = geocoded
            return geocoded
            
        except Exception as e:
            print(f"Warning: Geocoding failed for '{location}': {e}")
            return None
    
    @staticmethod
    def _normalize_facility_name(name: str) -> str:
        """Normalize facility name for deduplication."""
        return re.sub(r'[^a-z0-9]+', '', name.lower())

    @staticmethod
    def _clean_facility_name(name: str) -> str:
        """Normalize whitespace and trailing punctuation for facility names."""
        text = (name or "").strip()
        if not text:
            return ""
        text = re.sub(r"\s+", " ", text)
        text = re.sub(r"[\s\.;:,]+$", "", text)
        return text

    @staticmethod
    def _extract_website(tags: Dict, extratags: Optional[Dict] = None) -> str:
        """Pick the first available website field from OSM tags or extratags.
        
        Priority order: contact:website, website, url, contact:url, operator:website
        Checks both tags and extratags (from Nominatim details API)
        """
        keys = ["contact:website", "website", "url", "contact:url", "operator:website"]
        # Check extratags first (more authoritative from Nominatim)
        if extratags:
            for key in keys:
                value = extratags.get(key, "")
                if isinstance(value, str) and value.strip():
                    return value.strip()
        
        # Fallback to tags from Overpass
        for key in keys:
            value = tags.get(key, "")
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    @staticmethod
    def _normalize_website_url(url: str) -> str:
        """Normalize URL while preserving meaningful paths for facility websites."""
        if not url or not url.strip():
            return ""
        raw = url.strip()
        parsed = urlparse(raw if "://" in raw else f"https://{raw}")
        host = (parsed.netloc or "").strip().lower()
        if not host:
            return ""
        path = (parsed.path or "").strip()
        if path and path != "/":
            path = path.rstrip("/")
        else:
            path = ""
        normalized = f"https://{host}{path}"
        return normalized

    @staticmethod
    def _is_generic_homepage(url: str) -> bool:
        """Check if URL is just a homepage with no path."""
        if not url:
            return False
        parsed = urlparse(url if "://" in url else f"https://{url}")
        path = (parsed.path or "").strip()
        return path == "" or path == "/"

    @staticmethod
    def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Compute distance between two lat/lon points in kilometers."""
        r = 6371.0
        dlat = radians(lat2 - lat1)
        dlon = radians(lon2 - lon1)
        a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        return r * c

    @staticmethod
    def _normalize_wikidata_id(wikidata_id: Optional[str]) -> str:
        if not wikidata_id:
            return ""
        text = str(wikidata_id).strip()
        if not text:
            return ""
        if not text.upper().startswith("Q"):
            return ""
        return text.upper()

    def _fetch_wikidata_entity(self, wikidata_id: str) -> Optional[Dict]:
        if wikidata_id in self._wikidata_entity_cache:
            return self._wikidata_entity_cache[wikidata_id]
        try:
            resp = self._session.get(
                f"https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json",
                timeout=10,
            )
            resp.raise_for_status()
            payload = resp.json()
            entity = (payload.get("entities") or {}).get(wikidata_id)
            if isinstance(entity, dict):
                self._wikidata_entity_cache[wikidata_id] = entity
                return entity
        except Exception:
            return None
        return None

    def _clamp_bbox(self, resolved: dict, max_delta: float = 0.20) -> None:
        bbox = resolved.get("bbox")
        if not isinstance(bbox, list) or len(bbox) != 4:
            return
        west, south, east, north = bbox
        try:
            span_lon = float(east) - float(west)
            span_lat = float(north) - float(south)
            lat = float(resolved.get("lat", 0.0))
            lon = float(resolved.get("lon", 0.0))
        except Exception:
            return

        if span_lon <= (max_delta * 2.0) and span_lat <= (max_delta * 2.0):
            return

        resolved["bbox"] = [lon - max_delta, lat - max_delta, lon + max_delta, lat + max_delta]

    @staticmethod
    def _extract_wikidata_website(entity: Dict) -> str:
        claims = entity.get("claims", {}) if isinstance(entity, dict) else {}
        entries = claims.get("P856") or []
        for entry in entries:
            mainsnak = entry.get("mainsnak", {}) if isinstance(entry, dict) else {}
            datavalue = mainsnak.get("datavalue", {}) if isinstance(mainsnak, dict) else {}
            value = datavalue.get("value")
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    @staticmethod
    def _extract_wikidata_coords(entity: Dict) -> Optional[Tuple[float, float]]:
        claims = entity.get("claims", {}) if isinstance(entity, dict) else {}
        entries = claims.get("P625") or []
        for entry in entries:
            mainsnak = entry.get("mainsnak", {}) if isinstance(entry, dict) else {}
            datavalue = mainsnak.get("datavalue", {}) if isinstance(mainsnak, dict) else {}
            value = datavalue.get("value", {}) if isinstance(datavalue, dict) else {}
            lat = value.get("latitude")
            lon = value.get("longitude")
            if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
                return float(lat), float(lon)
        return None

    def _search_wikidata_ids(self, name: str, limit: int = 5) -> List[str]:
        if not name or not name.strip():
            return []
        try:
            resp = self._session.get(
                "https://www.wikidata.org/w/api.php",
                params={
                    "action": "wbsearchentities",
                    "search": name,
                    "format": "json",
                    "language": "en",
                    "limit": limit,
                },
                timeout=10,
            )
            resp.raise_for_status()
            payload = resp.json()
            results = payload.get("search", []) if isinstance(payload, dict) else []
            ids = []
            for item in results:
                wikidata_id = (item or {}).get("id")
                normalized = self._normalize_wikidata_id(wikidata_id)
                if normalized:
                    ids.append(normalized)
            return ids
        except Exception:
            return []

    def _resolve_wikidata_website(
        self,
        name: str,
        lat: Optional[float],
        lon: Optional[float],
        *,
        wikidata_id: Optional[str] = None,
    ) -> Optional[str]:
        normalized_id = self._normalize_wikidata_id(wikidata_id)
        if normalized_id:
            if normalized_id in self._wikidata_website_cache:
                return self._wikidata_website_cache[normalized_id]
            entity = self._fetch_wikidata_entity(normalized_id)
            if not entity:
                self._wikidata_website_cache[normalized_id] = None
                return None
            website = self._extract_wikidata_website(entity)
            normalized = self._normalize_website_url(website)
            if normalized and not self._is_aggregator(normalized):
                self._wikidata_website_cache[normalized_id] = normalized
                return normalized
            self._wikidata_website_cache[normalized_id] = None
            return None

        if lat is None or lon is None:
            return None

        cache_key = (name.strip().lower(), round(float(lat), 3), round(float(lon), 3))
        if cache_key in self._wikidata_search_cache:
            return self._wikidata_search_cache[cache_key]

        best_url = None
        best_distance = None
        for candidate_id in self._search_wikidata_ids(name):
            entity = self._fetch_wikidata_entity(candidate_id)
            if not entity:
                continue
            coords = self._extract_wikidata_coords(entity)
            if not coords:
                continue
            distance = self._haversine_km(lat, lon, coords[0], coords[1])
            if distance > 10.0:
                continue
            website = self._extract_wikidata_website(entity)
            normalized = self._normalize_website_url(website)
            if not normalized or self._is_aggregator(normalized):
                continue
            if best_distance is None or distance < best_distance:
                best_distance = distance
                best_url = normalized

        self._wikidata_search_cache[cache_key] = best_url
        return best_url
    
    @staticmethod
    def _score_facility(facility: Dict) -> int:
        """
        Score facility to prioritize major hospitals.
        Higher score = more important facility.
        """
        score = 0
        tags = facility.get("tags", {})
        name = facility.get("name", "").lower()
        
        # Emergency capability (highest priority)
        if tags.get("emergency") == "yes":
            score += 100
        elif "emergency" in tags.get("emergency", "").lower():
            score += 50
        
        # Hospital type
        if tags.get("amenity") == "hospital" or tags.get("healthcare") == "hospital":
            score += 80
        elif tags.get("healthcare") == "urgent_care":
            score += 60
        
        # Bed capacity
        try:
            beds = int(tags.get("beds", 0))
            score += min(beds // 10, 50)  # Cap at +50 for beds
        except (ValueError, TypeError):
            pass
        
        # Major facility name patterns (generic indicators)
        major_keywords = [
            "medical center", "memorial", "general", "regional", 
            "university", "children", "health system", "trauma"
        ]
        for keyword in major_keywords:
            if keyword in name:
                score += 20
                break
        
        return score
    
    @staticmethod
    def _canonicalize_domain(url: str) -> str:
        """
        Canonicalize URL to registrable domain (main site, not subdomain).
        Example: https://doctors.mountsinai.org/location/... => https://mountsinai.org
        """
        return normalize_domain(url)
    
    @staticmethod
    def _clean_rental_url(url: str) -> str:
        """
        Clean rental car URL to main domain.
        Examples:
            https://www.alamo.com/en_US/car-rental/... => https://www.alamo.com
            https://www.carrental-miamiairport.com/ => https://www.carrental-miamiairport.com
            http://avis.com/path => https://avis.com
        """
        if not url or not url.strip():
            return ""

        canonical = normalize_domain(url)
        return canonical or url

    @staticmethod
    def _is_medical_facility(facility_type: str, tags: Dict) -> bool:
        if facility_type in {"hospital", "clinic"}:
            return True
        if not isinstance(tags, dict):
            return False
        healthcare = (tags.get("healthcare") or "").lower()
        amenity = (tags.get("amenity") or "").lower()
        return healthcare in {"hospital", "clinic", "urgent_care"} or amenity in {"hospital", "clinic"}

    def _normalize_facility_website(self, url: str, facility_type: str, tags: Dict) -> str:
        if facility_type == "car rental":
            return self._clean_rental_url(url)
        if self._is_medical_facility(facility_type, tags):
            return self._normalize_website_url(url)
        return self._canonicalize_domain(url)

    def _resolve_structured_website(self, tags: Dict, extratags: Optional[Dict], facility_type: str) -> str:
        raw = self._extract_website(tags or {}, extratags)
        if not raw:
            return ""
        normalized = self._normalize_facility_website(raw, facility_type, tags or {})
        if not normalized or self._is_aggregator(normalized):
            return ""
        return normalized
    
    @staticmethod
    def _is_aggregator(url: str) -> bool:
        """Check if URL is an aggregator/directory site (not official hospital website)."""
        return is_aggregator_domain(url)
    
    def _rank_website_candidates(self, candidates: List[Dict], facility_name: str) -> List[Dict]:
        """
        Rank website candidates using deterministic heuristics.
        Prefer official root domains over directory pages.
        
        Args:
            candidates: List of {url, title, description} dicts
            facility_name: Hospital name for matching
            
        Returns:
            Sorted candidates (best first)
        """
        scored_candidates = []
        facility_normalized = facility_name.lower().strip()
        
        for candidate in candidates:
            score = 0
            url = candidate["url"].lower()
            title = candidate.get("title", "").lower()
            description = candidate.get("description", "").lower()
            
            # Exact facility name in title (strong signal)
            if facility_normalized in title:
                score += 10
            
            # Facility name in description
            if facility_normalized in description:
                score += 5
            
            # Prefer .org domains (hospitals often use .org)
            if ".org" in url:
                score += 5
            
            # Prefer health-related TLDs
            if any(tld in url for tld in [".health", ".med", ".hospital"]):
                score += 8
            
            # Prefer root or near-root paths
            path = url.split("//")[-1].split("/", 1)[-1] if "//" in url else ""
            path_depth = len([p for p in path.split("/") if p])
            
            if path_depth == 0:
                score += 3  # Root domain
            elif path_depth == 1:
                score += 1  # One level deep
            
            # Penalize directory-style paths (doctor finders, location pages)
            bad_patterns = ["/doctors/", "/locations/", "/find-a-doctor/", "/physicians/", 
                          "/providers/", "/directory/", "/search/", "/finder/"]
            if any(pattern in url for pattern in bad_patterns):
                score -= 5
            
            scored_candidates.append((score, candidate))
        
        # Sort by score descending
        scored_candidates.sort(key=lambda x: x[0], reverse=True)
        return [c for _, c in scored_candidates]
    
    def _enrich_facility_websites(
        self,
        facilities: List[Dict],
        city_name: str,
        facility_type: str = "hospital",
        *,
        country_code: Optional[str] = None,
        require_chain_for_ddg: bool = False,
    ) -> None:
        """Enrich facility websites using Nominatim OSM details first, then DuckDuckGo as fallback.
        
        Workflow:
        1. Try Nominatim details API with extratags (authoritative OSM data)
        2. If no website, fallback to DuckDuckGo search with ranking heuristics
        3. Normalize URLs and filter aggregators
        
        Args:
            facilities: List of facility dicts with 'name', 'tags', 'osm_id', 'osm_type' keys
            city_name: City name for search queries
            facility_type: Type of facility (e.g., "hospital", "car rental")
        """
        for facility in facilities:
            website_container = facility.get("tags") if isinstance(facility.get("tags"), dict) else facility
            tags = facility.get("tags") if isinstance(facility.get("tags"), dict) else {}
            website = ""
            extratags = None
            is_medical = self._is_medical_facility(facility_type, tags)
            is_store = facility_type in {"supermarket", "convenience", "pharmacy"}

            # Structured fields first (tags)
            website = self._resolve_structured_website(tags, None, facility_type)
            if not website:
                existing = website_container.get("website", "")
                if existing:
                    website = self._normalize_facility_website(existing, facility_type, tags)
                    if self._is_aggregator(website):
                        website = ""

            # Try Nominatim lookup (authoritative OSM data) when missing or generic
            osm_id = facility.get("osm_id")
            osm_type = facility.get("osm_type")
            if (not website or (is_medical and self._is_generic_homepage(website))) and osm_id and osm_type:
                details = self._lookup_osm_details(osm_id, osm_type)
                if details:
                    extratags = details.get("extratags", {})
                    candidate = self._resolve_structured_website(tags, extratags, facility_type)
                    if candidate:
                        website = candidate

            # Wikidata resolver for hospitals/clinics when missing or generic
            if is_medical and (not website or self._is_generic_homepage(website)):
                wikidata_id = ""
                if isinstance(tags, dict):
                    wikidata_id = tags.get("wikidata", "") or tags.get("wikidata:identifier", "") or ""
                if not wikidata_id and extratags:
                    wikidata_id = extratags.get("wikidata", "") or extratags.get("wikidata:identifier", "") or ""
                resolved = self._resolve_wikidata_website(
                    facility.get("name", ""),
                    facility.get("lat"),
                    facility.get("lon"),
                    wikidata_id=wikidata_id,
                )
                if resolved:
                    website = resolved

            if website and self._is_aggregator(website):
                website = ""

            if is_store:
                canonical = canonicalize_store_website(
                    facility.get("name", ""),
                    country_code,
                    website,
                    category=facility_type,
                )
                if canonical:
                    website = canonical

            if website and (not is_medical or not self._is_generic_homepage(website)):
                website_container["website"] = website
                continue

            if require_chain_for_ddg and is_store:
                if not is_store_chain_name(facility.get("name", ""), country_code, category=facility_type):
                    website_container["website"] = website or None
                    continue
            
            # Fallback to DDG search with exponential backoff
            for attempt in range(3):
                try:
                    query = f"{facility['name']} {city_name} official website"
                    results = self._ddg_search_with_timeout(query, max_results=5, timeout_seconds=10)
                    
                    if not results:
                        break
                        
                        # Filter out aggregators and pick best candidate
                        candidates = []
                        for r in results:
                            url = r.get("href", "")
                            if url and not self._is_aggregator(url):
                                candidates.append({
                                    "url": url,
                                    "title": r.get("title", ""),
                                    "description": r.get("body", "")
                                })
                        
                        if not candidates:
                            break
                        
                        # Rank candidates using deterministic heuristics
                        ranked_candidates = self._rank_website_candidates(candidates, facility['name'])
                        
                        # Use AI only if top candidates are tied or uncertain
                        if self.openai_client and len(ranked_candidates) > 1:
                            # Check if top 2 candidates have similar scores
                            # For now, just use AI to validate the top candidate
                            prompt = f"""Given these search results for "{facility['name']}", identify the official {facility_type} website URL.

Search results (ranked by relevance):
{json.dumps(ranked_candidates[:3], indent=2)}

Return JSON: {{"website": "https://..."}} with the OFFICIAL {facility_type} site, or {{"website": null}} if uncertain."""
                            
                            response = self.openai_client.chat.completions.create(
                                model=getattr(config, "OPENAI_MODEL", "gpt-5-mini"),
                                messages=[{"role": "user", "content": prompt}],
                                response_format={"type": "json_object"}
                            )
                            
                            result = json.loads(response.choices[0].message.content)
                            website = result.get("website")
                        else:
                            # Just take first ranked candidate
                            website = ranked_candidates[0]["url"] if ranked_candidates else None
                        
                        if website:
                            normalized = self._normalize_facility_website(website, facility_type, tags)
                            if normalized and not self._is_aggregator(normalized):
                                website_container["website"] = normalized
                                website = normalized
                    
                    time.sleep(1.5)  # Rate limit
                    break
                    
                except Exception as e:
                    if "202" in str(e) or "Ratelimit" in str(e).lower():
                        delay = (attempt + 1) * 2
                        if attempt < 2:
                            print(f"    WARN Rate limit, waiting {delay}s...")
                            time.sleep(delay)
                    else:
                        break

            if not website_container.get("website"):
                website_container["website"] = website or None

    def _is_hospital_or_urgent_care_result(self, result: Dict) -> bool:
        """Validate Nominatim results for hospital/urgent care classification."""
        result_class = (result.get("class") or "").lower()
        result_type = (result.get("type") or "").lower()
        if result_class in {"amenity", "healthcare"} and result_type in {"hospital", "urgent_care"}:
            return True
        if result_class and result_class not in {"amenity", "healthcare"}:
            return False

        osm_id = result.get("osm_id")
        osm_type = result.get("osm_type")
        if not osm_id or not osm_type:
            return False

        details = self._lookup_osm_details(osm_id, osm_type)
        if not details:
            return False

        details_class = (details.get("category") or details.get("class") or "").lower()
        details_type = (details.get("type") or "").lower()
        if details_class in {"amenity", "healthcare"} and details_type in {"hospital", "urgent_care"}:
            return True

        extratags = details.get("extratags") if isinstance(details.get("extratags"), dict) else {}
        amenity = (extratags.get("amenity") or "").lower()
        healthcare = (extratags.get("healthcare") or "").lower()
        if amenity == "hospital":
            return True
        if healthcare in {"hospital", "urgent_care"}:
            return True
        return False
    
    def _ddg_coverage_check(self, facilities: List[Dict], city_name: str, bbox: List[float]) -> List[Dict]:
        """
        Coverage safety net: search for top hospitals via DDG and add any missing facilities
        that can be verified via Nominatim (anti-hallucination).
        """
        try:
            query = f"top hospitals in {city_name}"
            results = self._ddg_search_with_timeout(query, max_results=10, timeout_seconds=15)
            
            if not results:
                return facilities
            
            # Extract hospital names from results
            existing_normalized = {self._normalize_facility_name(f["name"]) for f in facilities}
            
            # Try to find mentions of hospitals in search results
            mentions = set()
            for r in results:
                text = f"{r.get('title', '')} {r.get('body', '')}"
                # Look for hospital/medical center names
                hospital_patterns = [
                    r'([A-Z][a-z]+(INFO:\s+[A-Z][a-z]+)*\s+(INFO:Hospital|Medical Center|Health System))'
                ]
                for pattern in hospital_patterns:
                    matches = re.findall(pattern, text)
                    mentions.update(matches)
            
            # For each mentioned hospital not in our list, try to geocode it
            added = 0
            for mention in mentions:
                if added >= 5:  # Limit additions to avoid bloat
                    break
                    
                cleaned_name = self._clean_facility_name(mention)
                if not cleaned_name:
                    continue
                normalized = self._normalize_facility_name(cleaned_name)
                if normalized in existing_normalized:
                    continue
                
                # Try to geocode via Nominatim to verify it exists
                try:
                    self._rate_limit_nominatim()
                    search_query = f"{mention}, {city_name}"
                    resp = self._session.get(
                        "https://nominatim.openstreetmap.org/search",
                        params={"q": search_query, "format": "json", "limit": 1},
                        timeout=10
                    )
                    resp.raise_for_status()
                    results = resp.json()
                    
                    if results and len(results) > 0:
                        result = results[0]
                        if not self._is_hospital_or_urgent_care_result(result):
                            continue
                        lat = float(result.get("lat", 0))
                        lon = float(result.get("lon", 0))
                        
                        # Verify it's within our bbox
                        if (bbox[0] <= lon <= bbox[2] and bbox[1] <= lat <= bbox[3]):
                            facilities.append({
                                "name": cleaned_name,
                                "lat": lat,
                                "lon": lon,
                                "address": result.get("display_name", ""),
                                "tags": {
                                    "emergency": "",
                                    "healthcare": "hospital",
                                    "phone": "",
                                    "website": "",
                                    "source": "coverage_check"
                                }
                            })
                            existing_normalized.add(normalized)
                            added += 1
                            print(f"    OK Added from coverage check: {cleaned_name}")
                            time.sleep(1.0)  # Rate limit Nominatim
                            
                except Exception:
                    continue
            
        except Exception as e:
            print(f"    WARN Coverage check failed: {type(e).__name__}")
        
        return facilities

    def _clear_duplicate_generic_websites(self, facilities: List[Dict]) -> None:
        """Remove duplicate generic homepage URLs across hospital facilities."""
        counts: Dict[str, int] = {}
        for facility in facilities:
            container = facility.get("tags") if isinstance(facility.get("tags"), dict) else facility
            url = container.get("website")
            normalized = self._normalize_website_url(url) if url else ""
            if normalized and self._is_generic_homepage(normalized):
                counts[normalized] = counts.get(normalized, 0) + 1

        duplicates = {url for url, count in counts.items() if count > 1}
        if not duplicates:
            return

        for facility in facilities:
            container = facility.get("tags") if isinstance(facility.get("tags"), dict) else facility
            url = container.get("website")
            normalized = self._normalize_website_url(url) if url else ""
            if normalized in duplicates and self._is_generic_homepage(normalized):
                container["website"] = None
    
    def find_hospitals(self, bbox: List[float], city_name: str = "", max_results: int = 15) -> List[Dict]:
        """
        Find hospitals and urgent care facilities using comprehensive search:
        1. Query OSM Overpass for hospitals + urgent care (collect up to 200)
        2. Score and rank facilities (major hospitals first)
        3. Deduplicate by normalized name + location
        4. Enrich with DuckDuckGo for official websites
        5. Coverage safety net via DDG top hospitals
        
        Args:
            bbox: [west, south, east, north]
            city_name: City name for DDG searches
            max_results: Maximum number of facilities to return
        
        Returns:
            List of top-ranked hospitals/urgent care with name, lat, lon, address, tags
        """
        if not bbox or len(bbox) != 4:
            return []
        
        # Try multiple Overpass instances
        overpass_instances = [
            "https://overpass-api.de/api/interpreter",
            "https://overpass.kumi.systems/api/interpreter",
            "https://overpass.private.coffee/api/interpreter",
        ]
        
        all_facilities = []
        
        for attempt in range(len(overpass_instances)):
            try:
                bbox_str = f"{bbox[1]},{bbox[0]},{bbox[3]},{bbox[2]}"
                
                # Comprehensive query: node/way/relation, multiple criteria
                query = f"""
                [out:json][timeout:25];
                (
                  // Hospitals via amenity tag
                  node["amenity"="hospital"]({bbox_str});
                  way["amenity"="hospital"]({bbox_str});
                  relation["amenity"="hospital"]({bbox_str});
                  
                  // Hospitals via healthcare tag
                  node["healthcare"="hospital"]({bbox_str});
                  way["healthcare"="hospital"]({bbox_str});
                  relation["healthcare"="hospital"]({bbox_str});
                  
                  // Urgent care facilities
                  node["healthcare"="urgent_care"]({bbox_str});
                  way["healthcare"="urgent_care"]({bbox_str});
                  relation["healthcare"="urgent_care"]({bbox_str});
                  
                  // Emergency clinics
                  node["amenity"="clinic"]["emergency"="yes"]({bbox_str});
                  way["amenity"="clinic"]["emergency"="yes"]({bbox_str});
                  
                  // Clinics with urgent care in name
                  node["amenity"="clinic"]["name"~"urgent care",i]({bbox_str});
                  way["amenity"="clinic"]["name"~"urgent care",i]({bbox_str});
                );
                out center 200;
                """
                
                url = overpass_instances[attempt % len(overpass_instances)]
                resp = self._session.post(url, data={"data": query}, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                
                # Non-essential services to exclude
                excluded_services = {
                    "acupuncture", "massage", "physiotherapy", "rehabilitation",
                    "alternative", "chiropractic", "osteopathy", "counselling",
                    "psychotherapist", "speech_therapist", "occupational_therapist"
                }
                
                for element in data.get("elements", []):
                    # Capture OSM ID and type
                    osm_id = element.get("id")
                    osm_type = element.get("type")  # "node", "way", or "relation"
                    
                    # Get coordinates
                    lat = element.get("lat") or (element.get("center") or {}).get("lat")
                    lon = element.get("lon") or (element.get("center") or {}).get("lon")
                    
                    if not lat or not lon:
                        continue
                    
                    tags = element.get("tags", {})
                    
                    # Skip non-essential health services
                    healthcare_type = tags.get("healthcare", "").lower()
                    if healthcare_type in excluded_services:
                        continue
                    
                    specialty = tags.get("healthcare:speciality", "").lower()
                    if any(excl in specialty for excl in excluded_services):
                        continue
                    
                    name = self._clean_facility_name(tags.get("name"))
                    if not name:  # Skip unnamed facilities
                        continue
                    
                    # Build address
                    address_parts = []
                    for key in ["addr:housenumber", "addr:street", "addr:city"]:
                        if key in tags:
                            address_parts.append(tags[key])
                    address = ", ".join(address_parts) if address_parts else ""
                    
                    raw_website = self._extract_website(tags)
                    website = self._normalize_website_url(raw_website) if raw_website else None
                    if website and self._is_aggregator(website):
                        website = None

                    facility = {
                        "name": name,
                        "lat": lat,
                        "lon": lon,
                        "address": address,
                        "osm_id": osm_id,
                        "osm_type": osm_type,
                        "tags": {
                            "emergency": tags.get("emergency", ""),
                            "healthcare": healthcare_type or tags.get("amenity", "hospital"),
                            "phone": tags.get("phone", ""),
                            "website": website,
                            "beds": tags.get("beds", ""),
                            "amenity": tags.get("amenity", ""),
                        }
                    }
                    
                    all_facilities.append(facility)
                
                break  # Success
                
            except Exception as e:
                if attempt == 0:
                    print(f"  WARN Hospital query attempt {attempt + 1} failed: {type(e).__name__}, trying alternate server...")
                    time.sleep(2)
                    continue
                else:
                    print(f"  WARN Hospital query failed after {attempt + 1} attempts: {e}")
                    return []
        
        if not all_facilities:
            return []
        
        # Deduplicate by normalized name + rounded location
        seen = set()
        deduped = []
        for fac in all_facilities:
            # Create dedup key: normalized name + location rounded to ~100m
            key = (
                self._normalize_facility_name(fac["name"]),
                round(fac["lat"], 3),
                round(fac["lon"], 3)
            )
            if key not in seen:
                seen.add(key)
                deduped.append(fac)
        
        # Score and sort (deterministic: same input => same output)
        for fac in deduped:
            fac["_score"] = self._score_facility(fac)
        
        deduped.sort(key=lambda f: (-f["_score"], f["name"]))  # High score first, then alphabetical
        
        # Remove score field from output
        for fac in deduped:
            fac.pop("_score", None)
        
        # Take top N
        top_facilities = deduped[:max_results]

        if top_facilities and not any(f.get("tags", {}).get("website") for f in top_facilities):
            fallback_with_site = next(
                (f for f in deduped if f.get("tags", {}).get("website")),
                None,
            )
            if fallback_with_site and all(f.get("name") != fallback_with_site.get("name") for f in top_facilities):
                top_facilities[-1] = fallback_with_site
        
        print(f"  OK Found {len(top_facilities)} hospitals (from {len(all_facilities)} raw results)")
        
        # Enrich addresses via reverse geocoding for facilities with empty addresses
        print(f"  INFO Enriching addresses...")
        for facility in top_facilities:
            if not facility.get("address"):
                lat = facility.get("lat")
                lon = facility.get("lon")
                if lat and lon:
                    facility["address"] = self._reverse_geocode(lat, lon)
        
        # Enrich with websites (DDG-based, OpenAI optional)
        if city_name:
            print(f"  INFO Enriching websites...")
            self._enrich_facility_websites(top_facilities, city_name)
        
        # Coverage safety net (add missing major hospitals)
        if city_name:
            print(f"  INFO Running coverage check...")
            top_facilities = self._ddg_coverage_check(top_facilities, city_name, bbox)

        self._clear_duplicate_generic_websites(top_facilities)
        
        return top_facilities[:max_results]  # Ensure we don't exceed max after additions
    
    def find_pharmacies(
        self,
        bbox: List[float],
        city_name: str = "",
        max_results: int = 10,
        country_code: Optional[str] = None,
    ) -> List[Dict]:
        """
        Find pharmacies and drugstores using OpenStreetMap Overpass API.
        
        Args:
            bbox: [west, south, east, north]
            city_name: City name for website enrichment
            max_results: Maximum number of pharmacies to return
        
        Returns:
            List of pharmacies with name, lat, lon, address
        """
        if not bbox or len(bbox) != 4:
            return []
        
        overpass_instances = [
            "https://overpass-api.de/api/interpreter",
            "https://overpass.kumi.systems/api/interpreter",
            "https://overpass.private.coffee/api/interpreter",
        ]
        
        all_pharmacies = []
        
        for attempt in range(len(overpass_instances)):
            try:
                bbox_str = f"{bbox[1]},{bbox[0]},{bbox[3]},{bbox[2]}"
                
                query = f"""
                [out:json][timeout:15];
                (
                  node["amenity"="pharmacy"]({bbox_str});
                  way["amenity"="pharmacy"]({bbox_str});
                  relation["amenity"="pharmacy"]({bbox_str});
                );
                out center 100;
                """
                
                url = overpass_instances[attempt % len(overpass_instances)]
                resp = self._session.post(url, data={"data": query}, timeout=20)
                resp.raise_for_status()
                data = resp.json()
                
                for element in data.get("elements", []):
                    osm_id = element.get("id")
                    osm_type = element.get("type")
                    
                    lat = element.get("lat") or (element.get("center") or {}).get("lat")
                    lon = element.get("lon") or (element.get("center") or {}).get("lon")
                    
                    if not lat or not lon:
                        continue
                    
                    tags = element.get("tags", {})
                    name = (tags.get("name") or "").strip()

                    if not name:
                        continue

                    if name.upper() == "CREATE":
                        alt = (tags.get("brand") or tags.get("operator") or "").strip()
                        if alt and alt.upper() != "CREATE":
                            name = alt
                        else:
                            extracted_site = self._extract_website(tags)
                            if not extracted_site:
                                continue
                    
                    # Build address
                    address_parts = []
                    for key in ["addr:housenumber", "addr:street", "addr:city"]:
                        if key in tags:
                            address_parts.append(tags[key])
                    address = ", ".join(address_parts) if address_parts else ""
                    
                    pharmacy = {
                        "name": name,
                        "lat": lat,
                        "lon": lon,
                        "address": address,
                        "osm_id": osm_id,
                        "osm_type": osm_type,
                        "website": self._canonicalize_domain(self._extract_website(tags)),
                        "phone": tags.get("phone", ""),
                        "opening_hours": tags.get("opening_hours", ""),
                    }
                    
                    all_pharmacies.append(pharmacy)
                
                break
                
            except Exception as e:
                if attempt == 0:
                    print(f"  WARN Pharmacy query attempt {attempt + 1} failed, trying alternate server...")
                    time.sleep(2)
                    continue
                else:
                    print(f"  WARN Pharmacy query failed: {e}")
                    return []
        
        # Deduplicate
        seen = set()
        deduped = []
        for pharm in all_pharmacies:
            key = (
                self._normalize_facility_name(pharm["name"]),
                round(pharm["lat"], 3),
                round(pharm["lon"], 3)
            )
            if key not in seen:
                seen.add(key)
                deduped.append(pharm)
        
        # Sort by name for consistency
        deduped.sort(key=lambda p: p["name"])

        # Enrich with websites (DDG-based, OpenAI optional)
        if city_name and deduped:
            print(f"  INFO Enriching pharmacy websites...")
            self._enrich_facility_websites(
                deduped[:max_results],
                city_name,
                facility_type="pharmacy",
                country_code=country_code,
                require_chain_for_ddg=True,
            )
        
        return deduped[:max_results]
    
    def find_rental_cars(self, bbox: List[float], city_name: str = "", max_results: int = 10) -> List[Dict]:
        """
        Find car rental companies using OpenStreetMap Overpass API.
        
        Args:
            bbox: [west, south, east, north]
            city_name: City name for website enrichment
            max_results: Maximum number of rental companies to return
        
        Returns:
            List of rental car companies with name, lat, lon, address, website
        """
        if not bbox or len(bbox) != 4:
            return []
        
        overpass_instances = [
            "https://overpass-api.de/api/interpreter",
            "https://overpass.kumi.systems/api/interpreter",
            "https://overpass.private.coffee/api/interpreter",
        ]
        
        all_rentals = []
        
        for attempt in range(len(overpass_instances)):
            try:
                bbox_str = f"{bbox[1]},{bbox[0]},{bbox[3]},{bbox[2]}"
                
                query = f"""
                [out:json][timeout:15];
                (
                  node["amenity"="car_rental"]({bbox_str});
                  way["amenity"="car_rental"]({bbox_str});
                  relation["amenity"="car_rental"]({bbox_str});
                  node["shop"="car_rental"]({bbox_str});
                  way["shop"="car_rental"]({bbox_str});
                );
                out center 100;
                """
                
                url = overpass_instances[attempt % len(overpass_instances)]
                resp = self._session.post(url, data={"data": query}, timeout=20)
                resp.raise_for_status()
                data = resp.json()
                
                for element in data.get("elements", []):
                    osm_id = element.get("id")
                    osm_type = element.get("type")
                    
                    lat = element.get("lat") or (element.get("center") or {}).get("lat")
                    lon = element.get("lon") or (element.get("center") or {}).get("lon")
                    
                    if not lat or not lon:
                        continue
                    
                    tags = element.get("tags", {})
                    name = tags.get("name") or tags.get("brand") or tags.get("operator")
                    
                    if not name:
                        continue
                    
                    # Build address
                    address_parts = []
                    for key in ["addr:housenumber", "addr:street", "addr:city"]:
                        if key in tags:
                            address_parts.append(tags[key])
                    address = ", ".join(address_parts) if address_parts else ""
                    
                    rental = {
                        "name": name,
                        "lat": lat,
                        "lon": lon,
                        "address": address,
                        "osm_id": osm_id,
                        "osm_type": osm_type,
                        "website": self._clean_rental_url(self._extract_website(tags)),
                        "phone": tags.get("phone", ""),
                        "opening_hours": tags.get("opening_hours", ""),
                    }
                    
                    all_rentals.append(rental)
                
                break
                
            except Exception as e:
                if attempt == 0:
                    print(f"  WARN Rental car query attempt {attempt + 1} failed, trying alternate server...")
                    time.sleep(2)
                    continue
                else:
                    print(f"  WARN Rental car query failed: {e}")
                    return []
        
        # Deduplicate by name and location
        seen = set()
        deduped = []
        
        # Major rental car brands to prioritize
        major_brands = {
            "alamo", "avis", "budget", "enterprise", "hertz", "national",
            "sixt", "thrifty", "dollar", "europcar", "payless", "fox",
            "advantage", "ace", "e-z"
        }
        
        for rental in all_rentals:
            # Filter out non-rental companies (RV rentals, brokers, etc.)
            name_lower = rental["name"].lower()
            if "rv" in name_lower or "motorhome" in name_lower or "camper" in name_lower:
                continue  # Skip RV rentals
            if "flex" in name_lower and "rental" not in name_lower:
                continue  # Skip generic "flex" companies that aren't clearly rentals
            
            key = (
                self._normalize_facility_name(rental["name"]),
                round(rental["lat"], 3),
                round(rental["lon"], 3)
            )
            if key not in seen:
                seen.add(key)
                # Add priority score for major brands
                rental["_is_major"] = any(brand in name_lower for brand in major_brands)
                deduped.append(rental)
        
        # Sort by priority: major brands first, then alphabetically
        deduped.sort(key=lambda r: (not r.get("_is_major", False), r["name"]))
        
        # Remove priority flag before returning
        for r in deduped:
            r.pop("_is_major", None)
        
        # Rental car websites are resolved via allowlisted providers; skip enrichment.
        
        return deduped[:max_results]
    
    def find_stores(self, bbox: List[float], store_type: str = "supermarket", max_results: int = 10) -> List[Dict]:
        """
        Find stores (supermarkets or convenience stores) using OpenStreetMap.
        
        Args:
            bbox: [west, south, east, north]
            store_type: "supermarket" or "convenience"
            max_results: Maximum number of stores to return
        
        Returns:
            List of stores with name, lat, lon, address
        """
        if not bbox or len(bbox) != 4:
            return []
        
        overpass_instances = [
            "https://overpass-api.de/api/interpreter",
            "https://overpass.kumi.systems/api/interpreter",
            "https://overpass.private.coffee/api/interpreter",
        ]
        
        all_stores = []
        
        for attempt in range(len(overpass_instances)):
            try:
                bbox_str = f"{bbox[1]},{bbox[0]},{bbox[3]},{bbox[2]}"
                
                query = f"""
                [out:json][timeout:15];
                (
                  node["shop"="{store_type}"]({bbox_str});
                  way["shop"="{store_type}"]({bbox_str});
                  relation["shop"="{store_type}"]({bbox_str});
                );
                out center 100;
                """
                
                url = overpass_instances[attempt % len(overpass_instances)]
                resp = self._session.post(url, data={"data": query}, timeout=20)
                resp.raise_for_status()
                data = resp.json()
                
                for element in data.get("elements", []):
                    osm_id = element.get("id")
                    osm_type = element.get("type")
                    
                    lat = element.get("lat") or (element.get("center") or {}).get("lat")
                    lon = element.get("lon") or (element.get("center") or {}).get("lon")
                    
                    if not lat or not lon:
                        continue
                    
                    tags = element.get("tags", {})
                    name = tags.get("name")
                    
                    if not name:
                        continue
                    
                    # Build address
                    address_parts = []
                    for key in ["addr:housenumber", "addr:street", "addr:city"]:
                        if key in tags:
                            address_parts.append(tags[key])
                    address = ", ".join(address_parts) if address_parts else ""
                    
                    store = {
                        "name": name,
                        "lat": lat,
                        "lon": lon,
                        "address": address,
                        "osm_id": osm_id,
                        "osm_type": osm_type,
                        "website": self._canonicalize_domain(self._extract_website(tags)),
                        "phone": tags.get("phone", ""),
                        "opening_hours": tags.get("opening_hours", ""),
                    }
                    
                    all_stores.append(store)
                
                break
                
            except Exception as e:
                if attempt == 0:
                    print(f"  WARN Store query attempt {attempt + 1} failed, trying alternate server...")
                    time.sleep(2)
                    continue
                else:
                    print(f"  WARN Store query failed: {e}")
                    return []
        
        # Deduplicate
        seen = set()
        deduped = []
        for store in all_stores:
            key = (
                self._normalize_facility_name(store["name"]),
                round(store["lat"], 3),
                round(store["lon"], 3)
            )
            if key not in seen:
                seen.add(key)
                deduped.append(store)
        
        # Sort by name for consistency
        deduped.sort(key=lambda s: s["name"])
        
        return deduped[:max_results]
    
    def get_city_context(self, location: str, force_refresh: bool = False, city_key: Optional[str] = None) -> Dict:
        """
        Get complete city context: geocoding + hospitals.
        Uses monthly cache to avoid excessive API calls.
        
        This is the main method to call. Returns all data needed for the report.
        
        Args:
            location: City/location to fetch context for
            force_refresh: Force refresh even if cache is fresh
        
        Returns:
            {
                "resolved_location": {lat, lon, bbox, country_code, display_name},
                "hospitals": [{name, lat, lon, address, tags}]
            }
        
        Note: Transit information is now fetched separately via DuckDuckGo search.
        """
        print(f"Fetching city context for: {location}")
        
        effective_city_key = (city_key or "").strip() or self._location_to_city_key(location)

        # Check cache first (unless force refresh)
        if not force_refresh:
            cached = self._load_city_cache(location, effective_city_key)
            if cached:
                print(f"  OK Using cached city context (less than {self.cache_refresh_days} days old)")
                # Still show the info
                if cached.get("resolved_location"):
                    resolved = cached["resolved_location"]
                    print(f"  OK Geocoded: {resolved.get('display_name', location)}")
                    print(f"    Coordinates: ({resolved.get('lat', 0):.4f}, {resolved.get('lon', 0):.4f})")
                print(f"  OK Found {len(cached.get('hospitals', []))} hospitals")
                print(f"  OK Found {len(cached.get('pharmacies', []))} pharmacies")
                print(f"  OK Found {len(cached.get('rental_cars', []))} car rental companies")
                print(f"  OK Found {len(cached.get('supermarkets', []))} supermarkets")
                print(f"  OK Found {len(cached.get('convenience_stores', []))} convenience stores")
                return cached
        
        # Step 1: Geocode
        city_key = effective_city_key

        # Prague-specific hardening: avoid flaky geocoding by using configured coordinates.
        # This still produces verified infrastructure via OSM Overpass; we only skip the
        # Nominatim *geocode* call for this location.
        resolved = None
        try:
            import config

            city_cfg = getattr(config, "CITIES", {}).get(city_key) or {}
            is_prague = (
                (city_cfg.get("country_code") == "CZ")
                and (str(city_cfg.get("name") or "").lower().startswith("prague"))
                and city_cfg.get("latitude")
                and city_cfg.get("longitude")
            )
            if is_prague:
                lat = float(city_cfg["latitude"])
                lon = float(city_cfg["longitude"])
                delta = 0.15  # ~15-20km; sufficient for city-wide OSM queries
                resolved = {
                    "lat": lat,
                    "lon": lon,
                    "bbox": [lon - delta, lat - delta, lon + delta, lat + delta],
                    "country_code": "CZ",
                    "display_name": city_cfg.get("name", location),
                }
        except Exception:
            resolved = None

        if not resolved:
            resolved = self.geocode_location(location)

        if not resolved:
            print(f"  WARN Geocoding failed")
            try:
                import config

                airports = list((config.CITIES.get(city_key, {}) or {}).get("airports") or [])
            except Exception:
                airports = []
            context_data = {
                "resolved_location": None,
                "hospitals": [],
                "pharmacies": [],
                "rental_cars": [],
                "supermarkets": [],
                "convenience_stores": [],
                "airports": airports,
                "emergency_contacts": self._build_emergency_contacts(city_key, resolved_location=None),
                "coverage_status": "Partial",
                "missing_categories": ["hospitals", "pharmacies", "supermarkets", "convenience_stores"],
                "coverage_note": "Infrastructure data not available yet (geocoding failed).",
            }

            # Still persist an empty snapshot so downstream runs/tests can verify
            # the pipeline reached the datastore.
            self._save_city_cache(location, context_data, city_key)
            return context_data
        
        print(f"  OK Geocoded: {resolved['display_name']}")
        print(f"    Coordinates: ({resolved['lat']:.4f}, {resolved['lon']:.4f})")
        self._clamp_bbox(resolved)
        
        # Extract city name for DDG searches (first part of display_name)
        city_name = resolved['display_name'].split(',')[0].strip()
        
        # Load existing cache to preserve good data if new fetches fail
        cached_data = self._load_city_cache(location, city_key) if force_refresh else None
        
        # Step 2: Find hospitals
        hospitals = self.find_hospitals(resolved["bbox"], city_name=city_name, max_results=10)
        if len(hospitals) == 0 and cached_data and len(cached_data.get('hospitals', [])) > 0:
            hospitals = cached_data['hospitals']
            print(f"  OK Complete: {len(hospitals)} facilities (from cache - API failed)")
        else:
            print(f"  OK Complete: {len(hospitals)} facilities")
        
        # Step 3: Find pharmacies
        pharmacies = self.find_pharmacies(
            resolved["bbox"],
            city_name=city_name,
            max_results=10,
            country_code=resolved.get("country_code"),
        )
        if len(pharmacies) == 0 and cached_data and len(cached_data.get('pharmacies', [])) > 0:
            pharmacies = cached_data['pharmacies']
            print(f"  OK Found {len(pharmacies)} pharmacies (from cache - API failed)")
        else:
            print(f"  OK Found {len(pharmacies)} pharmacies")
        
        # Step 4: Find rental cars
        rental_cars = self.find_rental_cars(resolved["bbox"], city_name=city_name, max_results=10)
        if len(rental_cars) == 0 and cached_data and len(cached_data.get('rental_cars', [])) > 0:
            rental_cars = cached_data['rental_cars']
            print(f"  OK Found {len(rental_cars)} car rental companies (from cache - API failed)")
        else:
            print(f"  OK Found {len(rental_cars)} car rental companies")
        
        # Step 5: Find supermarkets
        supermarkets = self.find_stores(resolved["bbox"], store_type="supermarket", max_results=10)
        if len(supermarkets) == 0 and cached_data and len(cached_data.get('supermarkets', [])) > 0:
            supermarkets = cached_data['supermarkets']
            print(f"  OK Found {len(supermarkets)} supermarkets (from cache - API failed)")
        else:
            print(f"  OK Found {len(supermarkets)} supermarkets")
        
        # Step 6: Find convenience stores
        convenience_stores = self.find_stores(resolved["bbox"], store_type="convenience", max_results=10)
        if len(convenience_stores) == 0 and cached_data and len(cached_data.get('convenience_stores', [])) > 0:
            convenience_stores = cached_data['convenience_stores']
            print(f"  OK Found {len(convenience_stores)} convenience stores (from cache - API failed)")
        else:
            print(f"  OK Found {len(convenience_stores)} convenience stores")

        if supermarkets:
            self._enrich_facility_websites(
                supermarkets,
                city_name,
                facility_type="supermarket",
                country_code=resolved.get("country_code"),
                require_chain_for_ddg=True,
            )
        if convenience_stores:
            self._enrich_facility_websites(
                convenience_stores,
                city_name,
                facility_type="convenience",
                country_code=resolved.get("country_code"),
                require_chain_for_ddg=True,
            )
        
        emergency_contacts = self._build_emergency_contacts(city_key, resolved)

        missing_categories: List[str] = []
        if not hospitals:
            missing_categories.append("hospitals")
        if not pharmacies:
            missing_categories.append("pharmacies")
        if not supermarkets:
            missing_categories.append("supermarkets")
        if not convenience_stores:
            missing_categories.append("convenience_stores")

        coverage_status = "Full" if not missing_categories else "Partial"
        coverage_note = None
        if missing_categories:
            coverage_note = "Infrastructure data missing: " + ", ".join(missing_categories)

        try:
            import config

            airports = list((config.CITIES.get(city_key, {}) or {}).get("airports") or [])
        except Exception:
            airports = []

        # Build context data
        context_data = {
            "resolved_location": resolved,
            "hospitals": hospitals,
            "pharmacies": pharmacies,
            "rental_cars": rental_cars,
            "supermarkets": supermarkets,
            "convenience_stores": convenience_stores,
            "airports": airports,
            "emergency_contacts": emergency_contacts,
            "coverage_status": coverage_status,
            "missing_categories": missing_categories,
            "coverage_note": coverage_note,
        }

        if effective_city_key.lower() == "madrid":
            print("  DEBUG Madrid hospitals (pre-save):")
            for facility in hospitals:
                container = facility.get("tags") if isinstance(facility.get("tags"), dict) else facility
                name = facility.get("name", "")
                website = container.get("website")
                print(f"    - {name}: {website}")
        
        # Save to cache
        self._save_city_cache(location, context_data, city_key)
        print(f"  OK Cached city context for {self.cache_refresh_days} days")
        
        return context_data
