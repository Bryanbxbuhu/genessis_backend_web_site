"""
Supabase storage implementation - Schema v3.

Schema v3 changes from v2:
- Added city_reports table: stores latest AI-generated report per city
- Added curated_places table: manually curated must-include locations
- Added updated_at triggers on all tables
- Updated reset functions: reset_osint_runtime_data() and reset_osint_all()

Uses Supabase Postgres backend for all data storage.
"""

import os
import re
import unicodedata
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from typing import Dict, List, Optional, Any, Tuple

try:
    from supabase import create_client, Client
except ImportError:
    create_client = None
    Client = None

from .base import DataStore, FeedItem, CityContext, TransitSnapshot
from .normalize import normalize_domain

# Import config for country_code lookups
try:
    import config
except ImportError:
    config = None

try:
    from helpers.rental_car_links import detect_rental_car_provider
except ImportError:
    detect_rental_car_provider = None


class SupabaseStore(DataStore):
    """Supabase-based storage using Postgres (Schema v3)."""
    
    def __init__(self):
        """
        Initialize Supabase client.
        
        Requires environment variables:
            SUPABASE_URL: Your Supabase project URL
            SUPABASE_SERVICE_ROLE_KEY: Service role key (for server-side operations)
        """
        if create_client is None:
            raise RuntimeError(
                "Supabase client is not installed. Install with: pip install supabase"
            )
        
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        
        if not supabase_url or not supabase_key:
            raise RuntimeError(
                "Missing Supabase credentials. Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY environment variables."
            )
        
        self.client: Client = create_client(supabase_url, supabase_key)
        self._feed_item_optional_columns: Optional[bool] = None
        self._curated_place_optional_columns: Optional[bool] = None
    
    # ============================================================================
    # Schema v3: Reset and Seed
    # ============================================================================
    
    def reset_runtime_data(self) -> bool:
        """
        Truncate runtime tables while preserving city_reports and config.
        
        Returns:
            True if successful
        """
        try:
            print("   Calling runtime reset (preserving city_reports)...")
            sql = (
                "TRUNCATE TABLE "
                "public.feed_item_cities, "
                "public.feed_items, "
                "public.weather_forecasts, "
                "public.city_context_snapshots, "
                "public.transit_snapshots "
                "CASCADE;"
            )
            try:
                self.client.postgrest.rpc("exec_sql", {"sql": sql}).execute()
                print("   OK Runtime tables truncated (reports preserved)")
                return True
            except Exception as e:
                print(f"   WARN exec_sql failed, falling back to deletes: {e}")

            self.client.table("feed_item_cities").delete().neq("city_key", "").execute()
            self.client.table("feed_items").delete().neq(
                "id",
                "00000000-0000-0000-0000-000000000000",
            ).execute()
            self.client.table("weather_forecasts").delete().neq(
                "id",
                "00000000-0000-0000-0000-000000000000",
            ).execute()
            self.client.table("city_context_snapshots").delete().neq("city_key", "").execute()
            self.client.table("transit_snapshots").delete().neq("city_key", "").execute()
            print("   OK Runtime tables cleared (reports preserved)")
            return True
        except Exception as e:
            print(f"   ERROR Failed to reset runtime data: {e}")
            return False

    def reset_all_data(self) -> bool:
        """
        Truncate most tables while preserving city_reports and cities.
        WARNING: This deletes sources and curated_places config.
        
        Returns:
            True if successful
        """
        try:
            print("   Calling reset_all (preserving city_reports)...")
            sql = (
                "TRUNCATE TABLE "
                "public.feed_item_cities, "
                "public.feed_items, "
                "public.weather_forecasts, "
                "public.city_context_snapshots, "
                "public.transit_snapshots, "
                "public.curated_places, "
                "public.sources "
                "CASCADE;"
            )
            try:
                self.client.postgrest.rpc("exec_sql", {"sql": sql}).execute()
                print("   OK Tables truncated (reports preserved)")
                return True
            except Exception as e:
                print(f"   WARN exec_sql failed, falling back to deletes: {e}")

            self.client.table("feed_item_cities").delete().neq("city_key", "").execute()
            self.client.table("feed_items").delete().neq(
                "id",
                "00000000-0000-0000-0000-000000000000",
            ).execute()
            self.client.table("weather_forecasts").delete().neq(
                "id",
                "00000000-0000-0000-0000-000000000000",
            ).execute()
            self.client.table("city_context_snapshots").delete().neq("city_key", "").execute()
            self.client.table("transit_snapshots").delete().neq("city_key", "").execute()
            self.client.table("curated_places").delete().neq(
                "id",
                "00000000-0000-0000-0000-000000000000",
            ).execute()
            self.client.table("sources").delete().neq("source_key", "").execute()
            print("   OK Tables cleared (reports preserved)")
            return True
        except Exception as e:
            print(f"   ERROR Failed to reset all data: {e}")
            return False

    def reset_data(self) -> bool:
        """
        Legacy method for compatibility.
        Calls reset_all_data().
        """
        return self.reset_all_data()
    
    def reload_schema_cache(self) -> bool:
        """
        Reload PostgREST schema cache via reload_api_schema_cache() RPC.
        Useful after schema changes or after reset.
        
        Returns:
            True if successful
        """
        try:
            print("   🔄 Reloading PostgREST schema cache...")
            self.client.rpc("reload_api_schema_cache").execute()
            print("   ✓ Schema cache reloaded")
            return True
        except Exception as e:
            print(f"   ⚠ Failed to reload schema cache: {e}")
            return False
    
    def seed_base_data(self, cities: List[Dict], sources: List[Dict]) -> bool:
        """
        Seed cities and sources tables from config.
        
        Args:
            cities: List of city dicts from config.CITIES
            sources: List of source dicts from config.SOURCES
            
        Returns:
            True if successful
        """
        try:
            # Upsert cities
            print(f"   📍 Seeding {len(cities)} cities...")
            for city in cities:
                city_row = {
                    "city_key": city["city_key"],
                    "display_name": city.get("name", city["city_key"]),
                    "country_code": city.get("country_code"),
                    "aliases": city.get("aliases", []),
                    "enabled": city.get("enabled", True),
                    "latitude": city.get("latitude"),
                    "longitude": city.get("longitude"),
                    "timezone": city.get("timezone"),
                }
                self.client.table("cities").upsert(city_row, on_conflict="city_key").execute()
            print(f"   ✓ Cities seeded")
            
            # Upsert sources
            print(f"   📡 Seeding {len(sources)} sources...")
            for source in sources:
                source_row = {
                    "source_key": source["source_key"],
                    "name": source.get("name"),
                    "type": source["type"],
                    "url": source["url"],
                    "enabled": source.get("enabled", True),
                    "city_key": source.get("city_key"),
                    "tags": source.get("tags") or [],
                }
                self.client.table("sources").upsert(source_row, on_conflict="source_key").execute()
            print(f"   ✓ Sources seeded")
            
            # Reload schema cache to ensure new data is visible
            self.reload_schema_cache()
            
            return True
        except Exception as e:
            print(f"   ✗ Failed to seed base data: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def get_cities(self) -> List[Dict]:
        """Get all cities from database."""
        try:
            response = self.client.table("cities").select("*").execute()
            return response.data
        except Exception as e:
            print(f"Warning: Failed to fetch cities: {e}")
            return []
    
    def get_sources(self) -> List[Dict]:
        """Get all sources from database."""
        try:
            response = self.client.table("sources").select("*").execute()
            return response.data
        except Exception as e:
            print(f"Warning: Failed to fetch sources: {e}")
            return []
    
    # ============================================================================
    # Feed Items (Schema v3: unchanged from v2)
    # ============================================================================
    
    def upsert_feed_item(
        self,
        source_key: str,
        guid: Optional[str],
        url: Optional[str],
        title: str,
        summary: str,
        published_at: Optional[datetime],
        travel_relevance_score: Optional[float] = None,
        travel_keywords_matched: Optional[List[str]] = None,
        travel_relevance_reason: Optional[str] = None,
        raw: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """
        Upsert a single feed item (schema v3).
        
        Dedupe logic:
        - If guid exists: upsert by (source_key, guid)
        - Else if url exists: upsert by (source_key, url)
        - Else: insert without upsert
        
        Args:
            source_key: Source identifier
            guid: RSS GUID or None
            url: Article URL or None
            title: Article title
            summary: Article summary
            published_at: Publication datetime or None
            raw: Full original data
            
        Returns:
            feed_item_id (UUID as string) or None if failed
        """
        def _safe_parse_date(value: Optional[object]) -> Optional[datetime]:
            if not value:
                return None
            if isinstance(value, datetime):
                dt = value
            else:
                text = str(value).strip()
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

        def _derive_date_from_url(url_value: Optional[str]) -> Optional[datetime]:
            if not url_value:
                return None
            url_value = url_value.strip()
            patterns = [
                re.compile(r"(?P<y>\d{4})[\-/](?P<m>\d{1,2})[\-/](?P<d>\d{1,2})"),
                re.compile(r"(?P<d>\d{1,2})[\-/](?P<m>\d{1,2})[\-/](?P<y>\d{4})"),
            ]
            for pattern in patterns:
                match = pattern.search(url_value)
                if not match:
                    continue
                try:
                    year = int(match.group("y"))
                    month = int(match.group("m"))
                    day = int(match.group("d"))
                    dt = datetime(year, month, day, tzinfo=timezone.utc)
                    # Basic sanity check: ignore dates far in the past/future
                    if 2000 <= year <= datetime.now(timezone.utc).year + 1:
                        return dt
                except Exception:
                    continue
            return None

        optional_fields = (
            "travel_relevance_score",
            "travel_keywords_matched",
            "travel_relevance_reason",
        )

        def _strip_optional_fields(row: Dict[str, Any]) -> Dict[str, Any]:
            for field in optional_fields:
                row.pop(field, None)
            return row

        def _is_missing_column_error(exc: Exception) -> bool:
            message = str(exc).lower()
            if "column" in message and "does not exist" in message:
                return True
            if "could not find" in message and "schema cache" in message:
                return True
            if "pgrst204" in message:
                return True
            code = getattr(exc, "code", None)
            if code and str(code).lower() == "pgrst204":
                return True
            for arg in getattr(exc, "args", []) or []:
                if isinstance(arg, dict):
                    arg_code = str(arg.get("code", "")).lower()
                    arg_message = str(arg.get("message", "")).lower()
                    if arg_code == "pgrst204":
                        return True
                    if "could not find" in arg_message and "schema cache" in arg_message:
                        return True
            return False

        try:
            fetched_at = datetime.now(timezone.utc)
            published_iso = None
            derived_published = published_at or _safe_parse_date((raw or {}).get("published")) or _derive_date_from_url(url)
            if derived_published:
                try:
                    published_iso = derived_published.isoformat()
                except Exception:
                    published_iso = None
            
            row = {
                "source_key": source_key,
                "guid": guid if guid else None,
                "url": url if url else None,
                "title": title,
                "summary": summary,
                "fetched_at": fetched_at.isoformat(),
                "raw": raw or {},
            }
            
            # Only include published_at if we truly have it (avoid fabricating dates)
            if published_iso is not None:
                row["published_at"] = published_iso
            if travel_relevance_score is not None:
                row["travel_relevance_score"] = travel_relevance_score
            if travel_keywords_matched is not None:
                row["travel_keywords_matched"] = travel_keywords_matched
            if travel_relevance_reason is not None:
                row["travel_relevance_reason"] = travel_relevance_reason
            
            # Try to find existing item
            existing_id = None
            
            if guid:
                # Try to find by guid
                response = self.client.table("feed_items").select("id").eq("source_key", source_key).eq("guid", guid).execute()
                if response.data:
                    existing_id = response.data[0]["id"]
            
            if not existing_id and url:
                # Try to find by url
                response = self.client.table("feed_items").select("id").eq("source_key", source_key).eq("url", url).execute()
                if response.data:
                    existing_id = response.data[0]["id"]
            
            if self._feed_item_optional_columns is False:
                row = _strip_optional_fields(row)

            if existing_id:
                # Update existing
                row["id"] = existing_id
                try:
                    self.client.table("feed_items").update(row).eq("id", existing_id).execute()
                except Exception as exc:
                    if self._feed_item_optional_columns is not False and _is_missing_column_error(exc):
                        self._feed_item_optional_columns = False
                        row = _strip_optional_fields(row)
                        self.client.table("feed_items").update(row).eq("id", existing_id).execute()
                    else:
                        raise
                return existing_id
            else:
                # Insert new
                try:
                    response = self.client.table("feed_items").insert(row).execute()
                except Exception as exc:
                    if self._feed_item_optional_columns is not False and _is_missing_column_error(exc):
                        self._feed_item_optional_columns = False
                        row = _strip_optional_fields(row)
                        response = self.client.table("feed_items").insert(row).execute()
                    else:
                        raise
                if response.data:
                    return response.data[0]["id"]
                return None
                
        except Exception as e:
            print(f"Warning: Failed to upsert feed item: {e}")
            return None
    
    def link_item_to_city(self, feed_item_id: str, city_key: str, match_meta: Optional[Dict] = None) -> bool:
        """
        Link a feed item to a city (many-to-many).
        
        Args:
            feed_item_id: Feed item UUID
            city_key: City identifier
            match_meta: Optional metadata about the match (e.g., query, score)
            
        Returns:
            True if successful
        """
        try:
            row = {
                "feed_item_id": feed_item_id,
                "city_key": city_key,
                "match_meta": match_meta or {},
            }
            
            # Upsert: ON CONFLICT (feed_item_id, city_key) DO UPDATE SET match_meta=EXCLUDED.match_meta
            self.client.table("feed_item_cities").upsert(row, on_conflict="feed_item_id,city_key").execute()
            return True
        except Exception as e:
            print(f"Warning: Failed to link item to city: {e}")
            return False
    
    def get_feed_items(
        self,
        source_key: str,
        city_key: Optional[str] = None,
        since_hours: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List[FeedItem]:
        """
        Get feed items from Supabase (schema v3 with JOIN).
        
        Args:
            source_key: Source identifier
            city_key: Optional city filter (uses JOIN with feed_item_cities)
            since_hours: Only return items from the last N hours
            limit: Maximum number of items to return
            
        Returns:
            List of FeedItem objects, newest first
        """
        try:
            cutoff = None
            if since_hours:
                cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)

            rows = []
            
            if cutoff:
                # Query 1: Items with published_at >= cutoff
                if city_key:
                    query1 = (
                        self.client.table("feed_items")
                        .select("*, feed_item_cities!inner(city_key)")
                        .eq("source_key", source_key)
                        .eq("feed_item_cities.city_key", city_key)
                        .gte("published_at", cutoff.isoformat())
                        .order("published_at", desc=True)
                    )
                else:
                    query1 = (
                        self.client.table("feed_items")
                        .select("*")
                        .eq("source_key", source_key)
                        .gte("published_at", cutoff.isoformat())
                        .order("published_at", desc=True)
                    )
                
                response1 = query1.execute()
                rows.extend(response1.data or [])
                
                # Query 2: Items without published_at but with recent fetched_at
                if city_key:
                    query2 = (
                        self.client.table("feed_items")
                        .select("*, feed_item_cities!inner(city_key)")
                        .eq("source_key", source_key)
                        .eq("feed_item_cities.city_key", city_key)
                        .is_("published_at", "null")
                        .gte("fetched_at", cutoff.isoformat())
                        .order("fetched_at", desc=True)
                    )
                else:
                    query2 = (
                        self.client.table("feed_items")
                        .select("*")
                        .eq("source_key", source_key)
                        .is_("published_at", "null")
                        .gte("fetched_at", cutoff.isoformat())
                        .order("fetched_at", desc=True)
                    )
                
                response2 = query2.execute()
                rows.extend(response2.data or [])
            else:
                # No time filter - get all items
                if city_key:
                    query = (
                        self.client.table("feed_items")
                        .select("*, feed_item_cities!inner(city_key)")
                        .eq("source_key", source_key)
                        .eq("feed_item_cities.city_key", city_key)
                        .order("published_at", desc=True)
                    )
                else:
                    query = (
                        self.client.table("feed_items")
                        .select("*")
                        .eq("source_key", source_key)
                        .order("published_at", desc=True)
                    )
                
                response = query.execute()
                rows.extend(response.data or [])

            def _parse_dt(value):
                if not value:
                    return None
                try:
                    dt = datetime.fromisoformat(str(value))
                except Exception:
                    return None
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt

            items = []
            for row in rows:
                raw = row.get("raw", {}) or {}
                relevance_score = row.get("travel_relevance_score")
                keywords_matched = row.get("travel_keywords_matched")
                relevance_reason = row.get("travel_relevance_reason")
                if relevance_score is None and isinstance(raw, dict):
                    relevance_score = raw.get("travel_relevance_score")
                if keywords_matched is None and isinstance(raw, dict):
                    keywords_matched = raw.get("travel_keywords_matched")
                if relevance_reason is None and isinstance(raw, dict):
                    relevance_reason = raw.get("travel_relevance_reason")

                item = FeedItem(
                    id=row["id"],
                    source_key=row["source_key"],
                    city_key=city_key,
                    guid=row.get("guid"),
                    url=row.get("url"),
                    title=row.get("title", ""),
                    summary=row.get("summary", ""),
                    published_at=_parse_dt(row.get("published_at")),
                    fetched_at=_parse_dt(row.get("fetched_at")) or datetime.now(timezone.utc),
                    raw=raw,
                    travel_relevance_score=relevance_score,
                    travel_keywords_matched=keywords_matched,
                    travel_relevance_reason=relevance_reason,
                )
                items.append(item)

            def _coalesce_ts(item: FeedItem) -> datetime:
                return item.published_at or item.fetched_at

            items.sort(key=_coalesce_ts, reverse=True)
            if limit:
                items = items[:limit]

            return items
            
        except Exception as e:
            print(f"Warning: Failed to fetch feed items from Supabase: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def get_relevant_city_feed_items(
        self,
        city_key: str,
        lookback_hours: int = 48,
        limit: int = 50,
        min_score: float = 0.0,
        exclude_source_keys: Optional[List[str]] = None,
        require_published_at_for_incidents: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Get feed items for a city filtered and sorted for traveler relevance.

        Rules:
        - Time filter uses published_at when present; optionally allows fetched_at when published_at is null.
        - Relevance: keep when score >= min_score OR item is critical; drop when reason == "excluded".
        - Keywords: require non-empty keywords unless critical.
        - Exclude advisories/forecasts and any caller-provided sources.
        - Sorting: critical first, then score desc, then effective_time desc.
        - Dedup: normalized URL, else (normalized title, date) before applying limit.
        """
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)

            denylist = {
                "canada_travel_advisories",
                "us_travel_advisories",
                "open_meteo_forecast",
                "open_meteo",
            }
            if exclude_source_keys:
                denylist.update(exclude_source_keys)

            critical_terms = {
                "shooting", "bomb", "explosion", "terror", "attack", "killed",
                "murder", "fatal", "death", "emergency", "outbreak", "pandemic",
                "earthquake", "tsunami", "hurricane", "flood", "fire", "evacuation",
            }

            def _parse_dt(value: Optional[object]) -> Optional[datetime]:
                if not value:
                    return None
                if isinstance(value, datetime):
                    dt = value
                else:
                    text = str(value)
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

            # Pull city-linked items with published_at in window
            rows: List[Dict[str, Any]] = []
            query_pub = (
                self.client.table("feed_items")
                .select("*, feed_item_cities!inner(city_key)")
                .eq("feed_item_cities.city_key", city_key)
                .gte("published_at", cutoff.isoformat())
            )
            if denylist:
                query_pub = query_pub.not_.in_("source_key", list(denylist))
            response_pub = query_pub.execute()
            rows.extend(response_pub.data or [])

            # Optionally include undated items by fetched_at when allowed
            if not require_published_at_for_incidents:
                query_fetch = (
                    self.client.table("feed_items")
                    .select("*, feed_item_cities!inner(city_key)")
                    .eq("feed_item_cities.city_key", city_key)
                    .is_("published_at", "null")
                    .gte("fetched_at", cutoff.isoformat())
                )
                if denylist:
                    query_fetch = query_fetch.not_.in_("source_key", list(denylist))
                response_fetch = query_fetch.execute()
                rows.extend(response_fetch.data or [])

            # Dedup by ID
            seen_ids = set()
            unique_rows: List[Dict[str, Any]] = []
            for row in rows:
                if row.get("id") in seen_ids:
                    continue
                seen_ids.add(row.get("id"))
                unique_rows.append(row)

            filtered: List[Dict[str, Any]] = []
            for row in unique_rows:
                src = row.get("source_key")
                if src in denylist:
                    continue

                raw = row.get("raw", {}) or {}
                score = row.get("travel_relevance_score")
                if score is None and isinstance(raw, dict):
                    score = raw.get("travel_relevance_score")

                keywords = row.get("travel_keywords_matched")
                if keywords is None and isinstance(raw, dict):
                    keywords = raw.get("travel_keywords_matched")

                reason = row.get("travel_relevance_reason")
                if reason is None and isinstance(raw, dict):
                    reason = raw.get("travel_relevance_reason")

                # Exclude explicit “excluded” reasons
                if reason and "excluded" in str(reason).lower():
                    continue

                published_dt = _parse_dt(row.get("published_at"))
                fetched_dt = _parse_dt(row.get("fetched_at"))
                effective_time = published_dt or fetched_dt

                # Enforce published_at requirement when set
                if require_published_at_for_incidents and not published_dt:
                    continue

                # Time window enforcement using effective_time
                if not effective_time or effective_time < cutoff:
                    continue

                effective_score = score if isinstance(score, (int, float)) else 0.0

                # Critical flag
                is_critical = False
                if reason and "critical" in str(reason).lower():
                    is_critical = True
                if not is_critical and keywords and isinstance(keywords, list):
                    lowered = " ".join(str(k).lower() for k in keywords)
                    if any(term in lowered for term in critical_terms):
                        is_critical = True
                if not is_critical:
                    title_text = str(row.get("title", "")).lower()
                    if any(term in title_text for term in critical_terms):
                        is_critical = True

                # Relevance gate: keep if score >= min_score or critical
                if effective_score < min_score and not is_critical:
                    continue

                # Keywords requirement unless critical
                if not is_critical:
                    if not keywords or (isinstance(keywords, list) and len(keywords) == 0):
                        continue

                filtered.append({
                    **row,
                    "_effective_score": effective_score,
                    "_effective_time": effective_time,
                    "_is_critical": is_critical,
                })

            # Sort deterministically
            filtered.sort(
                key=lambda x: (
                    not x["_is_critical"],
                    -x["_effective_score"],
                    -x["_effective_time"].timestamp(),
                )
            )

            def _normalize_url(url_value: Optional[str]) -> str:
                if not url_value:
                    return ""
                url_value = str(url_value).lower().strip()
                return url_value.split("?")[0].split("#")[0]

            def _normalize_title(title_value: Optional[str]) -> str:
                if not title_value:
                    return ""
                text = str(title_value).lower()
                text = re.sub(r"[^a-z0-9\s]", " ", text)
                return re.sub(r"\s+", " ", text).strip()

            deduped: List[Dict[str, Any]] = []
            seen_urls = set()
            seen_title_dates = set()
            for row in filtered:
                eff_time = row.get("_effective_time")
                date_key = ""
                if isinstance(eff_time, datetime):
                    date_key = eff_time.date().isoformat()
                url_key = _normalize_url(row.get("url"))
                if url_key:
                    if url_key in seen_urls:
                        continue
                    seen_urls.add(url_key)
                else:
                    title_key = _normalize_title(row.get("title"))
                    combo = (title_key, date_key)
                    if combo in seen_title_dates:
                        continue
                    seen_title_dates.add(combo)
                deduped.append(row)
                if limit and len(deduped) >= limit:
                    break

            return deduped

        except Exception as e:
            print(f"Error querying relevant city feed items: {e}")
            return []
    
    def upsert_feed_items(self, items: List[FeedItem]) -> int:
        """
        Batch upsert feed items.
        Uses upsert_feed_item() + link_item_to_city() for schema v3.
        
        Args:
            items: List of FeedItem objects
            
        Returns:
            Number of items successfully stored
        """
        count = 0
        for item in items:
            # Upsert the item
            feed_item_id = self.upsert_feed_item(
                source_key=item.source_key,
                guid=item.guid,
                url=item.url,
                title=item.title,
                summary=item.summary,
                published_at=item.published_at,
                travel_relevance_score=item.travel_relevance_score,
                travel_keywords_matched=item.travel_keywords_matched,
                travel_relevance_reason=item.travel_relevance_reason,
                raw=item.raw,
            )
            
            if feed_item_id and item.city_key:
                # Link to city if city_key is provided
                self.link_item_to_city(feed_item_id, item.city_key, match_meta=item.match_meta)
                count += 1
            elif feed_item_id:
                count += 1
        
        return count
    
    # ============================================================================
    # Weather Forecasts (Schema v3: unchanged from v2)
    # ============================================================================
    
    def upsert_weather_forecast(
        self,
        city_key: str,
        provider: str,
        raw: Dict[str, Any],
        extracted_fields: Optional[Dict[str, Any]] = None,
    ) -> bool:
        try:
            now_iso = datetime.now(timezone.utc).isoformat()

            payload: Dict[str, Any] = {
                "city_key": city_key,
                "provider": provider,
                "fetched_at": now_iso,
                "raw": raw,
            }

            if extracted_fields:
                payload["current_temp_c"] = extracted_fields.get("current_temp_c")
                payload["current_wind_kph"] = extracted_fields.get("current_wind_kph")
                payload["current_precip_mm"] = extracted_fields.get("current_precip_mm")

            existing = (
                self.client.table("weather_forecasts")
                .select("id,fetched_at")
                .eq("city_key", city_key)
                .eq("provider", provider)
                .order("fetched_at", desc=True)
                .limit(25)
                .execute()
            )

            rows = existing.data or []

            if rows:
                keep_id = rows[0]["id"]

                self.client.table("weather_forecasts").update(payload).eq("id", keep_id).execute()

                extra_ids = [r["id"] for r in rows[1:] if r.get("id")]
                if extra_ids:
                    self.client.table("weather_forecasts").delete().in_("id", extra_ids).execute()

                return True

            self.client.table("weather_forecasts").insert(payload).execute()
            return True

        except Exception as e:
            print(f"Warning: Failed to replace weather forecast: {e}")
            return False
    
    def get_latest_weather_forecast(self, city_key: str, provider: str = "open_meteo") -> Optional[Dict[str, Any]]:
        """
        Get the most recent weather forecast for a city.
        
        Args:
            city_key: City identifier
            provider: Weather provider (default: "open_meteo")
            
        Returns:
            Dictionary with forecast data (including fetched_at timestamp) or None if no forecast exists
        """
        try:
            response = self.client.table("weather_forecasts") \
                .select("*") \
                .eq("city_key", city_key) \
                .eq("provider", provider) \
                .order("fetched_at", desc=True) \
                .limit(1) \
                .execute()
            
            if response.data:
                return response.data[0]
            return None
        except Exception as e:
            print(f"Warning: Failed to get latest weather forecast: {e}")
            return None
    
    def prune_weather_forecasts(self, city_key: str, provider: str, retention_days: float) -> int:
        """
        Delete weather forecasts older than retention_days.
        
        Args:
            city_key: City identifier
            provider: Weather provider
            retention_days: Delete forecasts older than this many days
            
        Returns:
            Number of rows deleted
        """
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
            cutoff_iso = cutoff.isoformat()
            
            # Delete old forecasts
            response = self.client.table("weather_forecasts") \
                .delete() \
                .eq("city_key", city_key) \
                .eq("provider", provider) \
                .lt("fetched_at", cutoff_iso) \
                .execute()
            
            # Count rows deleted (response.data contains deleted rows)
            deleted_count = len(response.data) if response.data else 0
            if deleted_count > 0:
                print(f"   Pruned {deleted_count} old weather forecast(s) for {city_key}")
            return deleted_count
        except Exception as e:
            print(f"Warning: Failed to prune weather forecasts: {e}")
            return 0
    
    def get_weather_forecast_placeholder(self, city_key: str, provider: str = "open_meteo") -> Optional[Dict[str, Any]]:
        """
        Get the latest weather forecast for a city.
        
        Args:
            city_key: City identifier
            provider: Weather provider (default: "open_meteo")
            
        Returns:
            Forecast dict or None
        """
        try:
            response = (
                self.client.table("weather_forecasts")
                .select("*")
                .eq("city_key", city_key)
                .eq("provider", provider)
                .order("fetched_at", desc=True)
                .limit(1)
                .execute()
            )
            
            if response.data:
                return response.data[0]
            return None
        except Exception as e:
            print(f"Warning: Failed to fetch weather forecast: {e}")
            return None
    
    # ============================================================================
    # City Context (Schema v3: unchanged from v2)
    # ============================================================================
    
    def get_city_context(self, city_key: str) -> Optional[CityContext]:
        """Get city context from Supabase."""
        try:
            response = self.client.table("city_context_snapshots").select("*").eq("city_key", city_key).execute()
            
            if not response.data:
                return None
            
            row = response.data[0]
            return CityContext(
                city_key=row["city_key"],
                context=row.get("context", {}),
                fetched_at=datetime.fromisoformat(row["fetched_at"]),
            )
            
        except Exception as e:
            print(f"Warning: Failed to fetch city context from Supabase: {e}")
            return None
    
    def upsert_city_context(self, context: CityContext) -> bool:
        """Insert or update city context in Supabase."""
        try:
            row = {
                "city_key": context.city_key,
                "context": context.context,
                "fetched_at": context.fetched_at.isoformat(),
            }
            
            self.client.table("city_context_snapshots").upsert(row, on_conflict="city_key").execute()
            return True
            
        except Exception as e:
            print(f"Warning: Failed to upsert city context to Supabase: {e}")
            return False
    
    # ============================================================================
    # Transit Snapshots (Schema v3: unchanged from v2)
    # ============================================================================
    
    def get_transit_snapshot(self, city_key: str) -> Optional[TransitSnapshot]:
        """Get transit snapshot from Supabase."""
        try:
            response = self.client.table("transit_snapshots").select("*").eq("city_key", city_key).execute()
            
            if not response.data:
                return None
            
            row = response.data[0]
            return TransitSnapshot(
                city_key=row["city_key"],
                transit=row.get("transit", {}),
                fetched_at=datetime.fromisoformat(row["fetched_at"]),
            )
            
        except Exception as e:
            print(f"Warning: Failed to fetch transit snapshot from Supabase: {e}")
            return None
    
    def upsert_transit_snapshot(self, snapshot: TransitSnapshot) -> bool:
        """Insert or update transit snapshot in Supabase."""
        try:
            row = {
                "city_key": snapshot.city_key,
                "transit": snapshot.transit,
                "fetched_at": snapshot.fetched_at.isoformat(),
            }
            
            self.client.table("transit_snapshots").upsert(row, on_conflict="city_key").execute()
            return True
            
        except Exception as e:
            print(f"Warning: Failed to upsert transit snapshot to Supabase: {e}")
            return False
    
    # ============================================================================
    # Curated Places (Schema v3: NEW)
    # ============================================================================

    @staticmethod
    def _normalize_curated_name(name: Optional[str]) -> str:
        if not name:
            return ""
        return str(name).strip()

    @staticmethod
    def _normalize_curated_name_key(name: Optional[str]) -> str:
        text = SupabaseStore._normalize_curated_name(name)
        if not text:
            return ""
        return re.sub(r"[\W_]+", "", text.lower())

    @staticmethod
    def _normalize_curated_text(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        return text if text else None

    @staticmethod
    def _normalize_curated_website(
        value: Optional[str],
        category: Optional[str] = None,
        name: Optional[str] = None,
    ) -> Optional[str]:
        text = SupabaseStore._normalize_curated_text(value)
        if not text:
            return None
        if category == "rental_car":
            from urllib.parse import urlparse

            parsed = urlparse(text if "://" in text else f"https://{text}")
            host = (parsed.netloc or parsed.hostname or "").strip()
            if not host:
                return None
            if "@" in host:
                host = host.split("@", 1)[1]
            host = host.split(":")[0]
            host = re.sub(r"^(?:www\.){2,}", "www.", host.lower())
            scheme = parsed.scheme if parsed.scheme else "https"
            path = parsed.path or ""
            if path == "/":
                path = ""
            elif path.endswith("/"):
                path = path[:-1]
            query = parsed.query or ""
            normalized = f"{scheme}://{host}{path}"
            if query:
                normalized = f"{normalized}?{query}"
            return normalized
        if category == "airport":
            from urllib.parse import urlparse

            parsed = urlparse(text if "://" in text else f"https://{text}")
            host = (parsed.netloc or parsed.hostname or "").strip()
            if not host:
                return None
            if "@" in host:
                host = host.split("@", 1)[1]
            host = host.split(":")[0]
            host = re.sub(r"^(?:www\.){2,}", "www.", host.lower())
            scheme = parsed.scheme if parsed.scheme else "https"
            path = parsed.path or ""
            if path == "/":
                path = ""
            elif path.endswith("/"):
                path = path[:-1]
            query = parsed.query or ""
            normalized = f"{scheme}://{host}{path}"
            if query:
                normalized = f"{normalized}?{query}"
            return normalized
        if category in {"hospital", "urgent_care"}:
            from urllib.parse import urlparse

            parsed = urlparse(text if "://" in text else f"https://{text}")
            host = (parsed.netloc or parsed.hostname or "").strip()
            if "@" in host:
                host = host.split("@", 1)[1]
            host = host.split(":")[0].lower()
            host = re.sub(r"^(?:www\.){2,}", "www.", host)
            if not host:
                return None
            path = parsed.path or ""
            if path and name:
                stopwords = {
                    "hospital",
                    "hospitals",
                    "clinic",
                    "clinics",
                    "medical",
                    "health",
                    "centre",
                    "center",
                    "university",
                    "college",
                    "general",
                    "trust",
                    "nhs",
                    "st",
                    "saint",
                    "royal",
                    "the",
                    "and",
                    "of",
                }
                tokens = re.split(r"[\W_]+", name.lower())
                tokens = [t for t in tokens if t and t not in stopwords and len(t) >= 4]
                if tokens and any(token in host for token in tokens):
                    path = ""
            if path and re.search(r"/[^/]+\.(?:html?|php|asp|aspx|jsp|cfm)$", path, re.IGNORECASE):
                path = ""
            if path == "/":
                path = ""
            elif path.endswith("/"):
                path = path[:-1]
            query = parsed.query or ""
            normalized = f"https://{host}{path}"
            if query:
                normalized = f"{normalized}?{query}"
            return normalized
        normalized = normalize_domain(text)
        if normalized:
            normalized = normalized.strip().rstrip("/")
        return normalized or None

    @staticmethod
    def _normalize_curated_bool(value: Optional[bool]) -> Optional[bool]:
        if value is None:
            return None
        return bool(value)

    @staticmethod
    def _normalize_curated_status(value: Optional[str]) -> Optional[str]:
        text = SupabaseStore._normalize_curated_text(value)
        return text.lower() if text else None

    @staticmethod
    def _normalize_curated_source(value: Optional[str]) -> Optional[str]:
        text = SupabaseStore._normalize_curated_text(value)
        return text.lower() if text else None

    @staticmethod
    def _normalize_curated_timestamp(value: Optional[object]) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.isoformat()
        text = str(value).strip()
        return text if text else None

    def _lookup_rental_car_website(self, name_key: str, country_code: Optional[str] = None) -> Optional[str]:
        """
        Look up a rental car website from existing curated_places.
        Disabled to avoid cross-city reuse; rental car URLs come from the allowlisted resolver.
        
        Args:
            name_key: Normalized name key (e.g., "enterprise", "avis")
            country_code: ISO country code (e.g., "ES", "US"). If provided, only matches from same country cities.
            
        Returns:
            Website URL or None
        """
        if not name_key:
            return None
        # Rental car URLs must come from the allowlisted resolver, not cross-city reuse.
        return None

    def prefetch_curated_places(self, city_key: str) -> Dict[Tuple[str, str], Dict[str, Any]]:
        """
        Prefetch curated_places for a city into a dict keyed by (category, normalized_name).
        """
        try:
            response = (
                self.client.table("curated_places")
                .select("*")
                .eq("city_key", city_key)
                .execute()
            )
            cache: Dict[Tuple[str, str], Dict[str, Any]] = {}
            for row in response.data or []:
                category = row.get("category")
                name = self._normalize_curated_name(row.get("name"))
                if not category or not name:
                    continue
                cache[(category, name)] = row
            return cache
        except Exception as e:
            print(f"Warning: Failed to prefetch curated places: {e}")
            return {}
    
    def get_curated_places(self, city_key: str, category: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get curated places from Supabase for a city.

        Important:
        Do not filter to must_include only.
        ReportDataLoader applies its own limits and ensures must_include rows are retained.
        
        Args:
            city_key: City identifier
            category: Optional category filter (hospital/urgent_care/embassy/etc)
            
        Returns:
            List of curated place dicts
        """
        try:
            query = (
                self.client
                .table("curated_places")
                .select("*")
                .eq("city_key", city_key)
            )
            
            if category:
                query = query.eq("category", category)

            # Prefer must_include first for stable report output
            try:
                query = query.order("must_include", desc=True).order("name", desc=False)
            except Exception:
                # If multi order is not supported by the client, keep it simple
                query = query.order("must_include", desc=True)
            
            response = query.execute()
            return response.data or []
            
        except Exception as e:
            print(f"Warning: Failed to fetch curated places from Supabase: {e}")
            return []

    def get_curated_places_all(self, category: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetch curated places across all cities (must_include only)."""
        try:
            query = self.client.table("curated_places").select("*").eq("must_include", True)
            if category:
                query = query.eq("category", category)
            response = query.execute()
            return response.data or []
        except Exception as e:
            print(f"Warning: Failed to fetch curated places (all cities): {e}")
            return []
    
    def upsert_curated_place(
        self,
        city_key: str,
        category: str,
        name: str,
        website: Optional[str] = None,
        notes: Optional[str] = None,
        must_include: bool = True,
        website_canonical: Optional[str] = None,
        website_source: Optional[str] = None,
        website_status: Optional[str] = None,
        website_verified_at: Optional[object] = None,
        website_verification_reason: Optional[str] = None,
    ) -> bool:
        """
        Add or update a curated place.
        
        Note: Location data (address/lat/lon) is stored in city_context_snapshots.context JSON,
        not in curated_places table per schema_v3.
        
        Args:
            city_key: City identifier
            category: Category (hospital/urgent_care/embassy/etc)
            name: Place name
            website: Optional website URL
            notes: Optional notes
            must_include: Whether to force-include in reports
            website_canonical: Canonicalized website URL after verification
            website_source: Website source tag (osm/manual/google_places)
            website_status: Verification status (ok/warn/reject)
            website_verified_at: Timestamp of latest verification
            website_verification_reason: Human-readable verification reason
            
        Returns:
            True if successful
        """
        if not city_key or not str(city_key).strip():
            print("Warning: Missing city_key for curated place upsert")
            return False
        try:
            optional_fields = (
                "website_canonical",
                "website_source",
                "website_status",
                "website_verified_at",
                "website_verification_reason",
            )

            def _strip_optional_fields(row: Dict[str, Any]) -> Dict[str, Any]:
                for field in optional_fields:
                    row.pop(field, None)
                return row

            def _is_missing_column_error(exc: Exception) -> bool:
                message = str(exc).lower()
                if "column" in message and "does not exist" in message:
                    return True
                if "could not find" in message and "schema cache" in message:
                    return True
                if "pgrst204" in message:
                    return True
                code = getattr(exc, "code", None)
                if code and str(code).lower() == "pgrst204":
                    return True
                for arg in getattr(exc, "args", []) or []:
                    if isinstance(arg, dict):
                        arg_code = str(arg.get("code", "")).lower()
                        arg_message = str(arg.get("message", "")).lower()
                        if arg_code == "pgrst204":
                            return True
                        if "could not find" in arg_message and "schema cache" in arg_message:
                            return True
                return False

            # Schema v3 does not define a unique constraint for (city_key, category, name),
            # so we implement a best-effort idempotent upsert in application code.
            normalized_website = self._normalize_curated_website(website, category, name)
            normalized_notes = self._normalize_curated_text(notes)
            normalized_must_include = self._normalize_curated_bool(must_include)
            normalized_canonical = self._normalize_curated_text(website_canonical)
            normalized_source = self._normalize_curated_source(website_source)
            normalized_status = self._normalize_curated_status(website_status)
            normalized_verified_at = self._normalize_curated_timestamp(website_verified_at)
            normalized_reason = self._normalize_curated_text(website_verification_reason)

            existing_row = None
            matched_by_provider = False

            if category == "rental_car" and detect_rental_car_provider:
                provider_key = detect_rental_car_provider(name)
                if provider_key:
                    response = (
                        self.client.table("curated_places")
                        .select("*")
                        .eq("city_key", city_key)
                        .eq("category", category)
                        .execute()
                    )
                    for row in response.data or []:
                        if detect_rental_car_provider(row.get("name", "")) == provider_key:
                            existing_row = row
                            matched_by_provider = True
                            break

            if existing_row is None:
                existing = (
                    self.client.table("curated_places")
                    .select("*")
                    .eq("city_key", city_key)
                    .eq("category", category)
                    .eq("name", name)
                    .limit(1)
                    .execute()
                )
                if existing.data:
                    existing_row = existing.data[0] or {}

            if existing_row:
                row = existing_row
                row_id = row.get("id")
                existing_raw_website = row.get("website")
                existing_website = self._normalize_curated_website(
                    existing_raw_website,
                    category,
                    row.get("name") or name,
                )
                existing_notes = self._normalize_curated_text(row.get("notes"))
                existing_must_include = self._normalize_curated_bool(row.get("must_include"))
                existing_name = self._normalize_curated_name(row.get("name"))
                existing_canonical = self._normalize_curated_text(row.get("website_canonical"))
                existing_source = self._normalize_curated_source(row.get("website_source"))
                existing_status = self._normalize_curated_status(row.get("website_status"))
                existing_verified_at = self._normalize_curated_timestamp(row.get("website_verified_at"))
                existing_reason = self._normalize_curated_text(row.get("website_verification_reason"))

                if normalized_website is None and existing_website is not None:
                    normalized_website = existing_website
                if normalized_canonical is None:
                    if existing_canonical is not None and normalized_website == existing_website:
                        normalized_canonical = existing_canonical
                    elif normalized_website is not None:
                        normalized_canonical = normalized_website

                if normalized_status is None and existing_status is not None:
                    normalized_status = existing_status
                if normalized_source is None and existing_source is not None:
                    normalized_source = existing_source
                if normalized_reason is None and existing_reason is not None:
                    normalized_reason = existing_reason
                if normalized_verified_at is None and existing_verified_at is not None:
                    normalized_verified_at = existing_verified_at

                existing_needs_cleanup = False
                if existing_raw_website and normalized_website:
                    existing_needs_cleanup = existing_raw_website.strip().rstrip("/") != normalized_website

                updates: Dict[str, Any] = {}
                if normalized_website != existing_website or existing_needs_cleanup:
                    updates["website"] = normalized_website
                if normalized_canonical != existing_canonical:
                    updates["website_canonical"] = normalized_canonical
                if normalized_notes != existing_notes:
                    updates["notes"] = normalized_notes
                if normalized_must_include != existing_must_include:
                    updates["must_include"] = normalized_must_include
                if normalized_source != existing_source:
                    updates["website_source"] = normalized_source
                if normalized_status != existing_status:
                    updates["website_status"] = normalized_status
                if normalized_verified_at != existing_verified_at:
                    updates["website_verified_at"] = normalized_verified_at
                if normalized_reason != existing_reason:
                    updates["website_verification_reason"] = normalized_reason
                if matched_by_provider and existing_name != self._normalize_curated_name(name):
                    updates["name"] = name

                if not updates:
                    return True

                if self._curated_place_optional_columns is False:
                    updates = _strip_optional_fields(updates)
                    if not updates:
                        return True

                if row_id:
                    try:
                        self.client.table("curated_places").update(updates).eq("id", row_id).execute()
                    except Exception as exc:
                        if self._curated_place_optional_columns is not False and _is_missing_column_error(exc):
                            self._curated_place_optional_columns = False
                            updates = _strip_optional_fields(updates)
                            if updates:
                                self.client.table("curated_places").update(updates).eq("id", row_id).execute()
                        else:
                            raise
                    return True

            if normalized_canonical is None and normalized_website is not None:
                normalized_canonical = normalized_website

            payload = {
                "city_key": city_key,
                "category": category,
                "name": name,
                "website": normalized_website,
                "notes": normalized_notes,
                "must_include": normalized_must_include,
                "website_canonical": normalized_canonical,
                "website_source": normalized_source,
                "website_status": normalized_status,
                "website_verified_at": normalized_verified_at,
                "website_verification_reason": normalized_reason,
            }
            if self._curated_place_optional_columns is False:
                payload = _strip_optional_fields(payload)
            try:
                self.client.table("curated_places").insert(payload).execute()
            except Exception as exc:
                if self._curated_place_optional_columns is not False and _is_missing_column_error(exc):
                    self._curated_place_optional_columns = False
                    payload = _strip_optional_fields(payload)
                    self.client.table("curated_places").insert(payload).execute()
                else:
                    raise
            return True
            
        except Exception as e:
            print(f"Warning: Failed to upsert curated place: {e}")
            return False
    
    # ============================================================================
    # City Reports (Schema v3: NEW)
    # ============================================================================
    
    def upsert_city_report(
        self,
        city_key: str,
        report_data: Dict[str, Any],
        trend_data: Optional[Dict[str, Any]],
        model: str,
        window_hours: int,
        pdf_file_url: Optional[str] = None,
        updated_by: Optional[str] = None,
    ) -> bool:
        """
        Save or update the latest AI-generated report for a city.
        Replaces previous report (upsert).
        
        Args:
            city_key: City identifier
            report_data: Full report JSON (narrative sections, etc.)
            trend_data: Optional trend statistics JSON
            model: AI model used (e.g., "gpt-4-turbo-preview")
            window_hours: Report time window (e.g., 48)
            pdf_file_url: Optional URL to generated PDF
            updated_by: Optional user/system identifier
            
        Returns:
            True if successful
        """
        from report_data_contract import validate_report_data

        # Enforce schema before writing. If invalid, fail the run loudly.
        validate_report_data(report_data)

        try:
            row = {
                "city_key": city_key,
                "report_data": report_data,
                "trend_data": trend_data,
                "model": model,
                "window_hours": window_hours,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "pdf_file_url": pdf_file_url,
                "updated_by": updated_by,
            }
            
            self.client.table("city_reports").upsert(row, on_conflict="city_key").execute()
            return True
            
        except Exception as e:
            print(f"Warning: Failed to upsert city report: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def get_city_report(self, city_key: str) -> Optional[Dict[str, Any]]:
        """
        Get the latest report for a city.
        
        Args:
            city_key: City identifier
            
        Returns:
            Report dict or None
        """
        try:
            response = (
                self.client.table("city_reports")
                .select("*")
                .eq("city_key", city_key)
                .execute()
            )
            
            if response.data:
                return response.data[0]
            return None
            
        except Exception as e:
            print(f"Warning: Failed to fetch city report: {e}")
            return None
    
    # ============================================================================
    # Utility Methods
    # ============================================================================
    
    def get_incident_trends(
        self,
        city_key: str,
        lookback_days: int = 30,
        categories: Optional[List[str]] = None,
    ) -> Dict[str, List[Dict]]:
        """
        Get daily incident counts for trend analysis.
        
        Returns raw daily buckets from feed_items for accurate trend computation.
        Includes days with zero incidents for proper visualization.
        
        Args:
            city_key: City identifier
            lookback_days: Number of days to look back
            categories: Optional list of categories to filter
            
        Returns:
            Dict with date buckets per category
        """
        try:
            # Calculate date range
            end_date = datetime.now(timezone.utc).date()
            start_date = end_date - timedelta(days=lookback_days)
            
            # Query feed items for city in date range
            response = (
                self.client.table("feed_items")
                .select("id, title, summary, published_at, feed_item_cities!inner(city_key)")
                .eq("feed_item_cities.city_key", city_key)
                .gte("published_at", start_date.isoformat())
                .lte("published_at", (end_date + timedelta(days=1)).isoformat())
                .order("published_at", desc=False)
                .execute()
            )
            
            # Categorize items
            def _norm(text: str) -> str:
                text = text or ""
                text = unicodedata.normalize("NFKD", text)
                text = "".join(c for c in text if not unicodedata.combining(c))
                text = text.lower()
                text = re.sub(r"\s+", " ", text).strip()
                return text
            
            def categorize_item(item: Dict) -> str:
                """Categorize an item as crime, traffic, weather, terrorism, health, or other."""
                text = _norm(f"{item.get('title', '')} {item.get('summary', '')}")
                
                # Terrorism keywords (highest priority)
                terrorism_kw = ["terror", "terrorist", "terrorism", "bomb", "explosion", "attack", "armed attack", "extremist"]
                if any(kw in text for kw in terrorism_kw):
                    return "terrorism"
                
                # Health keywords
                health_kw = ["outbreak", "epidemic", "disease", "virus", "covid", "flu", "measles", "contamination", "quarantine", "health alert"]
                if any(kw in text for kw in health_kw):
                    return "health"
                
                # Crime keywords
                crime_kw = ["shooting", "robbery", "assault", "murder", "stabbing", "carjacking", "theft", "violence", "crime", "gang", "kidnap"]
                if any(kw in text for kw in crime_kw):
                    return "crime"
                
                # Traffic keywords
                traffic_kw = ["traffic", "road closure", "highway", "accident", "crash", "delay", "congestion", "bridge closed", "transit strike"]
                if any(kw in text for kw in traffic_kw):
                    return "traffic"
                
                # Weather keywords
                weather_kw = ["hurricane", "storm", "flood", "tornado", "weather", "rain", "snow", "wind", "blizzard", "heat wave"]
                if any(kw in text for kw in weather_kw):
                    return "weather"
                
                return "other"
            
            # Group items by date and category
            from collections import defaultdict
            daily_counts = defaultdict(lambda: defaultdict(int))
            
            for item in response.data:
                if not item.get("published_at"):
                    continue
                
                pub_date = datetime.fromisoformat(item["published_at"]).date()
                category = categorize_item(item)
                
                daily_counts[pub_date][category] += 1
                daily_counts[pub_date]["total"] += 1
            
            # Build result with ALL dates (including zero-count days)
            result = {
                "crime": [],
                "traffic": [],
                "weather": [],
                "terrorism": [],
                "health": [],
                "other": [],
                "total": [],
            }
            
            # Generate all dates in range
            current_date = start_date
            while current_date <= end_date:
                for category in ["crime", "traffic", "weather", "terrorism", "health", "other", "total"]:
                    count = daily_counts.get(current_date, {}).get(category, 0)
                    result[category].append({
                        "date": current_date.isoformat(),
                        "count": count,
                    })
                current_date += timedelta(days=1)
            
            return result
            
        except Exception as e:
            print(f"Warning: Failed to get incident trends: {e}")
            import traceback
            traceback.print_exc()
            return {"crime": [], "traffic": [], "weather": [], "terrorism": [], "health": [], "other": [], "total": []}
    
    def is_city_context_fresh(self, city_key: str, max_age_days: int = 30) -> bool:
        """Check if city context is recent enough."""
        context = self.get_city_context(city_key)
        if not context:
            return False
        
        age_days = (datetime.now(timezone.utc) - context.fetched_at).total_seconds() / (24 * 60 * 60)
        return age_days < max_age_days
    
    def is_feed_fresh(self, source_key: str, max_age_hours: int = 24) -> bool:
        """
        Check if feed cache is recent enough.
        
        Args:
            source_key: Source identifier
            max_age_hours: Maximum age in hours
            
        Returns:
            True if cache exists and is fresh
        """
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
            
            response = (
                self.client.table("feed_items")
                .select("id")
                .eq("source_key", source_key)
                .gte("fetched_at", cutoff.isoformat())
                .limit(1)
                .execute()
            )
            
            return len(response.data) > 0
            
        except Exception:
            return False
    
    def purge_old_feed_items(self, days: int, city_key: Optional[str] = None) -> int:
        """
        Delete feed items older than specified days.
        
        Args:
            days: Delete items older than this many days
            city_key: Optional city filter
            
        Returns:
            Number of items deleted
        """
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(days=days)
            
            if city_key:
                # Get item IDs linked to this city
                links = (
                    self.client.table("feed_item_cities")
                    .select("feed_item_id")
                    .eq("city_key", city_key)
                    .execute()
                )
                item_ids = [link["feed_item_id"] for link in links.data]
                
                if not item_ids:
                    return 0
                
                # Delete items
                response = (
                    self.client.table("feed_items")
                    .delete()
                    .in_("id", item_ids)
                    .lt("published_at", cutoff.isoformat())
                    .execute()
                )
            else:
                # Delete all old items
                response = (
                    self.client.table("feed_items")
                    .delete()
                    .lt("published_at", cutoff.isoformat())
                    .execute()
                )
            
            deleted_count = len(response.data) if response.data else 0
            if deleted_count > 0:
                print(f"   ✓ Purged {deleted_count} feed items older than {days} days")
            
            return deleted_count
            
        except Exception as e:
            print(f"   ✗ Failed to purge old feed items: {e}")
            return 0
    
    def purge_old_weather_forecasts(self, days: int = 7, city_key: Optional[str] = None) -> int:
        """
        Delete weather forecasts older than specified days.
        
        Args:
            days: Delete forecasts older than this many days
            city_key: Optional city filter
            
        Returns:
            Number of forecasts deleted
        """
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(days=days)
            query = self.client.table("weather_forecasts").delete().lt("fetched_at", cutoff.isoformat())
            
            if city_key:
                query = query.eq("city_key", city_key)
            
            response = query.execute()
            deleted_count = len(response.data) if response.data else 0
            
            if deleted_count > 0:
                print(f"   ✓ Purged {deleted_count} weather forecasts older than {days} days")
            
            return deleted_count
            
        except Exception as e:
            print(f"   ✗ Failed to purge old weather forecasts: {e}")
            return 0

    def get_partner_link(self, partner_key: str, country_code: str) -> Optional[str]:
        """
        Fetch an enabled affiliate partner URL from the partner_links table.
        
        Args:
            partner_key: Partner identifier (e.g., 'spothero_parking')
            country_code: ISO country code (e.g., 'US')
            
        Returns:
            The partner URL if found and enabled, else None
        """
        try:
            response = self.client.table("partner_links").select("url").where(
                f"partner_key=eq.{partner_key},country_code=eq.{country_code},enabled=eq.true"
            ).execute()
            
            if response.data and len(response.data) > 0:
                return response.data[0].get("url")
            
            return None
        except Exception as e:
            # Silently fail - table may not exist yet, caller will use fallback
            return None

