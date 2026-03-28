"""
Unified Report Data Loader

Loads all report data from Supabase tables ONLY.
No parallel computation - single source of truth from database.

This ensures PDF and web dashboard use identical data.
"""

from typing import Dict, List, Optional, Any, Tuple
import hashlib
import re
import unicodedata
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass

from storage import get_datastore
from storage.base import DataStore
from coverage import ReportCoverage, validate_section_coverage
import config
from format_advisory import format_advisory_text
from helpers.emergency_numbers import resolve_emergency_numbers


@dataclass
class ReportViewModel:
    """
    Unified view model for report rendering.
    
    Used by both PDF generation and web dashboard to ensure
    identical data presentation.
    """
    # Metadata
    city_key: str
    city_name: str
    destination: str
    generated_utc: str
    report_id: str
    
    # Coverage tracking
    coverage: ReportCoverage
    coverage_status: str  # Summary message
    
    # Core data sections
    hospitals: List[Dict[str, Any]]
    transit: List[Dict[str, Any]]
    weather_summary: Optional[Dict[str, Any]]
    incidents: List[Dict[str, Any]]
    pharmacies: List[Dict[str, Any]]
    supermarkets: List[Dict[str, Any]]
    convenience_stores: List[Dict[str, Any]]
    rental_cars: List[Dict[str, Any]]
    airports: List[Dict[str, Any]]
    
    # Additional sections
    emergency_contacts: Dict[str, Any]
    travel_advisory: Optional[str]
    advisory_level: Optional[str]
    advisory_source: Optional[str]
    travel_advisory_url: Optional[str]
    
    # Partner links (configurable, DB-owned)
    parking: Optional[Dict[str, Any]]  # SpotHero parking link for US cities
    
    # AI-generated content (stored in city_reports if available)
    top_actions: List[str]
    whats_happening: List[str]
    risk_badges: List[Dict[str, str]]
    cues: List[Dict[str, str]]
    opsec: Optional[Dict[str, Any]]
    
    # Trend analysis
    trend_data: Optional[Dict[str, Any]]
    
    # Trip profile
    trip_profile: Optional[Dict[str, Any]]
    driving_pack: Optional[Dict[str, Any]]


class ReportDataLoader:
    """Loads complete report data from Supabase tables."""
    
    def __init__(self, datastore: Optional[DataStore] = None):
        """Initialize loader with datastore."""
        self.datastore = datastore or get_datastore()
        self._city_country_cache: Dict[str, str] = {}

    @staticmethod
    def _filter_curated_place_status(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Exclude warn/reject rows until reviewed."""
        filtered = []
        for row in rows or []:
            status = str(row.get("website_status") or "").strip().lower()
            if status in {"", "ok"}:
                filtered.append(row)
        return filtered

    @staticmethod
    def _effective_website_value(row: Optional[Dict[str, Any]]) -> str:
        if not isinstance(row, dict):
            return ""
        value = row.get("website") or row.get("website_canonical") or ""
        return str(value).strip()

    def _apply_effective_website(self, rows: List[Dict[str, Any]]) -> None:
        for row in rows or []:
            if isinstance(row, dict):
                row["website"] = self._effective_website_value(row)

    @staticmethod
    def _extract_context_website(entry: Optional[Dict[str, Any]]) -> str:
        if not isinstance(entry, dict):
            return ""
        direct = str(entry.get("website") or "").strip()
        if direct:
            return direct
        tags = entry.get("tags") if isinstance(entry.get("tags"), dict) else {}
        if not isinstance(tags, dict):
            return ""
        for key in ("contact:website", "website", "url", "contact:url", "operator:website"):
            value = str(tags.get(key) or "").strip()
            if value:
                return value
        return ""

    def _resolve_country_code(
        self,
        city_key: str,
        *,
        fallback_config: Optional[str] = None,
        city_row: Optional[Dict[str, Any]] = None,
    ) -> str:
        if not city_key:
            return ""
        if city_row and city_row.get("country_code"):
            return str(city_row.get("country_code") or "").strip().upper()
        if fallback_config:
            return str(fallback_config or "").strip().upper()
        if city_key in self._city_country_cache:
            return self._city_country_cache[city_key]
        cc = ""
        try:
            cc = str((config.CITIES.get(city_key, {}) or {}).get("country_code") or "").strip().upper()
        except Exception:
            cc = ""
        if not cc:
            try:
                client = getattr(self.datastore, "client", None)
                if client is not None:
                    resp = (
                        client.table("cities")
                        .select("country_code")
                        .eq("city_key", city_key)
                        .limit(1)
                        .execute()
                    )
                    if resp.data:
                        cc = str((resp.data[0] or {}).get("country_code") or "").strip().upper()
            except Exception:
                cc = ""
        self._city_country_cache[city_key] = cc
        return cc

    @staticmethod
    def _coerce_facility_list(value: Any) -> List[Dict[str, Any]]:
        if not isinstance(value, list):
            return []
        rows: List[Dict[str, Any]] = []
        for item in value:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            if not name:
                continue
            row = {
                "name": name,
                "address": item.get("address") or item.get("notes") or "",
                "website": str(item.get("website") or "").strip(),
            }
            if "lat" in item:
                row["lat"] = item.get("lat")
            if "lon" in item:
                row["lon"] = item.get("lon")
            if "notes" in item:
                row["notes"] = item.get("notes")
            if "iata" in item:
                row["iata"] = item.get("iata")
            rows.append(row)
        return rows

    def _get_previous_report_data(self, city_key: str) -> Dict[str, Any]:
        try:
            record = self.datastore.get_city_report(city_key)
        except Exception:
            record = None
        if not isinstance(record, dict):
            return {}
        payload = record.get("report_data")
        return payload if isinstance(payload, dict) else {}

    def _fallback_list_from_previous_report(
        self,
        city_key: str,
        current_rows: List[Dict[str, Any]],
        *,
        key: str,
        nested_parent: str | None = None,
    ) -> List[Dict[str, Any]]:
        if current_rows:
            return current_rows
        report_data = self._get_previous_report_data(city_key)
        candidate = report_data.get(key)
        if not isinstance(candidate, list) and nested_parent:
            parent = report_data.get(nested_parent)
            if isinstance(parent, dict):
                candidate = parent.get(key)
        fallback = self._coerce_facility_list(candidate)
        return fallback or current_rows
    
    def load_db_bundle(
        self,
        city_key: str,
        window_hours: int = 48,
    ) -> Dict[str, Any]:
        """
        Load all database-owned data for report generation.
        
        This method loads everything that should NOT be generated by the LLM:
        - City metadata (from cities table or config fallback)
        - City context (from city_context_snapshots)
        - Transit providers (from transit_snapshots)
        - Weather forecast (from weather_forecasts)
        - Curated places by category (from curated_places)
        - Feed items (from feed_items + feed_item_cities join)
        - Coverage flags
        
        Args:
            city_key: City identifier
            window_hours: Hours to look back for feed items
            
        Returns:
            Dict with keys:
                - city_row: City metadata
                - context: City context snapshot data
                - transit: Transit providers list
                - weather: Weather forecast data
                - curated_places: Dict by category (hospital, pharmacy, etc)
                - feed_items: Recent feed items
                - emergency_contacts: Emergency numbers
                - coverage: Coverage tracking flags
        """
        bundle = {
            'city_row': None,
            'context': {},
            'transit': [],
            'weather': None,
            'curated_places': {},
            'feed_items': [],
            'emergency_contacts': {},
            'coverage': {},
        }
        
        # Load city metadata (DB-first, config fallback)
        city_config = config.CITIES.get(city_key, {})
        try:
            client = getattr(self.datastore, "client", None)
            if client is not None:
                resp = (
                    client.table("cities")
                    .select("city_key,display_name,country_code,timezone,latitude,longitude,aliases,enabled")
                    .eq("city_key", city_key)
                    .limit(1)
                    .execute()
                )
                if resp.data:
                    bundle['city_row'] = resp.data[0]
        except Exception as e:
            print(f"Warning: Failed to load city from DB: {e}")
        
        # If no DB row, use config as fallback
        if not bundle['city_row']:
            bundle['city_row'] = {
                'city_key': city_key,
                'display_name': city_config.get('name', city_key),
                'country_code': city_config.get('country_code'),
                'timezone': city_config.get('timezone'),
                'latitude': city_config.get('latitude'),
                'longitude': city_config.get('longitude'),
            }
        
        # Load city context
        snapshot = self.datastore.get_city_context(city_key)
        if snapshot and snapshot.context:
            bundle['context'] = snapshot.context
        
        # Load transit from transit_snapshots
        transit_snapshot = self.datastore.get_transit_snapshot(city_key)
        if transit_snapshot and transit_snapshot.transit:
            providers = transit_snapshot.transit.get('providers', [])
            bundle['transit'] = providers
            bundle['coverage']['transit'] = len(providers) > 0
        else:
            bundle['transit'] = []
            bundle['coverage']['transit'] = False
        
        # Load weather from weather_forecasts
        try:
            response = self.datastore.client.table("weather_forecasts") \
                .select("*") \
                .eq("city_key", city_key) \
                .order("fetched_at", desc=True) \
                .limit(1) \
                .execute()
            
            if response.data:
                bundle['weather'] = response.data[0].get('raw', {})
                bundle['coverage']['weather'] = True
            else:
                bundle['coverage']['weather'] = False
        except Exception as e:
            print(f"Warning: Failed to load weather: {e}")
            bundle['coverage']['weather'] = False
        
        # Load curated places by category
        target_country = self._resolve_country_code(
            city_key,
            fallback_config=city_config.get("country_code"),
            city_row=bundle["city_row"],
        )
        categories = ['hospital', 'urgent_care', 'pharmacy', 'supermarket', 'convenience', 'convenience_store', 'rental_car']
        categories.append('airport')
        for cat in categories:
            places = self._load_curated_places(city_key, cat, target_country=target_country)
            bundle['curated_places'][cat] = places

        context_snapshot = bundle.get("context", {}) if isinstance(bundle.get("context"), dict) else {}
        context_facilities = (context_snapshot.get('hospitals') or []) + (context_snapshot.get('urgent_care') or [])
        if (
            not bundle['curated_places'].get('hospital')
            and not bundle['curated_places'].get('urgent_care')
            and context_facilities
        ):
            fallback = []
            for entry in context_facilities:
                name = entry.get("name", "")
                if not name:
                    continue
                fallback.append(
                    {
                        "name": name,
                        "address": entry.get("address") or entry.get("notes") or "",
                        "website": self._effective_website_value(entry),
                        "notes": entry.get("notes") or "",
                    }
                )
            bundle['curated_places']['hospital'] = fallback
        
        bundle['coverage']['hospitals'] = len(bundle['curated_places'].get('hospital', [])) > 0
        bundle['coverage']['pharmacies'] = len(bundle['curated_places'].get('pharmacy', [])) > 0
        bundle['coverage']['supermarkets'] = len(bundle['curated_places'].get('supermarket', [])) > 0
        
        # Load feed items using feed_item_cities join
        cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)
        try:
            # Get feed item IDs for this city
            mappings = self.datastore.client.table("feed_item_cities") \
                .select("feed_item_id") \
                .eq("city_key", city_key) \
                .execute()
            
            if mappings.data:
                item_ids = [m['feed_item_id'] for m in mappings.data]
                
                # Get feed items (fallback to fetched_at when published_at is missing)
                items_by_id: Dict[str, Dict] = {}
                response = (
                    self.datastore.client.table("feed_items")
                    .select("*")
                    .in_("id", item_ids)
                    .gte("published_at", cutoff.isoformat())
                    .order("published_at", desc=True)
                    .limit(200)  # Cap at 200 items
                    .execute()
                )
                for row in (response.data or []):
                    if row.get("id"):
                        items_by_id[row["id"]] = row

                # Fallback query for items without published_at
                if len(items_by_id) < 200:
                    fallback = (
                        self.datastore.client.table("feed_items")
                        .select("*")
                        .in_("id", item_ids)
                        .is_("published_at", "null")
                        .gte("fetched_at", cutoff.isoformat())
                        .order("fetched_at", desc=True)
                        .limit(200)
                        .execute()
                    )
                    for row in (fallback.data or []):
                        if row.get("id") and row["id"] not in items_by_id:
                            items_by_id[row["id"]] = row

                bundle['feed_items'] = list(items_by_id.values())
                bundle['coverage']['incidents'] = len(bundle['feed_items']) > 0
        except Exception as e:
            print(f"Warning: Failed to load feed items: {e}")
            bundle['coverage']['incidents'] = False
        
        # Load emergency contacts
        from emergency_numbers import normalize_emergency_contacts
        ctx = bundle.get('context', {})
        country_code = bundle['city_row'].get('country_code', '').strip().upper()
        resolved = resolve_emergency_numbers(city_config, ctx if isinstance(ctx, dict) else {}, country_code)
        bundle['emergency_contacts'] = normalize_emergency_contacts(resolved)
        
        return bundle
    
    def load_report_data(
        self,
        city_key: str,
        lookback_hours: int = 48,
        trip_profile: Optional[Dict] = None,
    ) -> ReportViewModel:
        """
        Load complete report data from database.
        
        Args:
            city_key: City identifier
            lookback_hours: Hours to look back for incidents
            trip_profile: Optional trip configuration
            
        Returns:
            ReportViewModel with all data loaded from DB
        """
        # DB-first city metadata (fallback to config if missing)
        city_config = config.CITIES.get(city_key, {})
        city_row = None
        try:
            client = getattr(self.datastore, "client", None)
            if client is not None:
                resp = (
                    client.table("cities")
                    .select("city_key,display_name,country_code,timezone,latitude,longitude,aliases,enabled")
                    .eq("city_key", city_key)
                    .limit(1)
                    .execute()
                )
                if resp.data:
                    city_row = resp.data[0]
        except Exception:
            city_row = None

        city_name = (city_row or {}).get("display_name") or city_config.get('name', city_key)
        
        # Initialize coverage tracker
        coverage = ReportCoverage()
        
        # Generate metadata
        now_utc = datetime.now(timezone.utc)
        generated_utc = now_utc.strftime('%Y-%m-%d %H:%M')
        report_id = f"{city_key}-{now_utc.strftime('%Y%m%d-%H%M')}"
        
        # Build destination string from config (already includes full name with country)
        # e.g., "Miami, Florida, United States", "Paris, France", "Lisbon, Portugal"
        destination = city_name
        
        # ====================================================================
        # Load Medical Facilities from curated_places
        # ====================================================================
        hospitals = self._load_medical_facilities(city_key, coverage)
        
        # ====================================================================
        # Load Transit from transit_snapshots
        # ====================================================================
        transit = self._load_transit(city_key, coverage)
        
        # ====================================================================
        # Load Weather from weather_forecasts
        # ====================================================================
        weather_summary = self._load_weather(city_key, coverage)
        
        # ====================================================================
        # Load Incidents from feed_items + feed_item_cities
        # ====================================================================
        incidents = self._load_incidents(city_key, lookback_hours, coverage)
        
        # ====================================================================
        # Load Supplies from curated_places
        # ====================================================================
        target_country = self._resolve_country_code(
            city_key,
            fallback_config=city_config.get("country_code"),
            city_row=city_row,
        )

        pharmacies = self._load_curated_places(city_key, 'pharmacy', target_country=target_country)
        supermarkets = self._load_curated_places(city_key, 'supermarket', target_country=target_country)
        convenience_stores = self._load_curated_places(city_key, 'convenience', target_country=target_country)
        rental_cars = self._load_curated_places(city_key, 'rental_car', target_country=target_country)
        airports = self._load_curated_places(city_key, 'airport', target_country=target_country)

        hospitals = self._fallback_list_from_previous_report(city_key, hospitals, key="hospitals")
        pharmacies = self._fallback_list_from_previous_report(
            city_key,
            pharmacies,
            key="pharmacies",
            nested_parent="supplies_services",
        )
        supermarkets = self._fallback_list_from_previous_report(
            city_key,
            supermarkets,
            key="supermarkets",
            nested_parent="supplies_services",
        )
        convenience_stores = self._fallback_list_from_previous_report(
            city_key,
            convenience_stores,
            key="convenience_stores",
            nested_parent="supplies_services",
        )
        rental_cars = self._fallback_list_from_previous_report(
            city_key,
            rental_cars,
            key="rental_cars",
            nested_parent="supplies_services",
        )
        airports = self._fallback_list_from_previous_report(city_key, airports, key="airports")

        validate_section_coverage(coverage, 'medical_facilities', hospitals, min_required=3)
        
        total_supplies = len(pharmacies) + len(supermarkets) + len(convenience_stores)
        supplies_min_required = 3
        if total_supplies == 0:
            context_snapshot = self.datastore.get_city_context(city_key)
            context = context_snapshot.context if context_snapshot and isinstance(context_snapshot.context, dict) else {}
            missing_categories = set(context.get("missing_categories") or [])
            if (
                str(context.get("coverage_status") or "").strip().lower() == "partial"
                and {"pharmacies", "supermarkets", "convenience_stores"}.issubset(missing_categories)
            ):
                # Do not flag false report partials when upstream context sources are
                # temporarily unavailable for all supply categories.
                supplies_min_required = 0

        validate_section_coverage(coverage, 'supplies', [1] * total_supplies, min_required=supplies_min_required)
        
        # ====================================================================
        # Load Emergency Contacts
        # ====================================================================
        emergency_contacts = self._load_emergency_contacts(city_key, city_config)
        
        # ====================================================================
        # Load Travel Advisory
        # ====================================================================
        advisory_city_config = dict(city_config or {})
        advisory_city_config["city_key"] = city_key
        advisory_city_config.setdefault("name", city_name)
        if not advisory_city_config.get("country_code"):
            advisory_city_config["country_code"] = (
                (bundle.get("city_row") or {}).get("country_code")
                if isinstance(bundle.get("city_row"), dict)
                else None
            )
        advisory_data = self._load_travel_advisory(advisory_city_config)
        
        # ====================================================================
        # Load Partner Links (Parking for US, etc.)
        # ====================================================================
        parking = self._load_parking_link(city_config)
        
        # ====================================================================
        # Load AI-generated content from city_reports (if exists)
        # ====================================================================
        ai_content = self._load_ai_generated_content(city_key)
        
        # ====================================================================
        # Compute coverage status
        # ====================================================================
        coverage_status = coverage.summary_message
        
        # ====================================================================
        # Build ReportViewModel
        # ====================================================================
        return ReportViewModel(
            city_key=city_key,
            city_name=city_name,
            destination=destination,
            generated_utc=generated_utc,
            report_id=report_id,
            coverage=coverage,
            coverage_status=coverage_status,
            hospitals=hospitals,
            transit=transit,
            weather_summary=weather_summary,
            incidents=incidents,
            pharmacies=pharmacies,
            supermarkets=supermarkets,
            convenience_stores=convenience_stores,
            rental_cars=rental_cars,
            airports=airports,
            emergency_contacts=emergency_contacts,
            travel_advisory=advisory_data.get('text'),
            advisory_level=advisory_data.get('level'),
            advisory_source=advisory_data.get('source'),
            travel_advisory_url=advisory_data.get('url'),
            parking=parking,
            top_actions=ai_content.get('top_actions', []),
            whats_happening=ai_content.get('whats_happening', []),
            risk_badges=ai_content.get('risk_badges', []),
            cues=ai_content.get('cues', []),
            opsec=ai_content.get('opsec'),
            trend_data=ai_content.get('trend_data'),
            trip_profile=trip_profile,
            driving_pack=ai_content.get('driving_pack'),
        )
    
    def _load_medical_facilities(self, city_key: str, coverage: ReportCoverage) -> List[Dict]:
        """Load hospitals and urgent care.

        Schema v3 stores the curated list (name/website/notes) in curated_places.
        Location fields (address/lat/lon) are sourced from city_context_snapshots.context JSON.
        """

        hospitals = self._filter_curated_place_status(
            self.datastore.get_curated_places(city_key, category='hospital')
        )
        urgent_care = self._filter_curated_place_status(
            self.datastore.get_curated_places(city_key, category='urgent_care')
        )
        self._apply_effective_website(hospitals)
        self._apply_effective_website(urgent_care)

        # Build a best-effort lookup from city_context snapshot
        context_snapshot = self.datastore.get_city_context(city_key)
        context = context_snapshot.context if context_snapshot else {}
        context_facilities = (context.get('hospitals') or []) + (context.get('urgent_care') or [])

        def norm_name(name: str) -> str:
            return ''.join(ch.lower() for ch in (name or '') if ch.isalnum() or ch.isspace()).strip()

        by_name = {}
        for f in context_facilities:
            n = norm_name(f.get('name', ''))
            if n:
                by_name[n] = f

        # Combine and format
        facilities = []
        for place in hospitals + urgent_care:
            name = place.get('name', '')
            ctx = by_name.get(norm_name(name), {})
            ctx_website = self._extract_context_website(ctx)
            website = self._effective_website_value(place) or ctx_website

            facility = {
                'name': name,
                'address': ctx.get('address', ''),
                'lat': ctx.get('lat'),
                'lon': ctx.get('lon'),
                'notes': place.get('notes'),
                'website': website,
            }
            facilities.append(facility)

        if not facilities and context_facilities:
            for entry in context_facilities:
                name = entry.get('name', '')
                if not name:
                    continue
                ctx_website = self._extract_context_website(entry)
                facilities.append(
                    {
                        'name': name,
                        'address': entry.get('address', ''),
                        'lat': entry.get('lat'),
                        'lon': entry.get('lon'),
                        'notes': entry.get('notes') or '',
                        'website': ctx_website or self._effective_website_value(entry),
                    }
                )
        elif len(facilities) < 3 and context_facilities:
            # Top up DB curated facilities with context snapshot entries when partial.
            seen_names = {
                norm_name(facility.get('name', ''))
                for facility in facilities
                if isinstance(facility, dict) and facility.get('name')
            }
            for entry in context_facilities:
                name = entry.get('name', '')
                normalized = norm_name(name)
                if not name or not normalized or normalized in seen_names:
                    continue
                ctx_website = self._extract_context_website(entry)
                facilities.append(
                    {
                        'name': name,
                        'address': entry.get('address', ''),
                        'lat': entry.get('lat'),
                        'lon': entry.get('lon'),
                        'notes': entry.get('notes') or '',
                        'website': ctx_website or self._effective_website_value(entry),
                    }
                )
                seen_names.add(normalized)
        
        # Update coverage
        min_required = 3
        missing_categories = set(context.get("missing_categories") or []) if isinstance(context, dict) else set()
        if (
            not facilities
            and str((context or {}).get("coverage_status") or "").strip().lower() == "partial"
            and "hospitals" in missing_categories
        ):
            # Avoid false partial status when context explicitly indicates temporary
            # upstream hospital source gaps.
            min_required = 0

        validate_section_coverage(coverage, 'medical_facilities', facilities, min_required=min_required)
        
        return facilities
    
    def _load_transit(self, city_key: str, coverage: ReportCoverage) -> List[Dict]:
        """Load transit providers from transit_snapshots."""
        snapshot = self.datastore.get_transit_snapshot(city_key)
        providers = []
        if snapshot and isinstance(snapshot.transit, dict):
            providers = snapshot.transit.get('providers', []) or []

        # Fallback to config if DB is missing
        if not providers:
            city_cfg = (config.CITIES.get(city_key) or {})
            systems = city_cfg.get("transit_systems") or []
            providers = [
                {
                    "name": (s or {}).get("name", ""),
                    "modes": (s or {}).get("modes", []) or [],
                    "website": (s or {}).get("website", "") or "",
                }
                for s in systems
                if isinstance(s, dict) and (s.get("name") or "").strip()
            ]

        # Update coverage
        validate_section_coverage(coverage, 'transit', providers, min_required=1)

        return providers
    
    def _load_weather(self, city_key: str, coverage: ReportCoverage) -> Optional[Dict]:
        """Load weather forecast from weather_forecasts table."""
        try:
            # Query latest weather forecast
            response = self.datastore.client.table("weather_forecasts") \
                .select("*") \
                .eq("city_key", city_key) \
                .order("fetched_at", desc=True) \
                .limit(1) \
                .execute()
            
            if not response.data:
                validate_section_coverage(coverage, 'weather', [], min_required=1)
                return None
            
            forecast = response.data[0]
            raw = forecast.get('raw', {})
            
            # Extract summary
            current = raw.get('current', {})
            daily = raw.get('daily', [])
            
            summary = {
                'current': f"{current.get('temperature', 'N/A')} - {current.get('condition', 'Unknown')}",
                'outlook': f"{len(daily)} day forecast available",
                'raw': raw,
            }
            
            validate_section_coverage(coverage, 'weather', [summary], min_required=1)
            return summary
            
        except Exception as e:
            print(f"Warning: Failed to load weather: {e}")
            validate_section_coverage(coverage, 'weather', [], min_required=1)
            return None
    
    def _load_incidents(self, city_key: str, lookback_hours: int, coverage: ReportCoverage) -> List[Dict]:
        """Load recent incidents from feed_items with relevance filtering."""
        try:
            items_data = self.datastore.get_relevant_city_feed_items(
                city_key=city_key,
                lookback_hours=lookback_hours,
                limit=50,
                min_score=0.0,
                exclude_source_keys=None,
                require_published_at_for_incidents=True,
            )
            
            if not items_data:
                # No incidents in the lookback window is a valid outcome, not missing data.
                validate_section_coverage(coverage, 'incidents', [], min_required=0)
                return []
            
            def _parse_ts(value: Optional[str]) -> Optional[datetime]:
                if not value:
                    return None
                text = str(value)
                if text.endswith("Z"):
                    text = text[:-1] + "+00:00"
                try:
                    dt = datetime.fromisoformat(text)
                except Exception:
                    return None
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            
            # Format incidents using effective_time (not "today" unless truly today)
            incidents = []
            for item in items_data:
                effective_time = item.get("_effective_time")
                if effective_time and not isinstance(effective_time, datetime):
                    effective_time = _parse_ts(effective_time)
                if not effective_time:
                    effective_time = (
                        _parse_ts(item.get("published_at"))
                        or _parse_ts(item.get("fetched_at"))
                        or datetime.min.replace(tzinfo=timezone.utc)
                    )
                # Compute recency using effective_time
                recency = None
                try:
                    if effective_time and effective_time.year > 1900:
                        today = datetime.now(timezone.utc).date()
                        item_date = effective_time.date()
                        delta = (item_date - today).days
                        if delta == 0:
                            recency = "today"
                        elif delta > 0:
                            recency = f"in {delta} day" if delta == 1 else f"in {delta} days"
                        else:
                            days = abs(delta)
                            recency = f"{days} day ago" if days == 1 else f"{days} days ago"
                except Exception:
                    recency = None
                
                # Use effective_time for date display
                date_str = effective_time.date().isoformat() if effective_time else ""
                
                incident = {
                    'date': date_str,
                    'recency': recency,
                    'type': 'News',
                    'impact': item.get('title', ''),
                    'where': '',
                }
                incidents.append(incident)
            
            validate_section_coverage(coverage, 'incidents', incidents, min_required=0)
            return incidents
            
        except Exception as e:
            print(f"Warning: Failed to load incidents: {e}")
            validate_section_coverage(coverage, 'incidents', [], min_required=1)
            return []
    
    def _load_curated_places(
        self,
        city_key: str,
        category: str,
        *,
        target_country: Optional[str] = None,
    ) -> List[Dict]:
        """Load curated places by category with provider-level dedup and website backfill."""

        places = self._filter_curated_place_status(
            self.datastore.get_curated_places(city_key, category=category)
        )

        if category in {"convenience", "convenience_store"}:
            alt_category = "convenience_store" if category == "convenience" else "convenience"
            alt_places = self._filter_curated_place_status(
                self.datastore.get_curated_places(city_key, category=alt_category)
            )
            if category == "convenience":
                places = list(alt_places) + list(places)
            else:
                places = list(places) + list(alt_places)

        if not places and category in {"pharmacy", "supermarket", "convenience", "convenience_store"}:
            context_snapshot = self.datastore.get_city_context(city_key)
            context = context_snapshot.context if context_snapshot and isinstance(context_snapshot.context, dict) else {}
            context_key = {
                "pharmacy": "pharmacies",
                "supermarket": "supermarkets",
                "convenience": "convenience_stores",
                "convenience_store": "convenience_stores",
            }.get(category, "")
            context_rows = context.get(context_key) if context_key and isinstance(context, dict) else []
            if isinstance(context_rows, list) and context_rows:
                places = []
                for entry in context_rows:
                    if not isinstance(entry, dict):
                        continue
                    name = str(entry.get("name") or "").strip()
                    if not name:
                        continue
                    places.append(
                        {
                            "name": name,
                            "notes": entry.get("address") or entry.get("notes") or "",
                            "website": self._extract_context_website(entry) or self._effective_website_value(entry),
                        }
                    )

        self._apply_effective_website(places)

        max_counts = {
            "pharmacy": 4,
            "supermarket": 4,
            "convenience": 4,
            "convenience_store": 4,
            "hospital": 4,
            "rental_car": 4,
        }
        max_count = max_counts.get(category)

        def _dedupe_key(row: Dict[str, Any]) -> tuple[str, str]:
            name = (row.get("name") or "").strip().casefold()
            website = self._effective_website_value(row).casefold()
            return (name, website)

        def _merge_must_include(
            base_rows: List[Dict[str, Any]],
            must_include_rows: List[Dict[str, Any]],
            limit: Optional[int],
        ) -> List[Dict[str, Any]]:
            seen = set()
            merged: List[Dict[str, Any]] = []
            for row in must_include_rows:
                key = _dedupe_key(row)
                if key in seen:
                    continue
                seen.add(key)
                merged.append(row)
            if limit is not None and len(merged) >= limit:
                return merged[:limit]
            for row in base_rows:
                key = _dedupe_key(row)
                if key in seen:
                    continue
                seen.add(key)
                merged.append(row)
                if limit is not None and len(merged) >= limit:
                    break
            return merged

        if category == "rental_car":
            from helpers.country_metadata import get_country_name_en
            from helpers.rental_car_links import (
                dedupe_rental_car_entries,
                detect_rental_car_provider,
                is_provider_url_allowed,
                resolve_rental_car_links_unvalidated,
            )

            target_country = (target_country or self._resolve_country_code(city_key)).upper()
            country_name_en = get_country_name_en(target_country)
            must_include_entries = [
                {
                    "name": p.get("name", ""),
                    "address": p.get("notes"),
                    "website": self._effective_website_value(p),
                }
                for p in places
                if p.get("name") and p.get("must_include")
            ]
            entries = [
                {
                    "name": p.get("name", ""),
                    "address": p.get("notes"),
                    "website": self._effective_website_value(p),
                }
                for p in places
            ]
            result = dedupe_rental_car_entries(entries, target_country, country_name_en)
            if result:
                result.sort(key=lambda r: r.get("name", "").lower())
                return _merge_must_include(result, must_include_entries, max_count)

            fallback_entries = list(entries)
            if not fallback_entries:
                context_snapshot = self.datastore.get_city_context(city_key)
                context = context_snapshot.context if context_snapshot else {}
                fallback_entries = [
                    {
                        "name": r.get("name", ""),
                        "address": r.get("address") or r.get("notes"),
                        "website": self._effective_website_value(r),
                    }
                    for r in (context.get("rental_cars") or [])
                    if isinstance(r, dict)
                ]

            cleaned: List[Dict[str, str]] = []
            for entry in fallback_entries:
                name = (entry.get("name") or "").strip()
                if not name:
                    continue
                website = self._effective_website_value(entry)
                provider = detect_rental_car_provider(name)
                if not provider or not is_provider_url_allowed(provider, website, target_country):
                    website = ""
                cleaned.append(
                    {
                        "name": name,
                        "address": entry.get("address") or entry.get("notes") or "",
                        "website": website,
                    }
                )

            # Final fallback for cities without curated/context rental data:
            # synthesize canonical providers for the destination country.
            if not cleaned:
                canonical_links = resolve_rental_car_links_unvalidated(target_country, country_name_en)
                for item in canonical_links:
                    website = str(item.get("url") or "").strip()
                    if not website:
                        continue
                    label = str(item.get("label") or item.get("provider") or "").strip()
                    if not label:
                        continue
                    cleaned.append(
                        {
                            "name": label,
                            "address": "",
                            "website": website,
                        }
                    )

            cleaned.sort(key=lambda r: r.get("name", "").lower())
            return _merge_must_include(cleaned, must_include_entries, max_count)

        if category == "airport":
            def _parse_airport_iata(notes: Optional[str]) -> str:
                text = (notes or "").strip()
                if not text:
                    return ""
                match = re.search(r"\biata[:\s]+([a-z0-9]{3})\b", text, re.IGNORECASE)
                if match:
                    return match.group(1).upper()
                if re.fullmatch(r"[a-z0-9]{3}", text, re.IGNORECASE):
                    return text.upper()
                return ""

            entries = [
                {
                    "name": p.get("name", ""),
                    "iata": _parse_airport_iata(p.get("notes")),
                    "website": self._effective_website_value(p),
                }
                for p in places
            ]

            if not entries:
                context_snapshot = self.datastore.get_city_context(city_key)
                context = context_snapshot.context if context_snapshot else {}
                entries = [
                    {
                        "name": a.get("name", ""),
                        "iata": (a.get("iata") or "").strip().upper(),
                        "website": self._effective_website_value(a),
                    }
                    for a in (context.get("airports") or [])
                    if isinstance(a, dict)
                ]
            if not entries:
                config_airports = (config.CITIES.get(city_key, {}) or {}).get("airports") or []
                entries = [
                    {
                        "name": a.get("name", ""),
                        "iata": (a.get("iata") or "").strip().upper(),
                        "website": self._effective_website_value(a),
                    }
                    for a in config_airports
                    if isinstance(a, dict)
                ]

            seen = set()
            result = []
            for entry in entries:
                name = (entry.get("name") or "").strip()
                if not name:
                    continue
                key = name.lower()
                if key in seen:
                    continue
                seen.add(key)
                result.append(
                    {
                        "name": name,
                        "iata": entry.get("iata", ""),
                        "website": self._effective_website_value(entry),
                    }
                )

            result.sort(key=lambda r: r.get("name", "").lower())
            return result[:3]

        chain_categories = {
            "pharmacy",
            "supermarket",
            "convenience",
            "convenience_store",
        }

        target_country = (target_country or self._resolve_country_code(city_key)).upper()

        must_include_entries = [
            {
                "name": p.get("name", ""),
                "address": p.get("notes"),
                "website": self._effective_website_value(p),
            }
            for p in places
            if p.get("name") and p.get("must_include")
        ]

        # Only run chain-style dedup for chain-like categories
        if category not in chain_categories:
            base_rows = [
                {
                    "name": p.get("name", ""),
                    "address": p.get("notes"),
                    "website": self._effective_website_value(p),
                }
                for p in places
            ]
            return _merge_must_include(base_rows, must_include_entries, max_count)

        GENERIC_PROVIDER_TOKENS = {
            "inc", "llc", "company", "co", "corp", "corporation", "ltd", "limited",
            "group", "sa", "ag", "gmbh", "plc", "holdings", "rent", "rental", "car",
            "auto", "vehicle", "rentacar", "rent-a-car",
        }

        def _provider_tokens_ascii(name: str) -> list[str]:
            text = (name or "").lower()
            text = re.sub(r"[^a-z0-9\s]", " ", text)
            tokens = [t for t in text.split() if t]
            return [t for t in tokens if t not in GENERIC_PROVIDER_TOKENS]

        def _provider_key(name: str) -> tuple[str, ...]:
            tokens = _provider_tokens_ascii(name)
            if tokens:
                return tuple(tokens)

            cleaned = unicodedata.normalize("NFKC", (name or "").strip().lower())
            cleaned = re.sub(r"\s+", " ", cleaned)
            if not cleaned:
                cleaned = "unknown"
            digest = hashlib.sha1(cleaned.encode("utf8")).hexdigest()[:12]
            return ("u", digest)

        def _domain_contains_tokens(url: str, tokens: list[str]) -> bool:
            from storage.normalize import normalize_domain
            import tldextract
            normalized = normalize_domain(url)
            if not normalized:
                return False
            ext = tldextract.extract(normalized)
            base = ext.domain or ""
            if not base:
                return False
            base = base.replace("-", "")
            return all(t.replace("-", "") in base for t in tokens)

        def _is_global_tld(url: str) -> bool:
            from storage.normalize import normalize_domain
            import tldextract
            normalized = normalize_domain(url)
            if not normalized:
                return False
            ext = tldextract.extract(normalized)
            suffix = (ext.suffix or "").lower()
            return suffix in {"com", "org", "net"}

        def _score(row: Dict[str, Any]) -> tuple:
            website = bool(self._effective_website_value(row))
            notes = bool((row or {}).get("notes"))
            address = bool((row or {}).get("address") or (row or {}).get("notes"))
            phone = bool((row or {}).get("phone"))
            return (
                1 if website else 0,
                1 if notes else 0,
                1 if address else 0,
                1 if phone else 0,
            )

        # Best rows for this city
        best_by_provider: Dict[Tuple[str, ...], Dict[str, Any]] = {}
        for row in places:
            key = _provider_key(row.get("name", ""))
            current = best_by_provider.get(key)
            if current is None or _score(row) > _score(current):
                best_by_provider[key] = row

        # Build cross-city website fallback map
        fallback_by_provider: Dict[Tuple[str, ...], Dict[str, Any]] = {}
        try:
            all_places = self.datastore.get_curated_places_all(category=category)
        except Exception:
            all_places = []

        for row in all_places:
            tokens = _provider_tokens_ascii(row.get("name", ""))
            if not tokens or not self._effective_website_value(row):
                continue
            # Only reuse if domain contains all provider tokens and is not an aggregator.
            if not _domain_contains_tokens(self._effective_website_value(row), tokens):
                continue
            from storage.normalize import is_aggregator_domain
            if is_aggregator_domain(self._effective_website_value(row)):
                continue

            source_country = self._resolve_country_code(row.get("city_key", ""))
            if not (_is_global_tld(self._effective_website_value(row)) or (source_country and target_country and source_country == target_country)):
                continue
            key = tuple(tokens)
            current = fallback_by_provider.get(key)
            if current is None or _score(row) > _score(current):
                fallback_by_provider[key] = row

        # Backfill missing websites from other cities
        for key, row in best_by_provider.items():
            if self._effective_website_value(row):
                continue
            if key and key[0] == "u":
                continue
            fallback = fallback_by_provider.get(key)
            if fallback and self._effective_website_value(fallback):
                row["website"] = self._effective_website_value(fallback)
                print(f"  INFO Fallback website filled for '{row.get('name','')}' -> {self._effective_website_value(fallback)}")

        # Return sorted list (stable ordering for predictability)
        result = [
            {
                "name": row.get("name", ""),
                "address": row.get("notes"),
                "website": self._effective_website_value(row),
            }
            for row in best_by_provider.values()
        ]
        result.sort(key=lambda r: r.get("name", "").lower())
        
        # Prefer entries with websites for chain-like categories
        if category in {"supermarket", "convenience", "convenience_store"}:
            # Sort by website presence (descending) then by name (ascending)
            result.sort(
                key=lambda r: (
                    not bool(r.get("website")),  # websites first (False sorts before True)
                    r.get("name", "").lower()    # then alphabetically by name
                )
            )

        return _merge_must_include(result, must_include_entries, max_count)
    
    def _load_emergency_contacts(self, city_key: str, city_config: Dict) -> Dict:
        """Load emergency contacts for city (DB-first).

        Source-of-truth: city_context_snapshots.context["emergency_numbers"] or ["emergency_contacts"].
        """

        snapshot = self.datastore.get_city_context(city_key)
        ctx = snapshot.context if snapshot and isinstance(snapshot.context, dict) else {}

        from emergency_numbers import normalize_emergency_contacts
        country_code = (city_config.get('country_code') or '').strip().upper()
        resolved = resolve_emergency_numbers(city_config, ctx if isinstance(ctx, dict) else {}, country_code)
        return normalize_emergency_contacts(resolved)
    
    def _load_travel_advisory(self, city_config: Dict) -> Dict:
        """Load official travel advisory data from datastore feed items."""

        advisory_empty = {"text": None, "level": None, "source": None, "url": None}

        def _safe_text(value: Any) -> str:
            if value is None:
                return ""
            return str(value).strip()

        def _normalize_text(value: str) -> str:
            lowered = (value or "").lower().strip()
            if not lowered:
                return ""
            normalized = unicodedata.normalize("NFKD", lowered)
            ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
            return re.sub(r"\s+", " ", ascii_text)

        def _slugify(value: str) -> str:
            slug = re.sub(r"[^a-z0-9]+", "-", _normalize_text(value))
            return slug.strip("-")

        def _country_name_from_city_name(value: str) -> str:
            text = _safe_text(value)
            if "," in text:
                return text.split(",")[-1].strip()
            return ""

        def _extract_advisory_text(item: Any) -> str:
            raw = item.raw if isinstance(getattr(item, "raw", None), dict) else {}
            summary = _safe_text(getattr(item, "summary", ""))
            if summary:
                return format_advisory_text(summary)

            advisory_text = _safe_text(raw.get("advisory_text"))
            recent_updates = _safe_text(raw.get("recent_updates"))
            if advisory_text and recent_updates:
                return format_advisory_text(f"{advisory_text}\n\n{recent_updates}")
            if advisory_text:
                return format_advisory_text(advisory_text)

            for key in ("summary", "content_html", "content"):
                candidate = _safe_text(raw.get(key))
                if candidate:
                    return format_advisory_text(candidate)
            if recent_updates:
                return format_advisory_text(recent_updates)
            return ""

        def _extract_level_from_text(text: str, *, canada: bool) -> Optional[int]:
            lowered = _normalize_text(text)
            match = re.search(r"\blevel\s*([0-4])\b", lowered)
            if match:
                return int(match.group(1))

            if canada:
                if "avoid all travel" in lowered:
                    return 3
                if "avoid non essential travel" in lowered or "avoid non-essential travel" in lowered:
                    return 2
                if "high degree of caution" in lowered:
                    return 1
                if "normal security precautions" in lowered:
                    return 0
            else:
                if "do not travel" in lowered:
                    return 4
                if "reconsider travel" in lowered:
                    return 3
                if "exercise increased caution" in lowered:
                    return 2
                if "exercise normal precautions" in lowered:
                    return 1
            return None

        def _level_to_int(value: Any, *, canada: bool, fallback_text: str = "") -> Optional[int]:
            if isinstance(value, (int, float)):
                return int(value)
            as_text = _safe_text(value)
            if as_text:
                match = re.search(r"\d+", as_text)
                if match:
                    return int(match.group(0))
                parsed = _extract_level_from_text(as_text, canada=canada)
                if parsed is not None:
                    return parsed
            if fallback_text:
                return _extract_level_from_text(fallback_text, canada=canada)
            return None

        def _effective_timestamp(item: Any) -> float:
            candidate = getattr(item, "published_at", None) or getattr(item, "fetched_at", None)
            if isinstance(candidate, datetime):
                if candidate.tzinfo is None:
                    candidate = candidate.replace(tzinfo=timezone.utc)
                return candidate.timestamp()
            return 0.0

        def _pick_best_summary(items: List[Any]) -> Optional[Any]:
            if not items:
                return None
            return max(items, key=lambda item: (_effective_timestamp(item), len(_extract_advisory_text(item))))

        country_code = _safe_text((city_config or {}).get("country_code")).upper()
        city_key = _safe_text((city_config or {}).get("city_key"))
        city_name = _safe_text((city_config or {}).get("name") or (city_config or {}).get("display_name"))

        if not country_code and city_key:
            country_code = self._resolve_country_code(city_key)

        # Fallback to cities table lookup when city_key is unavailable in config payload.
        if not country_code and city_name:
            try:
                client = getattr(self.datastore, "client", None)
                if client is not None:
                    resp = (
                        client.table("cities")
                        .select("country_code")
                        .eq("display_name", city_name)
                        .limit(1)
                        .execute()
                    )
                    if resp.data:
                        country_code = _safe_text((resp.data[0] or {}).get("country_code")).upper()
            except Exception:
                country_code = ""

        if not country_code:
            return advisory_empty

        country_name_from_city = _country_name_from_city_name(city_name)
        if not country_name_from_city:
            try:
                from helpers.country_metadata import get_country_name_en

                country_name_from_city = _safe_text(get_country_name_en(country_code))
            except Exception:
                country_name_from_city = ""

        # US destinations use Government of Canada advisories.
        if country_code == "US":
            try:
                feed_items = self.datastore.get_feed_items(source_key="canada_travel_advisories")
            except Exception:
                feed_items = []
            if not feed_items:
                return advisory_empty

            us_items = []
            for item in feed_items:
                raw = item.raw if isinstance(getattr(item, "raw", None), dict) else {}
                if _safe_text(raw.get("country_code")).upper() == "US":
                    us_items.append(item)

            best = _pick_best_summary(us_items or feed_items)
            if not best:
                return advisory_empty

            raw = best.raw if isinstance(getattr(best, "raw", None), dict) else {}
            text = _extract_advisory_text(best)
            level_value = _level_to_int(
                raw.get("advisory_state"),
                canada=True,
                fallback_text=f"{_safe_text(best.title)} {text}",
            )
            canada_level_map = {
                0: "Exercise normal security precautions",
                1: "Exercise a high degree of caution",
                2: "Avoid non essential travel",
                3: "Avoid all travel",
            }
            if level_value is not None:
                level_value = max(0, min(level_value, 3))
            level_label = canada_level_map.get(level_value) if level_value is not None else None

            return {
                "text": text or None,
                "level": level_label,
                "source": "Government of Canada Travel Advisories",
                "url": "https://travel.gc.ca/destinations/united-states",
            }

        # Non-US destinations use U.S. Department of State advisories.
        try:
            feed_items = self.datastore.get_feed_items(source_key="us_travel_advisories")
        except Exception:
            feed_items = []
        if not feed_items:
            return advisory_empty

        target_country = _normalize_text(country_name_from_city)
        aliases = {target_country} if target_country else set()
        alias_map = {
            "czech republic": {"czechia"},
            "czechia": {"czech republic"},
            "united states": {"usa", "u s", "u.s."},
            "united kingdom": {"uk", "great britain"},
            "russian federation": {"russia"},
            "russia": {"russian federation"},
        }
        for alias in list(aliases):
            aliases.update(alias_map.get(alias, set()))

        matches: List[Tuple[int, float, int, Any]] = []
        for item in feed_items:
            raw = item.raw if isinstance(getattr(item, "raw", None), dict) else {}
            raw_country = _normalize_text(_safe_text(raw.get("country_name")))
            title_norm = _normalize_text(_safe_text(getattr(item, "title", "")))

            score = 0
            if raw_country and raw_country in aliases:
                score += 2
            if aliases and any(alias and alias in title_norm for alias in aliases):
                score += 1
            if score > 0:
                matches.append((score, _effective_timestamp(item), len(_extract_advisory_text(item)), item))

        if matches:
            matches.sort(key=lambda row: (row[0], row[1], row[2]), reverse=True)
            best = matches[0][3]
        else:
            best = _pick_best_summary(feed_items)

        if not best:
            return advisory_empty

        raw = best.raw if isinstance(getattr(best, "raw", None), dict) else {}
        text = _extract_advisory_text(best)
        level_value = _level_to_int(
            raw.get("advisory_level"),
            canada=False,
            fallback_text=f"{_safe_text(best.title)} {text}",
        )
        state_level_map = {
            1: "Level 1: Exercise Normal Precautions",
            2: "Level 2: Exercise Increased Caution",
            3: "Level 3: Reconsider Travel",
            4: "Level 4: Do not travel",
        }
        if level_value is not None:
            level_value = max(1, min(level_value, 4))
        level_label = state_level_map.get(level_value) if level_value is not None else None

        country_slug = _slugify(_safe_text(raw.get("country_slug")))
        # UAE uses a non-standard State Dept URL (same override as travel_agent.py)
        _COUNTRY_ADVISORY_URL_MAP = {
            "united-arab-emirates": "https://travel.state.gov/content/travel/en/traveladvisories/traveladvisories/united-arab-emirates-travel-advisory.html",
        }
        advisory_url = (
            _COUNTRY_ADVISORY_URL_MAP.get(
                country_slug,
                f"https://travel.state.gov/en/international-travel/travel-advisories/{country_slug}.html",
            )
            if country_slug
            else _safe_text(getattr(best, "url", "")) or None
        )

        return {
            "text": text or None,
            "level": level_label,
            "source": "U.S. Department of State Travel Advisories",
            "url": advisory_url,
        }
    
    def _load_parking_link(self, city_config: Dict) -> Optional[Dict]:
        """
        Load SpotHero parking links for US cities.

        This returns a hardcoded pair of SpotHero referral links for US cities
        and intentionally does NOT consult the database or environment variables.

        Non-US cities: Returns None (no parking section)
        """
        country_code = city_config.get('country_code', '').strip().upper()

        # Non-US cities don't get parking section
        if country_code != 'US':
            return None

        # Always return the hardcoded parking links for US cities.
        # Keep backward-compatible `url` and `cta_text` fields for older renderers.
        links = getattr(config, 'SPOTHERO_PARKING_LINKS', None) or []
        first = links[0] if links and len(links) > 0 else None

        return {
            'provider': '',
            'title': 'Parking',
            'description': 'Reserve parking in advance near your destination. Compare garages and lots and lock in a spot.',
            'links': links,
            'cta_text': (first.get('label') if first else ""),
            'url': (first.get('url') if first else ""),
        }
    
    def _load_ai_generated_content(self, city_key: str) -> Dict:
        """Load AI-generated content from city_reports if available."""
        report = self.datastore.get_city_report(city_key)
        
        if not report:
            return {}
        
        return report.get('report_data', {})
