#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Supabase Data Sync Script (Schema v2)

Fetches and stores data from all sources into Supabase:
- Local RSS feeds (Local 10 Miami)
- Travel advisories (Canada)
- GDELT news (geolocation-based)
- NWS weather alerts (US cities only)
- Open-Meteo forecasts
- City context (hospitals, transit)

Supports different sync modalities:
- Full sync: All sources (feeds + city context)
- Fast sync: --skip-context (feeds only, 10x faster)
- Context-only: --context-only (hospitals/transit only)
- Reset: --reset (truncate runtime tables + reseed from config)
"""

import argparse
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Dict, Any

# Fix Windows console encoding issues
if sys.platform == 'win32' and hasattr(sys.stdout, 'buffer'):
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import config
from news_relevance import compute_travel_relevance, parse_published_at
from storage import get_datastore
from storage.base import FeedItem
from storage.base import TransitSnapshot
from city_context import CityContextTool
from storage.normalize import normalize_domain, is_aggregator_domain
from emergency_numbers import get_emergency_contacts
from storage.base import CityContext
from helpers.country_metadata import get_country_name_en
from helpers.rental_car_links import resolve_rental_car_links, detect_rental_car_provider

# Ingest-time relevance thresholds (sourced from config for consistency)
INGEST_MIN_SCORE = getattr(config, "INGEST_MIN_SCORE", 0.5)
INGEST_MIN_KEYWORDS = getattr(config, "INGEST_MIN_KEYWORDS", 2)
INGEST_MIN_STRONG = getattr(config, "INGEST_MIN_STRONG", 1)

_CURATED_PLACE_SEEDS_CACHE: Dict[str, Dict[str, List[Dict]]] | None = None


def load_curated_place_seeds() -> Dict[str, Dict[str, List[Dict]]]:
    """Load curated place seeds once; return empty dict on error."""
    global _CURATED_PLACE_SEEDS_CACHE
    if _CURATED_PLACE_SEEDS_CACHE is not None:
        return _CURATED_PLACE_SEEDS_CACHE

    seeds_path = project_root / "config_data" / "curated_places_seed_websites.json"
    try:
        if not seeds_path.exists():
            print(f"   WARN Seed file missing: {seeds_path}")
            _CURATED_PLACE_SEEDS_CACHE = {}
            return _CURATED_PLACE_SEEDS_CACHE
        import json

        payload = json.loads(seeds_path.read_text(encoding="utf-8"))
        cities = payload.get("cities", {}) if isinstance(payload, dict) else {}
        if not isinstance(cities, dict):
            raise ValueError("Invalid seeds format")
        _CURATED_PLACE_SEEDS_CACHE = cities
        return _CURATED_PLACE_SEEDS_CACHE
    except Exception as exc:
        print(f"   WARN Failed to load curated place seeds: {type(exc).__name__}")
        _CURATED_PLACE_SEEDS_CACHE = {}
        return _CURATED_PLACE_SEEDS_CACHE


def _wikimedia_enrichment_enabled(city_key: str) -> bool:
    normalized = (city_key or "").strip().lower()
    if not normalized:
        return False
    try:
        from wikimedia_enrichment import is_wikimedia_enrichment_enabled
        return is_wikimedia_enrichment_enabled(normalized)
    except Exception:
        return False


def sync_seeded_curated_places(city_key: str, datastore) -> None:
    """Upsert curated_places from seed file for a city (authoritative)."""
    from urllib.parse import urlparse

    seeds_by_city = load_curated_place_seeds()
    seeds_for_city = seeds_by_city.get(city_key, {}) if isinstance(seeds_by_city, dict) else {}
    if not isinstance(seeds_for_city, dict) or not seeds_for_city:
        return

    category_map = {
        "pharmacies": "pharmacy",
        "supermarkets": "supermarket",
        "convenience_stores": "convenience_store",
        "hospitals": "hospital",
        "rental_cars": "rental_car",
    }

    def _normalize_seed_website(raw: object) -> str | None:
        text = str(raw or "").strip()
        if not text:
            return None
        if "://" not in text:
            text = f"https://{text}"
        parsed = urlparse(text)
        if parsed.scheme not in {"http", "https"}:
            return None
        if not parsed.netloc:
            return None
        normalized = parsed._replace(
            scheme=parsed.scheme.lower(),
            netloc=parsed.netloc.lower(),
        ).geturl()
        return normalized

    for seed_key, category in category_map.items():
        entries = seeds_for_city.get(seed_key)
        if not isinstance(entries, list):
            continue

        if hasattr(datastore, "client"):
            try:
                datastore.client.table("curated_places") \
                    .delete() \
                    .eq("city_key", city_key) \
                    .eq("category", category) \
                    .execute()
            except Exception as exc:
                print(f"   WARN Seed reset failed for {city_key}/{category}: {exc}")

        for entry in entries:
            if not isinstance(entry, dict):
                continue
            name = (entry.get("name") or "").strip()
            if not name:
                continue
            website = _normalize_seed_website(entry.get("website"))
            if not website:
                continue
            datastore.upsert_curated_place(
                city_key=city_key,
                category=category,
                name=name,
                website=website,
                notes=None,
                must_include=True,
                website_canonical=website,
                website_source="manual",
                website_status="ok",
                website_verified_at=datetime.now(timezone.utc).isoformat(),
                website_verification_reason="seed",
            )


def _upsert_transit_from_config(city_key: str, city_config: Dict, datastore, *, force: bool = False) -> bool:
    """Populate transit_snapshots using config.CITIES[city_key]['transit_systems'].

    Report loader expects transit_snapshots.transit = {'providers': [...]}.
    """
    try:
        # Respect DB-owned values: only write if missing or forced.
        existing = None
        try:
            existing = datastore.get_transit_snapshot(city_key)
        except Exception:
            existing = None

        if not force and existing and isinstance(getattr(existing, "transit", None), dict):
            existing_providers = existing.transit.get("providers")
            if isinstance(existing_providers, list) and len(existing_providers) > 0:
                return True

        systems = city_config.get("transit_systems") or []
        providers = []
        for system in systems:
            name = (system or {}).get("name")
            if not name:
                continue
            providers.append(
                {
                    "name": name,
                    "modes": (system or {}).get("modes", []),
                    "website": (system or {}).get("website", ""),
                }
            )

        if not providers:
            return True

        snapshot = TransitSnapshot(
            city_key=city_key,
            transit={"providers": providers},
            fetched_at=datetime.now(timezone.utc),
        )
        return bool(datastore.upsert_transit_snapshot(snapshot))
    except Exception as e:
        print(f"   âš  Failed to upsert transit snapshot from config: {e}")
        return False


def _upsert_emergency_numbers_into_city_context(
    city_key: str,
    city_config: Dict,
    datastore,
    *,
    force: bool = False,
) -> bool:
    """Ensure city_context_snapshots.context has emergency numbers/contacts.

    Source-of-truth is the DB snapshot. We only write if missing or forced.
    """
    try:
        existing_obj = None
        try:
            existing_obj = datastore.get_city_context(city_key)
        except Exception:
            existing_obj = None

        existing_ctx = existing_obj.context if existing_obj and isinstance(existing_obj.context, dict) else {}
        if not isinstance(existing_ctx, dict):
            existing_ctx = {}

        if not force:
            if isinstance(existing_ctx.get("emergency_numbers"), dict) or isinstance(
                existing_ctx.get("emergency_contacts"), dict
            ):
                return True

        # Prefer city_config country code, then resolved_location country_code if present.
        country_code = (city_config.get("country_code") or "").strip().upper()
        resolved = existing_ctx.get("resolved_location") if isinstance(existing_ctx.get("resolved_location"), dict) else None
        if not country_code and resolved:
            country_code = (resolved.get("country_code") or "").strip().upper()

        contacts = get_emergency_contacts(country_code)
        existing_ctx["emergency_numbers"] = contacts
        existing_ctx["emergency_contacts"] = contacts

        # If we don't have a context snapshot yet (or it was non-dict), still upsert minimal.
        if "resolved_location" not in existing_ctx:
            lat = city_config.get("latitude")
            lon = city_config.get("longitude")
            existing_ctx["resolved_location"] = {
                "lat": lat,
                "lon": lon,
                "country_code": country_code,
                "display_name": city_config.get("name"),
            }

        ctx_obj = CityContext(
            city_key=city_key,
            context=existing_ctx,
            fetched_at=datetime.now(timezone.utc),
        )
        return bool(datastore.upsert_city_context(ctx_obj))
    except Exception as e:
        print(f"   âš  Failed to upsert emergency numbers into city context: {e}")
        return False


def _quality_gate_curated_place(
    city_key: str,
    category: str,
    name: str,
    website: str | None,
    city_lat: float | None,
    city_lon: float | None,
) -> tuple[str | None, str, str, str | None]:
    try:
        from helpers.curated_places_quality_gate import quality_gate_curated_place
        return quality_gate_curated_place(
            city_key=city_key,
            category=category,
            name=name,
            website=website,
            city_lat=city_lat,
            city_lon=city_lon,
        )
    except Exception as exc:
        reason = f"quality_gate_error:{type(exc).__name__}"
        return website, "warn", reason, "osm"


def _sync_curated_places_from_context(city_key: str, context: Dict, datastore) -> int:
    """Mirror a subset of city_context snapshot into curated_places.

    curated_places is the single source of truth for the report/UI for these categories.
    Location fields stay in city_context_snapshots; we store name/website/notes here.
    """
    import re
    from helpers.store_chain_canonical import canonicalize_store_website

    def _as_list(value):
        return value if isinstance(value, list) else []

    # Map city_context keys -> curated_places categories used by report_loader
    category_map = {
        "hospitals": "hospital",
        "urgent_care": "urgent_care",
        "pharmacies": "pharmacy",
        "supermarkets": "supermarket",
        "convenience_stores": "convenience",
        "rental_cars": "rental_car",
        "airports": "airport",
    }
    medical_categories = {"hospital", "urgent_care"}
    require_website_categories = {
        "hospital",
        "urgent_care",
        "pharmacy",
        "supermarket",
        "convenience",
        "airport",
    }
    promotion_limits = {
        "hospital": 3,
        "urgent_care": 3,
        "pharmacy": 3,
        "supermarket": 3,
        "convenience": 3,
        "rental_car": 3,
        "airport": 3,
    }
    wikimedia_enabled = _wikimedia_enrichment_enabled(city_key)
    
    # Compute country_code for this city (needed for rental car canonicalization)
    country_code = None
    if city_key in config.CITIES:
        country_code = config.CITIES[city_key].get("country_code")
    if not country_code and "resolved_location" in context:
        country_code = context["resolved_location"].get("country_code")
    country_code = (country_code or "").strip().upper()
    country_name_en = get_country_name_en(country_code)
    city_lat = None
    city_lon = None
    if city_key in config.CITIES:
        city_lat = config.CITIES[city_key].get("latitude")
        city_lon = config.CITIES[city_key].get("longitude")
    if (city_lat is None or city_lon is None) and "resolved_location" in context:
        resolved = context.get("resolved_location") or {}
        if isinstance(resolved, dict):
            city_lat = resolved.get("lat", city_lat)
            city_lon = resolved.get("lon", city_lon)

    def _normalize_special_url(raw: str) -> str | None:
        from urllib.parse import urlparse

        text = (raw or "").strip()
        if not text:
            return None
        parsed = urlparse(text if "://" in text else f"https://{text}")
        host = (parsed.netloc or "").strip().lower()
        if not host:
            return None
        host = host.split(":")[0]
        if not any(host.endswith(suffix) for suffix in ("comunidad.madrid", "madrid.org")):
            return None
        path = (parsed.path or "").strip()
        if not path or path == "/":
            return f"https://{host}"
        return f"https://{host}{path}".rstrip("/")

    def _collapse_www_prefix(host: str) -> str:
        return re.sub(r"^(?:www\.){2,}", "www.", host.lower())

    def _name_tokens(value: str | None) -> list[str]:
        if not value:
            return []
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
        tokens = re.split(r"[\W_]+", value.lower())
        return [t for t in tokens if t and t not in stopwords and len(t) >= 4]

    def _normalize_medical_url(raw: str, facility_name: str | None = None) -> str | None:
        from urllib.parse import urlparse

        text = (raw or "").strip()
        if not text:
            return None
        parsed = urlparse(text if "://" in text else f"https://{text}")
        host = (parsed.netloc or parsed.hostname or "").strip()
        if not host:
            return None
        if "@" in host:
            host = host.split("@", 1)[1]
        host = host.split(":")[0]
        host = _collapse_www_prefix(host)
        path = parsed.path or ""
        if path and facility_name:
            tokens = _name_tokens(facility_name)
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

    def _normalize_rental_url(raw: str) -> str | None:
        from urllib.parse import urlparse

        text = (raw or "").strip()
        if not text:
            return None
        parsed = urlparse(text if "://" in text else f"https://{text}")
        host = (parsed.netloc or parsed.hostname or "").strip()
        if not host:
            return None
        if "@" in host:
            host = host.split("@", 1)[1]
        host = host.split(":")[0]
        host = _collapse_www_prefix(host)
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

    def _normalize_airport_url(raw: str) -> str | None:
        return _normalize_rental_url(raw)

    def _is_nhs_directory_url(raw: str) -> bool:
        from urllib.parse import urlparse

        text = (raw or "").strip()
        if not text:
            return False
        parsed = urlparse(text if "://" in text else f"https://{text}")
        host = (parsed.netloc or parsed.hostname or "").lower().strip()
        if "@" in host:
            host = host.split("@", 1)[1]
        host = host.split(":")[0]
        path = (parsed.path or "").lower()
        if host in {"www.nhs.uk", "nhs.uk"}:
            if not path or path == "/":
                return True
            if path.startswith("/services") or path.startswith("/service-search") or "/service-search" in path:
                return True
        return False

    def _normalize_website(value: str | None, curated_category: str, facility_name: str | None = None) -> str | None:
        text = _normalize_text(value)
        if not text:
            return None
        if _is_nhs_directory_url(text):
            return None
        special = _normalize_special_url(text)
        if special:
            text = special
        if curated_category == "rental_car":
            return _normalize_rental_url(text)
        if curated_category == "airport":
            return _normalize_airport_url(text)
        if curated_category in medical_categories:
            return _normalize_medical_url(text, facility_name)
        normalized = normalize_domain(text)
        if normalized:
            normalized = normalized.strip().rstrip("/")
        return normalized or None

    def _extract_website(item: Dict, curated_category: str) -> str | None:
        """Extract official website from an OSM-derived item.

        Priority: contact:website, website, url, contact:url, operator:website. Prefer item['website'] if present.
        Normalization varies by category: hospitals/urgent care preserve subdomains and paths, others canonicalize
        to a registrable domain when appropriate.
        """

        candidates: list[str] = []

        direct = (item.get("website") or "").strip()
        if direct:
            candidates.append(direct)

        tags = item.get("tags")
        if isinstance(tags, dict):
            for key in ("contact:website", "website", "url", "contact:url", "operator:website"):
                val = (tags.get(key) or "").strip()
                if val:
                    candidates.append(val)

        for raw in candidates:
            normalized = _normalize_website(raw, curated_category, item.get("name"))
            if not normalized:
                continue
            if is_aggregator_domain(normalized):
                continue
            return normalized

        return None

    def _normalize_name(value: str | None) -> str:
        return (value or "").strip()

    def _normalize_text(value: str | None) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text if text else None

    def _normalize_name_key(value: str | None) -> str:
        text = (value or "").strip()
        if not text:
            return ""
        text = re.sub(r"\s+", " ", text)
        return text.casefold()

    def _normalize_bool(value: bool | None) -> bool | None:
        if value is None:
            return None
        return bool(value)

    def _normalize_status(value: str | None) -> str | None:
        text = _normalize_text(value)
        return text.lower() if text else None

    def _normalize_source(value: str | None) -> str | None:
        text = _normalize_text(value)
        return text.lower() if text else None

    def _normalize_and_validate_website(raw: str | None, curated_category: str, name: str | None) -> str | None:
        normalized = _normalize_website(raw, curated_category, name)
        if not normalized:
            return None
        if is_aggregator_domain(normalized):
            return None
        return normalized

    existing_cache = {}
    if hasattr(datastore, "prefetch_curated_places"):
        try:
            existing_cache = datastore.prefetch_curated_places(city_key) or {}
        except Exception:
            existing_cache = {}
    elif hasattr(datastore, "get_curated_places"):
        try:
            existing_rows = datastore.get_curated_places(city_key)
            existing_cache = {
                (row.get("category"), _normalize_name(row.get("name"))): row
                for row in (existing_rows or [])
                if row.get("category") and _normalize_name(row.get("name"))
            }
        except Exception:
            existing_cache = {}

    seeds_by_city = load_curated_place_seeds()
    seeds_for_city = seeds_by_city.get(city_key, {}) if isinstance(seeds_by_city, dict) else {}

    upserted = 0
    for ctx_key, curated_category in category_map.items():
        seed_context_keys = {"supermarkets", "pharmacies", "convenience_stores", "hospitals"}
        deny = getattr(config, "CURATED_PLACE_NAME_DENYLIST", {}).get(curated_category, set())
        if deny and hasattr(datastore, "client"):
            for bad in deny:
                try:
                    datastore.client.table("curated_places") \
                        .delete() \
                        .eq("city_key", city_key) \
                        .eq("category", curated_category) \
                        .ilike("name", bad) \
                        .execute()
                except Exception:
                    pass

        if ctx_key in seed_context_keys:
            seed_entries = _as_list((seeds_for_city or {}).get(ctx_key))
            discovered = _as_list(context.get(ctx_key))
            ordered_keys: list[str] = []
            by_key: Dict[str, Dict[str, object]] = {}

            def _add_candidate(name: str, website: str | None, source: str) -> None:
                key = _normalize_name_key(name)
                if not key:
                    return
                if not website and not wikimedia_enabled:
                    return
                if key in by_key:
                    if not by_key[key].get("website") and website:
                        by_key[key]["website"] = website
                        by_key[key]["source"] = source
                    return
                by_key[key] = {
                    "name": name,
                    "website": website,
                    "source": source,
                }
                ordered_keys.append(key)

            for entry in seed_entries:
                if not isinstance(entry, dict):
                    continue
                name = (entry.get("name") or "").strip()
                if not name:
                    continue
                raw = (entry.get("website") or "").strip()
                normalized = _normalize_and_validate_website(raw, curated_category, name)
                _add_candidate(name, normalized, "seed")

            for item in discovered:
                if not isinstance(item, dict):
                    continue
                name = (item.get("name") or "").strip()
                if not name:
                    continue
                normalized = _extract_website(item, curated_category)
                _add_candidate(name, normalized, "discovered")

            if wikimedia_enabled and ordered_keys:
                try:
                    from wikimedia_enrichment import enrich_places_with_wikimedia

                    candidates = [by_key[key] for key in ordered_keys if key in by_key]
                    missing = [
                        entry
                        for entry in candidates
                        if not (entry.get("website") or "").strip()
                    ]
                    if missing:
                        enrich_places_with_wikimedia(city_key, candidates)
                        for entry in candidates:
                            if not (entry.get("website") or "").strip():
                                candidate_url = (
                                    entry.get("wikimedia_candidate_website_final")
                                    or entry.get("wikimedia_candidate_website")
                                )
                                if candidate_url:
                                    normalized = _normalize_and_validate_website(
                                        candidate_url,
                                        curated_category,
                                        entry.get("name"),
                                    )
                                    if normalized:
                                        entry["website"] = normalized
                                        entry["website_source"] = "wikimedia"
                                        entry["website_verification_reason"] = "wikidata official website"

                            entry.pop("wikimedia_candidate_website", None)
                            entry.pop("wikimedia_candidate_website_final", None)
                            entry.pop("wikimedia_confidence", None)
                            entry.pop("wikimedia_conflict", None)
                except Exception as exc:
                    print(
                        f"   WARN Wikimedia enrichment failed for {curated_category}: {exc}"
                    )

            selected: list[Dict[str, object]] = []
            for key in ordered_keys:
                entry = by_key.get(key)
                if not entry:
                    continue
                if not (entry.get("website") or "").strip():
                    continue
                selected.append(entry)
                if len(selected) >= 4:
                    break

            if hasattr(datastore, "client"):
                try:
                    datastore.client.table("curated_places") \
                        .delete() \
                        .eq("city_key", city_key) \
                        .eq("category", curated_category) \
                        .execute()
                except Exception:
                    pass

            inserted = 0
            max_insert = 4
            for entry in selected:
                if inserted >= max_insert:
                    break
                name = str(entry.get("name") or "").strip()
                website = str(entry.get("website") or "").strip() if entry.get("website") else None
                if not name or not website:
                    continue

                if entry.get("website_source") == "wikimedia":
                    final_website = website
                    website_status = "ok"
                    website_reason = entry.get("website_verification_reason") or "wikidata official website"
                    website_source = "wikimedia"
                else:
                    final_website, website_status, website_reason, website_source = _quality_gate_curated_place(
                        city_key=city_key,
                        category=curated_category,
                        name=name,
                        website=website,
                        city_lat=city_lat,
                        city_lon=city_lon,
                    )
                    if website_status == "reject" or not final_website:
                        continue

                new_status = _normalize_status(website_status)
                new_reason = _normalize_text(website_reason)
                new_source = _normalize_source(website_source)
                new_canonical = _normalize_text(final_website)

                ok = datastore.upsert_curated_place(
                    city_key=city_key,
                    category=curated_category,
                    name=name,
                    website=final_website,
                    notes=None,
                    must_include=True,
                    website_canonical=new_canonical,
                    website_source=new_source,
                    website_status=new_status,
                    website_verified_at=datetime.now(timezone.utc).isoformat(),
                    website_verification_reason=new_reason,
                )
                if ok:
                    upserted += 1
                    inserted += 1
            continue

        if curated_category == "rental_car":
            provider_urls: Dict[str, str] = {}
            provider_labels: Dict[str, str] = {}
            if country_code:
                rental_links = resolve_rental_car_links(country_code, country_name_en)
                provider_urls = {
                    entry.get("provider"): entry.get("url")
                    for entry in rental_links
                    if entry.get("provider") and entry.get("url")
                }
                provider_labels = {
                    entry.get("provider"): entry.get("label") or entry.get("provider") or ""
                    for entry in rental_links
                    if entry.get("provider")
                }

            def _rental_score(row: Dict[str, object]) -> tuple:
                website = row.get("website") or ""
                notes = row.get("notes") or ""
                name = row.get("name") or ""
                return (
                    1 if website else 0,
                    len(notes),
                    len(name),
                )

            best_by_provider: Dict[str, Dict[str, Optional[str]]] = {}
            for item in _as_list(context.get(ctx_key)):
                if not isinstance(item, dict):
                    continue
                name = (item.get("name") or "").strip()
                if not name:
                    continue
                provider = detect_rental_car_provider(name)
                if not provider:
                    continue

                website = _normalize_website(item.get("website"), curated_category, name)
                notes = _normalize_text(item.get("address") or item.get("notes"))
                candidate = {
                    "name": name,
                    "website": website,
                    "notes": notes,
                }
                current = best_by_provider.get(provider)
                if current is None or _rental_score(candidate) > _rental_score(current):
                    best_by_provider[provider] = candidate

            if not best_by_provider and provider_urls:
                for provider, url in provider_urls.items():
                    label = provider_labels.get(provider, "") or provider
                    if not label:
                        continue
                    best_by_provider[provider] = {
                        "name": label,
                        "website": _normalize_website(url, curated_category, label),
                        "notes": None,
                    }

            rental_candidates = list(best_by_provider.items())
            rental_candidates.sort(key=lambda item: _rental_score(item[1]), reverse=True)
            promote_limit = promotion_limits.get(curated_category)
            promoted_count = 0
            promoted_names = set()
            processed_any = bool(rental_candidates)

            for provider, candidate in rental_candidates:
                name = provider_labels.get(provider) or candidate.get("name") or provider
                if not name:
                    continue
                website = candidate.get("website") or provider_urls.get(provider)
                notes = candidate.get("notes")

                cache_key = (curated_category, _normalize_name(name))
                existing_row = existing_cache.get(cache_key)
                existing_website = None
                existing_notes = None
                existing_must_include = None
                existing_status = None
                existing_reason = None
                existing_source = None
                existing_canonical = None
                existing_requires_cleanup = False
                if existing_row:
                    existing_raw_website = existing_row.get("website")
                    existing_website = _normalize_website(existing_raw_website, curated_category, name)
                    existing_notes = _normalize_text(existing_row.get("notes"))
                    existing_must_include = _normalize_bool(existing_row.get("must_include"))
                    existing_status = _normalize_status(existing_row.get("website_status"))
                    existing_reason = _normalize_text(existing_row.get("website_verification_reason"))
                    existing_source = _normalize_source(existing_row.get("website_source"))
                    existing_canonical = _normalize_text(existing_row.get("website_canonical"))
                    if existing_raw_website and existing_website:
                        existing_requires_cleanup = existing_raw_website.strip().rstrip("/") != existing_website

                new_website = _normalize_website(website, curated_category, name)
                new_notes = _normalize_text(notes)
                new_must_include = _normalize_bool(False)
                from_existing = False

                if new_website is None and existing_website is not None:
                    new_website = existing_website
                    from_existing = True

                final_website, website_status, website_reason, website_source = _quality_gate_curated_place(
                    city_key=city_key,
                    category=curated_category,
                    name=name,
                    website=new_website,
                    city_lat=city_lat,
                    city_lon=city_lon,
                )

                if website_status == "reject":
                    continue
                if curated_category in require_website_categories and not final_website:
                    continue

                if from_existing and website_source == "osm":
                    website_source = existing_source or "manual"
                if existing_source and final_website and final_website == existing_website and website_source in {None, "osm"}:
                    website_source = existing_source

                new_status = _normalize_status(website_status)
                new_reason = _normalize_text(website_reason)
                new_source = _normalize_source(website_source)
                new_canonical = _normalize_text(final_website)
                if new_status == "ok" and (promote_limit is None or promoted_count < promote_limit):
                    new_must_include = _normalize_bool(True)
                    promoted_count += 1
                    promoted_names.add(_normalize_name(name))

                if existing_reason and existing_reason.lower() == "seed":
                    new_must_include = _normalize_bool(True)

                if existing_row:
                    if (
                        final_website == existing_website
                        and new_notes == existing_notes
                        and new_must_include == existing_must_include
                        and new_status == existing_status
                        and new_reason == existing_reason
                        and new_source == existing_source
                        and new_canonical == existing_canonical
                        and not existing_requires_cleanup
                    ):
                        continue

                ok = datastore.upsert_curated_place(
                    city_key=city_key,
                    category=curated_category,
                    name=name,
                    website=final_website,
                    notes=new_notes,
                    must_include=new_must_include,
                    website_canonical=new_canonical,
                    website_source=new_source,
                    website_status=new_status,
                    website_verified_at=datetime.now(timezone.utc).isoformat(),
                    website_verification_reason=new_reason,
                )
                if ok:
                    upserted += 1

            if processed_any and promoted_names:
                for (cat_key, existing_name), row in existing_cache.items():
                    if cat_key != curated_category:
                        continue
                    if not _normalize_bool(row.get("must_include")):
                        continue
                    if _normalize_name(row.get("name")) in promoted_names:
                        continue
                    datastore.upsert_curated_place(
                        city_key=city_key,
                        category=curated_category,
                        name=row.get("name") or existing_name,
                        website=None,
                        notes=None,
                        must_include=False,
                    )
            continue

        items = _as_list(context.get(ctx_key))
        promote_limit = promotion_limits.get(curated_category)
        promoted_count = 0
        promoted_names = set()
        processed_any = bool(items)
        for item in items[:10]:
            if not isinstance(item, dict):
                continue
            name = (item.get("name") or "").strip()
            if not name:
                continue
            deny = getattr(config, "CURATED_PLACE_NAME_DENYLIST", {}).get(curated_category, set())
            if deny and name.strip().lower() in deny:
                continue
            website = _extract_website(item, curated_category)
            # report_loader currently renders curated_places.notes as an "address" for supplies.
            notes = (item.get("address") or item.get("notes") or "").strip() or None
            if curated_category == "airport":
                iata = (item.get("iata") or "").strip().upper()
                if iata:
                    iata_note = f"IATA: {iata}"
                    if notes:
                        if iata_note.lower() not in notes.lower():
                            notes = f"{notes} | {iata_note}"
                    else:
                        notes = iata_note
            cache_key = (curated_category, _normalize_name(name))
            existing_row = existing_cache.get(cache_key)
            existing_website = None
            existing_notes = None
            existing_must_include = None
            existing_status = None
            existing_reason = None
            existing_source = None
            existing_canonical = None
            existing_requires_cleanup = False
            if existing_row:
                existing_raw_website = existing_row.get("website")
                existing_website = _normalize_website(existing_raw_website, curated_category, name)
                existing_notes = _normalize_text(existing_row.get("notes"))
                existing_must_include = _normalize_bool(existing_row.get("must_include"))
                existing_status = _normalize_status(existing_row.get("website_status"))
                existing_reason = _normalize_text(existing_row.get("website_verification_reason"))
                existing_source = _normalize_source(existing_row.get("website_source"))
                existing_canonical = _normalize_text(existing_row.get("website_canonical"))
                if existing_raw_website and existing_website:
                    existing_requires_cleanup = existing_raw_website.strip().rstrip("/") != existing_website

            new_website = _normalize_website(website, curated_category, name)
            new_notes = _normalize_text(notes)
            new_must_include = _normalize_bool(False)
            from_existing = False

            if new_website is None and existing_website is not None:
                new_website = existing_website
                from_existing = True

            canonical_applied = False
            if (
                city_key in {"minneapolis", "minneapolis,-minnesota,-united-states"}
                and country_code == "US"
                and curated_category in {"pharmacy", "supermarket", "convenience"}
            ):
                canonical = canonicalize_store_website(
                    name,
                    country_code,
                    new_website,
                    category=curated_category,
                )
                if canonical and canonical != new_website:
                    new_website = canonical
                    canonical_applied = True

            final_website, website_status, website_reason, website_source = _quality_gate_curated_place(
                city_key=city_key,
                category=curated_category,
                name=name,
                website=new_website,
                city_lat=city_lat,
                city_lon=city_lon,
            )

            if website_status == "reject":
                continue
            if curated_category in require_website_categories and not final_website:
                continue

            if canonical_applied and final_website and website_status != "reject":
                website_source = "store_chain_canonical"
                website_reason = "canonical_chain_match"

            if from_existing and website_source == "osm":
                website_source = existing_source or "manual"
            if existing_source and final_website and final_website == existing_website and website_source in {None, "osm"}:
                website_source = existing_source

            new_status = _normalize_status(website_status)
            new_reason = _normalize_text(website_reason)
            new_source = _normalize_source(website_source)
            new_canonical = _normalize_text(final_website)
            if new_status == "ok" and (promote_limit is None or promoted_count < promote_limit):
                new_must_include = _normalize_bool(True)
                promoted_count += 1
                promoted_names.add(_normalize_name(name))

            if existing_row:
                if (
                    final_website == existing_website
                    and new_notes == existing_notes
                    and new_must_include == existing_must_include
                    and new_status == existing_status
                    and new_reason == existing_reason
                    and new_source == existing_source
                    and new_canonical == existing_canonical
                    and not existing_requires_cleanup
                ):
                    if not (curated_category == "rental_car" and final_website is None):
                        continue

            ok = datastore.upsert_curated_place(
                city_key=city_key,
                category=curated_category,
                name=name,
                website=final_website,
                notes=new_notes,
                must_include=new_must_include,
                website_canonical=new_canonical,
                website_source=new_source,
                website_status=new_status,
                website_verified_at=datetime.now(timezone.utc).isoformat(),
                website_verification_reason=new_reason,
            )
            if ok:
                upserted += 1
        if processed_any and promoted_names:
            for (cat_key, existing_name), row in existing_cache.items():
                if cat_key != curated_category:
                    continue
                if not _normalize_bool(row.get("must_include")):
                    continue
                if _normalize_name(row.get("name")) in promoted_names:
                    continue
                datastore.upsert_curated_place(
                    city_key=city_key,
                    category=curated_category,
                    name=row.get("name") or existing_name,
                    website=None,
                    notes=None,
                    must_include=False,
                )
    return upserted


def _detect_language(feed, title: str, summary: str) -> str:
    """Detect language from feed metadata or infer from content.
    
    Returns language code like 'pt-pt', 'es', 'en', 'fr', etc.
    """
    # Try feed-level language first
    feed_lang = feed.feed.get("language", "").lower().strip()
    if feed_lang:
        return feed_lang
    
    import re

    text_sample = f"{title} {summary}"
    if re.search(r"[\u3040-\u30ff\u4e00-\u9fff]", text_sample):
        return "ja"

    # Infer from Portuguese stopwords (cheap heuristic)
    pt_stopwords = ["de", "que", "nÃ£o", "nao", "para", "uma", "com", "por", "ao", "os", "as", "do", "da", "em"]
    text_sample = text_sample.lower()
    pt_count = sum(1 for word in pt_stopwords if f" {word} " in f" {text_sample} ")
    
    # If 3+ Portuguese stopwords found, likely Portuguese
    if pt_count >= 3:
        return "pt-pt"
    
    fr_stopwords = ["le", "la", "les", "des", "du", "une", "un", "et", "pour", "sur", "dans", "avec", "au", "aux"]
    fr_count = sum(1 for word in fr_stopwords if f" {word} " in f" {text_sample} ")
    if fr_count >= 3:
        return "fr"

    # Infer from Spanish stopwords (cheap heuristic)
    es_stopwords = ["de", "la", "que", "el", "en", "y", "a", "los", "del", "las", "por", "con", "una", "un"]
    es_count = sum(1 for word in es_stopwords if f" {word} " in f" {text_sample} ")
    
    # If 3+ Spanish stopwords found, likely Spanish
    if es_count >= 3:
        return "es"
    
    return "en"  # Default to English


def fetch_rss_feed(
    feed_url: str,
    feed_name: str,
    source_key: str,
    bypass_keyword_filter: bool = False,
    fallback_url: str | None = None,
    locale_hint: str | None = None,
    status_out: Dict[str, Any] | None = None,
) -> List[FeedItem]:
    """Fetch RSS feed and convert to FeedItem objects.

    When config.FILTER_BY_KEYWORDS is True, filters to travel-relevant items only.
    Set bypass_keyword_filter=True for official alert sources (for example embassy alerts)
    so items are not dropped solely by keyword-model relevance.
    """
    import feedparser
    import hashlib
    import requests

    print(f"   Fetching {feed_name}...")

    def _set_status(**kwargs: Any) -> None:
        if status_out is not None:
            status_out.update(kwargs)

    _set_status(
        source_key=source_key,
        name=feed_name,
        human_name=feed_name,
        primary_url=feed_url,
        fallback_url=fallback_url,
        final_url_used=None,
        http_status_code=None,
        content_type=None,
        total_entries_parsed=0,
        items_returned_after_filtering=0,
        used_fallback=False,
        status="error",
        error_message=None,
    )

    try:
        def _looks_like_html(content_type: str | None, snippet: str) -> bool:
            content_type = (content_type or "").lower()
            snippet = (snippet or "").lstrip().lower()
            if "text/html" in content_type or "application/xhtml" in content_type:
                if snippet.startswith("<?xml") or snippet.startswith("<rss") or snippet.startswith("<feed"):
                    return False
                return True
            if snippet.startswith("<!doctype html") or snippet.startswith("<html"):
                return True
            return False

        def _fetch_url(url: str) -> requests.Response | None:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36",
                "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8",
            }
            try:
                return requests.get(url, headers=headers, timeout=20)
            except Exception as exc:
                print(f"   WARN Failed to fetch {feed_name} ({url}): {exc}")
                return None

        used_fallback = False

        def _fetch_with_fallback(primary: str, fallback: str | None) -> requests.Response | None:
            nonlocal used_fallback
            response = _fetch_url(primary)
            if response is None or response.status_code != 200:
                if fallback:
                    status = "no response" if response is None else f"HTTP {response.status_code}"
                    print(f"   WARN {feed_name} failed at {primary} ({status}); trying fallback {fallback}")
                    used_fallback = True
                    response = _fetch_url(fallback)
            return response

        response = _fetch_with_fallback(feed_url, fallback_url)
        if response is None:
            print(f"   WARN Failed to fetch {feed_name}")
            _set_status(
                used_fallback=used_fallback,
                final_url_used=fallback_url if used_fallback else feed_url,
                status="error",
                error_message="request_failed",
            )
            return []

        status_code = response.status_code
        content_type = response.headers.get("Content-Type")
        _set_status(
            used_fallback=used_fallback,
            final_url_used=response.url or (fallback_url if used_fallback else feed_url),
            http_status_code=status_code,
            content_type=content_type,
        )
        if status_code != 200:
            print(f"   WARN Failed to fetch {feed_name} (HTTP {status_code})")
            _set_status(status="error", error_message=f"http_status_{status_code}")
            return []

        snippet = response.content[:1024].decode("utf-8", errors="ignore")
        if _looks_like_html(content_type, snippet):
            if fallback_url and not used_fallback and response.url != fallback_url:
                print(f"   WARN {feed_name} returned HTML from {response.url}; trying fallback {fallback_url}")
                used_fallback = True
                response = _fetch_url(fallback_url)
                if response is None:
                    print(f"   WARN Failed to fetch {feed_name} (fallback)")
                    _set_status(
                        used_fallback=True,
                        final_url_used=fallback_url,
                        status="error",
                        error_message="fallback_request_failed",
                    )
                    return []
                status_code = response.status_code
                content_type = response.headers.get("Content-Type")
                _set_status(
                    used_fallback=True,
                    final_url_used=response.url or fallback_url,
                    http_status_code=status_code,
                    content_type=content_type,
                )
                if status_code != 200:
                    print(f"   WARN Failed to fetch {feed_name} (fallback HTTP {status_code})")
                    _set_status(status="error", error_message=f"http_status_{status_code}")
                    return []
                snippet = response.content[:1024].decode("utf-8", errors="ignore")

            if _looks_like_html(content_type, snippet):
                print(f"   WARN {feed_name} returned HTML; skipping items")
                _set_status(
                    used_fallback=used_fallback,
                    status="error",
                    error_message="html_response_detected",
                )
                return []

        feed = feedparser.parse(response.content)

        total_entries = len(feed.entries or [])
        _set_status(total_entries_parsed=total_entries, used_fallback=used_fallback)
        bozo_exception = str(getattr(feed, "bozo_exception", "")) if getattr(feed, "bozo", False) else ""

        if total_entries == 0:
            if getattr(feed, "bozo", False):
                message = f"bozo_parse_error:{bozo_exception}" if bozo_exception else "bozo_parse_error"
                _set_status(status="error", error_message=message)
            else:
                _set_status(status="warning", error_message="no_entries_parsed")
            print(f"   WARN No entries found in {feed_name}")
            return []

        _set_status(
            status="ok",
            error_message=(f"bozo_parse_warning:{bozo_exception}" if bozo_exception else None),
        )
        print(f"   -> Parsed {total_entries} entries from {feed_name}; locale hint: {locale_hint or 'None'}")

        items = []
        skipped_low_relevance = 0
        now = datetime.now(timezone.utc)
        lang = locale_hint or "en"

        for entry in feed.entries:
            try:
                # Extract fields from RSS entry
                title = entry.get('title', 'Untitled')
                link = entry.get('link', '')
                content = ''
                if 'content' in entry and entry['content']:
                    # Some parsers return a list of content objects
                    if isinstance(entry['content'], list):
                        content = (entry['content'][0] or {}).get('value', '')
                    else:
                        content = entry['content']
                if not content:
                    content = entry.get('content_html') or entry.get('content:encoded') or ''
                description = entry.get('summary', entry.get('description', ''))
                summary = content if content else description

                tags = entry.get('tags') or []
                categories = []
                for tag in tags:
                    if isinstance(tag, dict):
                        term = tag.get('term') or tag.get('label') or tag.get('value')
                        if term:
                            categories.append(term)
                    elif tag:
                        categories.append(str(tag))

                # Parse published date
                published_date = parse_published_at(entry, now)

                # Detect language (or use provided locale hint)
                if locale_hint:
                    lang = locale_hint
                else:
                    lang = _detect_language(feed, title, summary)
                locale = (lang or "").split("-")[0]
                locale = locale if locale in getattr(config, "EXTRA_LOCALE_KEYWORDS", {}) else None

                # Score based on configured relevance mode
                relevance_mode = getattr(config, "TRAVEL_RELEVANCE_MODE", "keywords")

                if relevance_mode == "semantic_multilingual":
                    # Try semantic scoring (language-agnostic)
                    try:
                        from travel_relevance_semantic import score_semantic_relevance
                        score, reason = score_semantic_relevance(title, summary)
                        relevance = {
                            "score": score,
                            "matched_keywords": [],
                            "regular_matches": [],
                            "strong_matches": [],
                            "category_matches": [],
                            "is_critical": False,
                            "reason": reason,
                            "excluded": False,
                        }
                        keyword_matches = 0
                        strong_matches = 0
                    except ImportError:
                        # Fall back to keywords if semantic libraries not available
                        print("   WARN Semantic scoring unavailable; falling back to keywords")
                        relevance = compute_travel_relevance(
                            title=title,
                            summary=summary,
                            content=content,
                            categories=categories,
                            locale=locale,
                        )
                        keyword_matches = len(relevance["regular_matches"])
                        strong_matches = len(relevance["strong_matches"])
                else:
                    # Traditional keyword-based scoring (default)
                    relevance = compute_travel_relevance(
                        title=title,
                        summary=summary,
                        content=content,
                        categories=categories,
                        locale=locale,
                    )
                    keyword_matches = len(relevance["regular_matches"])
                    strong_matches = len(relevance["strong_matches"])

                # Determine threshold based on mode
                if relevance_mode == "semantic_multilingual":
                    threshold = getattr(config, "SEMANTIC_RELEVANCE_THRESHOLD", 0.33)
                    is_relevant = relevance["score"] >= threshold
                else:
                    # Keyword mode
                    # Allow locale-specific relaxation for Russian: accept a single keyword match
                    if locale == "ru":
                        is_relevant = (
                            relevance["score"] >= INGEST_MIN_SCORE
                            or strong_matches >= INGEST_MIN_STRONG
                            or keyword_matches >= 1
                        )
                    else:
                        is_relevant = (
                            relevance["score"] >= INGEST_MIN_SCORE
                            or strong_matches >= INGEST_MIN_STRONG
                            or keyword_matches >= INGEST_MIN_KEYWORDS
                        )

                if bypass_keyword_filter:
                    sentinel = "__official_alert_bypass__"
                    matched_keywords = list(relevance.get("matched_keywords") or [])
                    regular_matches = list(relevance.get("regular_matches") or [])
                    if sentinel not in matched_keywords:
                        matched_keywords.append(sentinel)
                    if sentinel not in regular_matches:
                        regular_matches.append(sentinel)
                    relevance["score"] = 1.0
                    relevance["matched_keywords"] = matched_keywords
                    relevance["regular_matches"] = regular_matches
                    relevance["is_critical"] = True

                    reason_parts = [
                        part.strip()
                        for part in str(relevance.get("reason") or "").split(",")
                        if part.strip()
                    ]
                    lowered_parts = {part.lower() for part in reason_parts}
                    if "critical" not in lowered_parts:
                        reason_parts.append("critical")
                    if "official_alert_bypass" not in lowered_parts:
                        reason_parts.append("official_alert_bypass")
                    relevance["reason"] = ", ".join(reason_parts) if reason_parts else "critical, official_alert_bypass"
                    is_relevant = True

                # Skip items that are not travel-relevant at ingest time (when enabled)
                if getattr(config, "FILTER_BY_KEYWORDS", True) and not bypass_keyword_filter:
                    if not is_relevant:
                        skipped_low_relevance += 1
                        continue

                # Generate unique ID from link or title+published
                guid = entry.get('id', link)
                if guid:
                    item_id = hashlib.sha256(guid.encode()).hexdigest()[:16]
                else:
                    item_id = hashlib.sha256(f"{title}{published_date}".encode()).hexdigest()[:16]

                # Create FeedItem (match base.py dataclass fields)
                raw_payload = {
                    'entry': dict(entry),
                    'author': entry.get('author'),
                    'tags': entry.get('tags'),
                    'content_html': content,
                    'categories': categories,
                    'lang': lang,
                    'travel_relevance_score': relevance["score"],
                    'travel_keywords_matched': relevance["matched_keywords"],
                    'travel_relevance_reason': relevance["reason"],
                    'is_critical': bool(relevance.get("is_critical")),
                    'official_alert_bypass': bool(bypass_keyword_filter),
                }
                item = FeedItem(
                    id=f"{source_key}_{item_id}",
                    source_key=source_key,
                    city_key=None,  # Will be set later
                    guid=guid,
                    url=link,
                    title=title,
                    summary=summary,
                    published_at=published_date,
                    fetched_at=now,
                    travel_relevance_score=relevance["score"],
                    travel_keywords_matched=relevance["matched_keywords"],
                    travel_relevance_reason=relevance["reason"],
                    raw=raw_payload,
                )
                items.append(item)

            except Exception as e:
                print(f"   WARN Failed to parse entry: {e}")
                continue

        kept_count = len(items)
        _set_status(items_returned_after_filtering=kept_count)
        skipped_note = f" (skipped {skipped_low_relevance} low-relevance)" if skipped_low_relevance else ""
        print(f"   OK Retrieved {kept_count} travel-relevant items from {feed_name}{skipped_note}; locale used: {lang}")
        return items

    except Exception as e:
        _set_status(status="error", error_message=str(e))
        print(f"   ERROR Failed to fetch {feed_name}: {e}")
        return []


def fetch_us_state_dept_advisories(filter_country_name: str | None = None) -> List[FeedItem]:
    """Fetch US State Department travel advisories from RSS feed.

    If filter_country_name is provided, return only advisories whose title mentions that country.
    """
    feed_url = "https://travel.state.gov/_res/rss/TAsTWs.xml"
    source_key = "us_travel_advisories"
    feed_name = "US State Department Travel Advisories"
    
    # Mapping of country names to official State Department URL slugs
    COUNTRY_SLUG_MAP = {
        "czech republic": "czechia",
        "czechia": "czechia",
        "italy": "italy",
        "portugal": "portugal",
        "france": "france",
        "mexico": "mexico",
        "united kingdom": "united-kingdom",
        "united states": "united-states",
    }
    
    # Use the generic RSS fetch function
    items = fetch_rss_feed(
        feed_url,
        feed_name,
        source_key,
        bypass_keyword_filter=True,
    )
    
    # Parse advisory levels and country info from entries.
    # Example title: "France Travel Advisory - Level 2: Exercise Increased Caution"
    import re

    level_pattern = re.compile(r"\blevel\s*([1-4])\b", re.IGNORECASE)
    country_pattern = re.compile(
        r"^(.+?)(?:\s+Travel Advisory|\s+Travel Warning|\s*[-\u2013\u2014]\s*Level)",
        re.IGNORECASE,
    )

    for item in items:
        title = (item.title or "").strip()
        summary = (item.summary or "").strip()

        if not isinstance(item.raw, dict):
            item.raw = {}

        level = None
        level_source = None

        # Primary source: title
        level_match = level_pattern.search(title)
        if level_match:
            level = int(level_match.group(1))
            level_source = "title"
        else:
            # Fallback source: summary/content text
            fallback_text = summary
            if not fallback_text:
                fallback_text = str(item.raw.get("summary") or item.raw.get("content") or "")
            fallback_match = level_pattern.search(fallback_text)
            if fallback_match:
                level = int(fallback_match.group(1))
                level_source = "summary"

        if level is not None:
            item.raw["advisory_level"] = level
            item.raw["advisory_level_source"] = level_source

        # Extract country name from title (usually before "Travel Advisory" or " - Level")
        country_match = country_pattern.match(title)
        if country_match:
            extracted_country = country_match.group(1).strip()
            item.raw["country_name"] = extracted_country

            # Add country_slug using the mapping
            country_slug = COUNTRY_SLUG_MAP.get(extracted_country.lower(), "")
            if not country_slug:
                # Fallback: normalize country name to slug format
                country_slug = re.sub(r"[^a-z0-9]+", "-", extracted_country.lower()).strip("-")
            item.raw["country_slug"] = country_slug

    # Optional country filter
    if filter_country_name:
        want = (filter_country_name or "").strip().lower()
        filtered = []
        for item in items:
            title = (item.title or "").strip().lower()
            if want and want in title:
                filtered.append(item)
        items = filtered
    
    return items


def fetch_canada_advisories(country_code: str) -> List[FeedItem]:
    """Fetch Canada travel advisories for a specific country code (e.g., 'US', 'CA', 'FR')."""
    import urllib.request
    import json
    import hashlib
    
    url = "https://data.international.gc.ca/travel-voyage/index-updated.json"
    source_name = "Canada Travel Advisories"
    
    print(f"   Fetching {source_name}...")
    
    try:
        # Create SSL context to handle certificate issues
        import ssl
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        with urllib.request.urlopen(url, timeout=15, context=ssl_context) as response:
            data = json.loads(response.read().decode('utf-8'))
        
        # API returns dict with country codes as keys, not a list
        countries_data = data.get('data', {})
        target_advisory = countries_data.get((country_code or "").upper())
        
        if not target_advisory:
            print(f"   âš  No advisory found for {country_code}")
            return []
    except urllib.error.URLError as e:
        print(f"   âš  Network error fetching {source_name}: {e}")
        print(f"   â†’ Skipping Canada advisories (source unavailable)")
        return []
    except TimeoutError:
        print(f"   âš  Timeout fetching {source_name}")
        print(f"   â†’ Skipping Canada advisories (timeout after 15s)")
        return []
    except Exception as e:
        print(f"   âš  Error fetching {source_name}: {e}")
        print(f"   â†’ Skipping Canada advisories")
        return []
    
    try:
        
        # Create a single FeedItem for the US advisory
        now = datetime.now(timezone.utc)
        
        # Extract advisory info from the 'eng' section
        eng_data = target_advisory.get('eng', {})
        advisory_text = eng_data.get('advisory-text', '')
        recent_updates = eng_data.get('recent-updates', '')
        
        # Advisory state: 0 = normal, 1 = exercise caution, 2+ = higher levels
        advisory_state = target_advisory.get('advisory-state', 0)
        
        date_pub = target_advisory.get('date-published', {})
        updated_date = date_pub.get('date', '') if isinstance(date_pub, dict) else str(date_pub)
        
        content = {
            "advisory_text": advisory_text,
            "recent_updates": recent_updates,
            "advisory_state": advisory_state,
            "updated": updated_date,
            "country_code": country_code.upper(),
            "country_name": eng_data.get('name', country_code.upper()),
        }
        
        content_str = json.dumps(content)
        content_hash = hashlib.sha256(content_str.encode()).hexdigest()[:16]
        
        item = FeedItem(
            id=f"canada_advisory_{country_code.lower()}_{content_hash}",
            source_key="canada_travel_advisories",
            city_key=None,  # Global advisory
            guid=f"canada_advisory_{country_code.lower()}_{content_hash}",
            url=f"https://travel.gc.ca/destinations/{country_code.lower()}",
            title=f"Canada Travel Advisory: {country_code.upper()}",
            summary=f"{advisory_text}. {recent_updates}" if recent_updates else advisory_text,
            published_at=now,
            fetched_at=now,
            raw=content,
        )
        
        print(f"   âœ“ Retrieved US travel advisory")
        return [item]
        
    except Exception as e:
        print(f"   âœ— Failed to fetch {source_name}: {e}")
        return []


def ensure_city_exists(city_key: str, city_config: Dict, datastore) -> bool:
    """
    Ensure city record exists in database, creating it if necessary.
    
    Args:
        city_key: City identifier (e.g., "miami")
        city_config: City configuration from config.CITIES
        datastore: Storage backend
        
    Returns:
        True if city exists or was created successfully
    """
    # Check if city exists in database
    try:
        cities = datastore.get_cities()
        city_exists = any(c.get('city_key') == city_key for c in cities)
        
        if not city_exists:
            print(f"   ðŸ“ City '{city_key}' not found in database, creating...")
            
            # Prepare city data for seeding
            cities_data = [{
                "city_key": city_key,
                "name": city_config['name'],
                "country_code": city_config.get('country_code'),
                "latitude": city_config.get('latitude'),
                "longitude": city_config.get('longitude'),
                "timezone": city_config.get('timezone'),
                "aliases": city_config.get('aliases', []),
                "enabled": True,
            }]
            
            # Seed the city using existing seed_base_data method
            if hasattr(datastore, 'seed_base_data'):
                if datastore.seed_base_data(cities_data, []):
                    print(f"   âœ“ City '{city_key}' created successfully")
                    return True
                else:
                    print(f"   âœ— Failed to create city '{city_key}'")
                    return False
            else:
                print(f"   âœ— Datastore does not support seeding")
                return False
        
        return True
        
    except Exception as e:
        print(f"   âœ— Failed to check/create city: {e}")
        import traceback
        traceback.print_exc()
        return False


def ensure_city_sources_exist(city_key: str, datastore) -> bool:
    """Ensure configured sources for a city exist in the sources table (additive only)."""
    try:
        existing_sources = datastore.get_sources() if hasattr(datastore, "get_sources") else []
        existing_keys = {s.get("source_key") for s in existing_sources if s.get("source_key")}

        expected_sources = [
            s for s in config.SOURCES
            if s.get("city_key") == city_key and s.get("enabled", True)
        ]

        missing_sources = [s for s in expected_sources if s.get("source_key") not in existing_keys]

        if not missing_sources:
            return True

        print(f"   ðŸ“¡ Seeding missing sources for '{city_key}': {[s.get('source_key') for s in missing_sources]}")

        if hasattr(datastore, "seed_base_data"):
            if datastore.seed_base_data([], missing_sources):
                print(f"   âœ“ Sources upserted for '{city_key}'")
                return True
            print(f"   âœ— Failed to upsert sources for '{city_key}'")
            return False

        print("   âœ— Datastore does not support seeding sources")
        return False
    except Exception as e:
        print(f"   âœ— Failed to ensure sources for '{city_key}': {e}")
        import traceback
        traceback.print_exc()
        return False


def sync_global_advisories(datastore) -> None:
    """Fetch and upsert global travel advisories used by all city reports."""
    print("\nðŸŒ Fetching global sources (travel advisories)...")
    try:
        advisory_items = fetch_canada_advisories("US")
        if advisory_items:
            datastore.upsert_feed_items(advisory_items)
            print("   âœ“ Stored Canada travel advisory (US)")
    except Exception as e:
        print(f"   âš  Failed to fetch/store Canada advisories: {e}")

    try:
        us_state_items = fetch_us_state_dept_advisories()
        if us_state_items:
            datastore.upsert_feed_items(us_state_items)
            print(f"   âœ“ Stored {len(us_state_items)} US State Dept advisories")
    except Exception as e:
        print(f"   âš  Failed to fetch/store US State Dept advisories: {e}")


def sync_city(
    city_key: str,
    city_config: Dict,
    datastore,
    skip_context: bool = False,
    context_only: bool = False,
    force: bool = False,
    reporter: Any = None,
) -> bool:
    """
    Sync all data sources for a specific city.
    
    Args:
        city_key: City identifier (e.g., "miami")
        city_config: City configuration from config.CITIES
        datastore: Storage backend
        skip_context: Skip hospital/transit sync (faster)
        context_only: Only sync hospital/transit (skip feeds)
        force: Bypass throttling
        
    Returns:
        True if successful
    """
    print(f"\n{'='*60}")
    print(f"Syncing city: {city_config['name']} ({city_key})")
    print(f"{'='*60}")
    
    # Ensure city exists in database
    if not ensure_city_exists(city_key, city_config, datastore):
        print(f"âœ— Cannot sync city '{city_key}' - failed to ensure city exists in database")
        return False

    # Ensure city sources exist (additive only)
    if not ensure_city_sources_exist(city_key, datastore):
        print(f"âœ— Cannot sync city '{city_key}' - failed to ensure sources exist in database")
        return False

    wikimedia_enabled = _wikimedia_enrichment_enabled(city_key)

    # Always apply seeded curated places (even when --skip-context is used)
    sync_seeded_curated_places(city_key, datastore)

    # Always ensure transit_snapshots exists from config (does not require context calls)
    try:
        transit_ok = _upsert_transit_from_config(city_key, city_config, datastore, force=False)
        if transit_ok:
            print("   âœ“ Transit snapshot ensured (config bootstrap)")
    except Exception as e:
        print(f"   âš  Transit bootstrap failed: {e}")
    
    lat = city_config.get('latitude')
    lon = city_config.get('longitude')
    timezone_str = city_config.get('timezone', 'America/New_York')
    city_name = city_config['name']
    
    if not lat or not lon:
        print(f"âœ— Missing coordinates for {city_key}")
        return False
    
    # ========================================================================
    # City Context (Hospitals, Pharmacies, Stores, Transit) - Monthly refresh
    # ========================================================================
    
    if context_only or not skip_context:  # Run if --context-only OR not skipping context
        print(f"\nðŸ“ Fetching city context (hospitals, pharmacies, supermarkets, convenience stores, transit)...")
        try:
            city_tool = CityContextTool(
                openai_api_key=config.OPENAI_API_KEY,
                datastore=datastore,
            )
            
            # This will fetch and cache city context via datastore
            context = city_tool.get_city_context(city_name, force_refresh=force, city_key=city_key)
            
            if context:
                hospitals = len(context.get('hospitals', []))
                pharmacies = len(context.get('pharmacies', []))
                supermarkets = len(context.get('supermarkets', []))
                conv_stores = len(context.get('convenience_stores', []))
                print(f"   âœ“ City context updated: {hospitals} hospitals, {pharmacies} pharmacies, {supermarkets} supermarkets, {conv_stores} convenience stores")

                # Populate curated_places (schema v3 canonical source for supplies/medical lists)
                curated_count = _sync_curated_places_from_context(city_key, context, datastore)
                if curated_count:
                    print(f"   âœ“ Curated places upserted: {curated_count} rows")

                # Ensure emergency numbers are DB-owned in city_context_snapshots
                em_ok = _upsert_emergency_numbers_into_city_context(city_key, city_config, datastore, force=force)
                if em_ok:
                    print(f"   âœ“ Emergency numbers upserted")

            else:
                print(f"   âš  No city context returned for {city_key}")

                # Even if city context fetch failed, still ensure emergency numbers + transit exist.
                em_ok = _upsert_emergency_numbers_into_city_context(city_key, city_config, datastore, force=force)
                if em_ok:
                    print(f"   âœ“ Emergency numbers upserted")
                
        except Exception as e:
            print(f"   âœ— Failed to fetch city context: {e}")
    else:
        print(f"\nâ© Skipping city context (--skip-context flag)")
        if wikimedia_enabled:
            context_snapshot = None
            try:
                ctx_obj = datastore.get_city_context(city_key)
                if ctx_obj and isinstance(getattr(ctx_obj, "context", None), dict):
                    context_snapshot = ctx_obj.context
            except Exception:
                context_snapshot = None

            if context_snapshot is None:
                try:
                    city_tool = CityContextTool(
                        openai_api_key=config.OPENAI_API_KEY,
                        datastore=datastore,
                    )
                    context_snapshot = city_tool.get_city_context(
                        city_name,
                        force_refresh=True,
                        city_key=city_key,
                    )
                except Exception as e:
                    print(f"   ??? Failed to fetch context for curated places: {e}")
                    context_snapshot = None

            if context_snapshot:
                curated_count = _sync_curated_places_from_context(city_key, context_snapshot, datastore)
                if curated_count:
                    print(f"   ??? Curated places upserted: {curated_count} rows")
    
    # ========================================================================
    # Feed Sync (News, Alerts, Weather)
    # ========================================================================
    
    if context_only:
        print(f"\nâ© Skipping feeds (--context-only flag)")
        return True
    
    all_items = []
    
    # --------------------------------------------------------------------
    # 1. Local RSS Feeds (City-specific)
    # --------------------------------------------------------------------
    
    print(f"\nðŸ“° Fetching RSS feeds for {city_key}...")
    
    for source in config.SOURCES:
        if source['type'] != 'rss':
            continue

        # Only fetch enabled city-specific RSS feeds.
        if source.get('city_key') != city_key or not source.get("enabled", True):
            continue

        source_url = str(source.get("url") or "").lower()
        source_key_text = str(source.get("source_key") or "").lower()
        fallback_url = source.get("fallback_url")
        bypass_keyword_filter = (
            "usembassy.gov" in source_url
            or "usembassy" in source_key_text
            or "us_embassy" in source_key_text
        )

        status_out: Dict[str, Any] = {}
        items = fetch_rss_feed(
            source["url"],
            source["name"],
            source["source_key"],
            bypass_keyword_filter=bypass_keyword_filter,
            fallback_url=fallback_url,
            locale_hint=city_config.get("language"),
            status_out=status_out,
        )

        # Tag items with city_key
        for item in items:
            item.city_key = city_key
            item.match_meta = {
                "match_type": "city_specific_source",
                "city_key": city_key,
                "source_key": source.get("source_key"),
            }

        all_items.extend(items)

        if reporter is not None:
            status_out.update(
                {
                    "city_key": city_key,
                    "source_key": source.get("source_key"),
                    "name": source.get("name"),
                    "human_name": source.get("name"),
                    "items_returned_after_filtering": len(items),
                }
            )
            reporter.add_record(status_out)
    
    # --------------------------------------------------------------------
    # 2. GDELT Geo News
    # --------------------------------------------------------------------
    
    print(f"\nðŸŒ Fetching GDELT news for {city_key}...")
    gdelt_raw_count = 0
    gdelt_returned_count = 0
    try:
        from sources.gdelt_geo import fetch_gdelt_geo
        
        radius_km = 50
        gdelt_items = fetch_gdelt_geo(
            city_name=city_name,
            city_key=city_key,
            lat=lat,
            lon=lon,
            radius_km=radius_km,
            max_records=100,
            timespan="7d",
        )
        gdelt_raw_count = len(gdelt_items)
        
        # Tag with city_key
        for item in gdelt_items:
            item.city_key = city_key
            item.match_meta = {
                "match_type": "gdelt_geo_query",
                "city_key": city_key,
                "radius_km": radius_km,
                "source_key": "gdelt_geo",
            }

        # Apply keyword filtering (optional) before adding
        if getattr(config, "FILTER_BY_KEYWORDS", True):
            filtered = []
            for it in gdelt_items:
                # Recompute or use existing relevance fields
                relevance = compute_travel_relevance(
                    title=it.title or "",
                    summary=it.summary or "",
                    content="",
                    categories=None,
                )
                keyword_matches = len(relevance["regular_matches"])
                strong_matches = len(relevance["strong_matches"])
                is_relevant = (
                    relevance["score"] >= INGEST_MIN_SCORE
                    or strong_matches >= INGEST_MIN_STRONG
                    or keyword_matches >= INGEST_MIN_KEYWORDS
                )
                if is_relevant:
                    filtered.append(it)
            gdelt_items = filtered

        gdelt_returned_count = len(gdelt_items)
        all_items.extend(gdelt_items)
        if reporter is not None:
            reporter.add_record(
                {
                    "source_key": "gdelt_geo",
                    "name": "GDELT Geo News",
                    "human_name": "GDELT Geo News",
                    "city_key": city_key,
                    "total_entries_parsed": gdelt_raw_count,
                    "items_returned_after_filtering": gdelt_returned_count,
                    "status": "ok",
                }
            )
        
    except Exception as e:
        print(f"   âœ— Failed to fetch GDELT: {e}")
        if reporter is not None:
            reporter.add_record(
                {
                    "source_key": "gdelt_geo",
                    "name": "GDELT Geo News",
                    "human_name": "GDELT Geo News",
                    "city_key": city_key,
                    "total_entries_parsed": gdelt_raw_count,
                    "items_returned_after_filtering": gdelt_returned_count,
                    "status": "error",
                    "error_message": str(e),
                }
            )
    
    # --------------------------------------------------------------------
    # 3. NWS Weather Alerts (US cities only)
    # --------------------------------------------------------------------
    
    if city_config.get('country_code') == 'US':
        print(f"\nðŸŒ¦ï¸  Fetching NWS alerts for {city_key}...")
        nws_count = 0
        try:
            from sources.nws_alerts import fetch_nws_alerts
            
            nws_items = fetch_nws_alerts(
                city_name=city_name,
                city_key=city_key,
                lat=lat,
                lon=lon,
            )
            nws_count = len(nws_items)
            
            # Tag with city_key
            for item in nws_items:
                item.city_key = city_key
                item.match_meta = {
                    "match_type": "nws_point_query",
                    "city_key": city_key,
                    "source_key": "nws_alerts",
                }
            
            all_items.extend(nws_items)
            if reporter is not None:
                reporter.add_record(
                    {
                        "source_key": "nws_alerts",
                        "name": "NWS Alerts",
                        "human_name": "NWS Alerts",
                        "city_key": city_key,
                        "total_entries_parsed": nws_count,
                        "items_returned_after_filtering": nws_count,
                        "status": "ok",
                    }
                )
            
        except Exception as e:
            print(f"   âœ— Failed to fetch NWS alerts: {e}")
            if reporter is not None:
                reporter.add_record(
                    {
                        "source_key": "nws_alerts",
                        "name": "NWS Alerts",
                        "human_name": "NWS Alerts",
                        "city_key": city_key,
                        "status": "error",
                        "error_message": str(e),
                    }
                )
    else:
        print(f"\nâ© Skipping NWS alerts (not a US city)")
        if reporter is not None:
            reporter.add_record(
                {
                    "source_key": "nws_alerts",
                    "name": "NWS Alerts",
                    "human_name": "NWS Alerts",
                    "city_key": city_key,
                    "status": "skipped",
                    "error_message": "not_us_city",
                }
            )
    
    # --------------------------------------------------------------------
    # 4. Open-Meteo Weather Forecast (stored in weather_forecasts table)
    # --------------------------------------------------------------------
    
    print(f"\nâ›… Checking weather forecast for {city_key}...")
    
    # Check if latest forecast is still fresh (within TTL)
    from config import WEATHER_FORECAST_TTL_HOURS, WEATHER_FORECAST_RETENTION_DAYS
    latest_forecast = datastore.get_latest_weather_forecast(city_key, provider="open_meteo")
    
    skip_fetch = False
    weather_status: Dict[str, Any] = {
        "source_key": "open_meteo_forecast",
        "name": "Open-Meteo Forecast",
        "human_name": "Open-Meteo Forecast",
        "city_key": city_key,
        "status": "ok",
        "total_entries_parsed": 0,
        "items_returned_after_filtering": 0,
    }
    if latest_forecast:
        fetched_at_str = latest_forecast.get('fetched_at')
        if fetched_at_str:
            fetched_at = datetime.fromisoformat(fetched_at_str.replace('Z', '+00:00'))
            age_hours = (datetime.now(timezone.utc) - fetched_at).total_seconds() / 3600
            if age_hours < WEATHER_FORECAST_TTL_HOURS:
                print(f"   âœ“ Weather forecast is fresh ({age_hours:.1f}h old, TTL: {WEATHER_FORECAST_TTL_HOURS}h)")
                skip_fetch = True
                weather_status.update(
                    {
                        "status": "skipped",
                        "error_message": f"cache_fresh:{age_hours:.1f}h",
                    }
                )
    
    if not skip_fetch:
        try:
            from sources.open_meteo import fetch_open_meteo_forecast
            
            weather_items = fetch_open_meteo_forecast(
                city_name=city_name,
                city_key=city_key,
                lat=lat,
                lon=lon,
                timezone_str=timezone_str,
            )
            weather_count = len(weather_items)
            weather_status.update(
                {
                    "status": "ok",
                    "total_entries_parsed": weather_count,
                    "items_returned_after_filtering": weather_count,
                    "error_message": None,
                }
            )
            
            # Store weather forecast in dedicated table (schema v2)
            if weather_items:
                weather_item = weather_items[0]  # Should be single forecast item
                raw_forecast = weather_item.raw
                
                # Extract current conditions for indexed fields
                current = raw_forecast.get('current', {})
                extracted_fields = {
                    'current_temp_c': (current.get('temperature_2m', 0) - 32) * 5/9,  # F to C
                    'current_wind_kph': current.get('wind_speed_10m', 0) * 1.60934,  # mph to kph
                    'current_precip_mm': current.get('precipitation', 0) * 25.4,  # in to mm
                }
                
                datastore.upsert_weather_forecast(
                    city_key=city_key,
                    provider="open_meteo",
                    raw=raw_forecast,
                    extracted_fields=extracted_fields,
                )
            
        except Exception as e:
            print(f"   âœ— Failed to fetch weather forecast: {e}")
            weather_status.update({"status": "error", "error_message": str(e)})
    if reporter is not None:
        reporter.add_record(weather_status)
    
    # ========================================================================
    # Store Feed Items
    # ========================================================================
    
    if all_items:
        print(f"\nðŸ’¾ Storing {len(all_items)} feed items...")
        try:
            # Use batch upsert with deduplication
            stored_count = datastore.upsert_feed_items(all_items)
            print(f"   âœ“ Stored/updated {stored_count} feed items")
        except Exception as e:
            print(f"   âœ— Failed to store feed items: {e}")
            import traceback
            traceback.print_exc()
            return False
    else:
        print(f"\nâš  No feed items to store")

    # ========================================================================
    # Global Advisories (fetched once, stored for all cities)
    # ========================================================================
    # Note: Advisories are stored globally, not per-city. Each city filters
    # the advisory it needs at report generation time based on country code.
    # We skip this if FILTER_COUNTRY_ADVISORIES is False to maintain backward compatibility.
    # ========================================================================
    
    # Skip per-city advisory sync - advisories are synced globally in the --all path
    
    # ========================================================================
    # Cleanup Old Data (30-day retention)
    # ========================================================================
    
    if force:  # Only cleanup on full sync with --force
        print(f"\nðŸ—‘ï¸  Cleaning up old data...")
        try:
            # Clean up feed items older than 30 days
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=30)
            
            if hasattr(datastore, 'cleanup_old_feed_items'):
                deleted = datastore.cleanup_old_feed_items(cutoff_date)
                print(f"   âœ“ Cleaned up {deleted} feed items older than 30 days")
            
        except Exception as e:
            print(f"   âš  Cleanup failed: {e}")
    
    print(f"\nâœ… Sync complete for {city_key}")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Sync data sources to Supabase (Schema v2)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Initial setup (runtime reset + reseed + full sync)
  python scripts/sync_supabase.py --city miami --reset --force
  
  # Daily fast sync (skip hospital/transit)
  python scripts/sync_supabase.py --city miami --skip-context --force
  
  # Monthly infrastructure update
  python scripts/sync_supabase.py --city miami --context-only --force
  
  # Full sync with cleanup
  python scripts/sync_supabase.py --city miami --force
  
  # Sync all enabled cities
  python scripts/sync_supabase.py --all --force
        """
    )
    
    parser.add_argument(
        "--city",
        type=str,
        help="City key to sync (e.g., 'miami')",
    )
    
    parser.add_argument(
        "--all",
        action="store_true",
        help="Sync all enabled cities from config.CITIES",
    )
    
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Truncate runtime tables and reseed from config (requires --force)",
    )
    
    parser.add_argument(
        "--force",
        action="store_true",
        help="Bypass throttling, enable cleanup",
    )
    
    parser.add_argument(
        "--skip-context",
        action="store_true",
        help="Skip hospital/transit sync (faster daily sync)",
    )
    
    parser.add_argument(
        "--context-only",
        action="store_true",
        help="Only sync hospital/transit (skip feeds)",
    )
    
    parser.add_argument(
        "--feeds-only",
        action="store_true",
        help="Alias for --skip-context",
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.city and not args.all:
        parser.error("Must specify --city or --all")
    
    if args.reset and not args.force:
        parser.error("--reset requires --force flag for safety")
    
    if args.skip_context and args.context_only:
        parser.error("Cannot use --skip-context and --context-only together")
    
    # Handle --feeds-only alias
    if args.feeds_only:
        args.skip_context = True
    
    # Get datastore
    try:
        datastore = get_datastore()
    except Exception as e:
        print(f"âœ— Failed to initialize datastore: {e}")
        print("\nMake sure environment variables are set:")
        print("  STORAGE_BACKEND=supabase")
        print("  SUPABASE_URL=https://your-project.supabase.co")
        print("  SUPABASE_SERVICE_ROLE_KEY=your-service-role-key")
        return 1
    
    # ========================================================================
    # Reset Mode (Runtime Tables + Reseed)
    # ========================================================================
    
    if args.reset:
        print("\n" + "="*60)
        print("RESET MODE: Truncating runtime tables (preserving city_reports)")
        print("="*60)
        
        if not hasattr(datastore, 'reset_runtime_data'):
            print("âœ— Datastore does not support runtime reset (file-based?)")
            return 1
        
        # Truncate runtime tables only (preserve city_reports and config)
        if not datastore.reset_runtime_data():
            print("âœ— Failed to reset runtime data")
            return 1
        
        # Reseed cities and sources from config
        print("\nðŸ“ Reseeding cities and sources from config...")
        
        # Prepare city data for seeding
        cities_data = []
        for city_key, city_config in config.CITIES.items():
            if city_config.get('enabled', True):
                cities_data.append({
                    "city_key": city_key,
                    "name": city_config['name'],
                    "country_code": city_config.get('country_code'),
                    "latitude": city_config.get('latitude'),
                    "longitude": city_config.get('longitude'),
                    "timezone": city_config.get('timezone'),
                    "aliases": city_config.get('aliases', []),
                    "enabled": True,
                })
        
        if not hasattr(datastore, 'seed_base_data'):
            print("âœ— Datastore does not support seeding")
            return 1
        
        if not datastore.seed_base_data(cities_data, config.SOURCES):
            print("âœ— Failed to seed base data")
            return 1
        
        print("\nâœ… Reset and reseed complete")
    
    # ========================================================================
    # Sync Mode
    # ========================================================================
    
    if args.all:
        # Sync all enabled cities
        print("\n" + "="*60)
        print("Syncing all enabled cities")
        print("="*60)
        
        sync_global_advisories(datastore)

        # Sync each enabled city
        failed_cities = []
        for city_key, city_config in config.CITIES.items():
            if not city_config.get('enabled', True):
                print(f"\nâ© Skipping disabled city: {city_key}")
                continue
            
            success = sync_city(
                city_key=city_key,
                city_config=city_config,
                datastore=datastore,
                skip_context=args.skip_context,
                context_only=args.context_only,
                force=args.force,
            )
            
            if not success:
                failed_cities.append(city_key)
        
        if failed_cities:
            print(f"\nâš  Failed cities: {', '.join(failed_cities)}")
            return 1
        else:
            print(f"\nâœ… All cities synced successfully")
            return 0
    
    else:
        # Sync single city
        city_key = args.city
        
        if city_key not in config.CITIES:
            print(f"âœ— Unknown city: {city_key}")
            print(f"Available cities: {', '.join(config.CITIES.keys())}")
            return 1
        
        city_config = config.CITIES[city_key]
        
        if not city_config.get('enabled', True):
            print(f"âš  City '{city_key}' is disabled in config")
        
        # Keep single-city behavior aligned with --all: refresh global advisories too.
        sync_global_advisories(datastore)

        success = sync_city(
            city_key=city_key,
            city_config=city_config,
            datastore=datastore,
            skip_context=args.skip_context,
            context_only=args.context_only,
            force=args.force,
        )
        
        return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())

