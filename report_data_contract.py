"""Canonical report_data contract.

This module defines:
- A canonical top-level report_data shape that must always be present when writing to
  `city_reports.report_data`.
- A builder that returns a fully-populated structure (with empty defaults) so runs never
  store partial/minimal dicts.

Notes
- The authoritative DB schema stores `report_data` as JSONB; we enforce structure in Python.
- We keep legacy/renderer-friendly keys (e.g., destination, top_actions) alongside the
  canonical sections to avoid breaking existing rendering paths.
"""

from __future__ import annotations

from datetime import datetime, timezone
from urllib.parse import quote_plus, urlparse
import re
from typing import Any, Dict, Optional

from helpers.emergency_numbers import resolve_emergency_numbers
from helpers.google_maps import build_google_maps_search_url
from helpers.location_label import build_city_label


REQUIRED_TOP_LEVEL_KEYS = [
    "executive_summary",
    "current_incidents",
    "city_essentials",
    "supplies_services",
    "opsec",
    "contacts_sources",
    "trend_data",
    "meta",
    "travel_essentials",
]

APP_LIKE_CATEGORIES = {"rideshare", "local transportation apps"}

APP_STORE_LINKS_BY_LABEL: Dict[str, Dict[str, str]] = {
    "uber": {
        "ios_url": "https://apps.apple.com/us/app/uber-request-a-ride/id368677368",
        "android_url": "https://play.google.com/store/apps/details?id=com.ubercab",
    },
    "lyft": {
        "ios_url": "https://apps.apple.com/us/app/lyft/id529379082",
        "android_url": "https://play.google.com/store/apps/details?id=me.lyft.android",
    },
    "bolt": {
        "ios_url": "https://apps.apple.com/us/app/bolt-request-a-ride/id675033630",
        "android_url": "https://play.google.com/store/apps/details?id=ee.mtakso.client",
    },
    "cabify": {
        "ios_url": "https://apps.apple.com/us/app/cabify/id476087442",
        "android_url": "https://play.google.com/store/apps/details?id=com.cabify.rider",
    },
    "freenow": {
        "ios_url": "https://apps.apple.com/us/app/freenow-by-lyft-taxi-more/id357852748",
        "android_url": "https://play.google.com/store/apps/details?id=taxi.android.client",
    },
    "ola": {
        "ios_url": "https://apps.apple.com/us/app/ola-book-cab-auto-bike-taxi/id539179365",
        "android_url": "https://play.google.com/store/apps/details?id=com.olacabs.customer",
    },
    "careem": {
        "ios_url": "https://apps.apple.com/us/app/careem-rides-food-grocery/id592978487",
        "android_url": "https://play.google.com/store/apps/details?id=com.careem.acma",
    },
    "didi": {
        "ios_url": "https://apps.apple.com/us/app/didi-rider-affordable-rides/id1362398401",
        "android_url": "https://play.google.com/store/apps/details?id=com.didiglobal.passenger",
    },
    "go miami-dade transit": {
        "ios_url": "https://apps.apple.com/us/app/go-miami-dade-transit/id464426407",
        "android_url": "https://play.google.com/store/apps/details?id=gov.miamidade.MDTTracker",
    },
    "yandex go": {
        "ios_url": "https://apps.apple.com/us/app/yandex-go-taxi-food-market/id472650686",
        "android_url": "https://play.google.com/store/apps/details?id=ru.yandex.taxi",
    },
    "indrive": {
        "ios_url": "https://apps.apple.com/us/app/indrive-save-on-city-rides/id780125801",
        "android_url": "https://play.google.com/store/apps/details?id=sinet.startup.inDriver",
    },
    "ile-de-france mobilites": {
        "ios_url": "https://apps.apple.com/us/app/id484527651",
        "android_url": "https://play.google.com/store/apps/details?id=com.applidium.vianavigo",
    },
    "bonjour ratp": {
        "ios_url": "https://apps.apple.com/us/app/bonjour-ratp/id507107090",
        "android_url": "https://play.google.com/store/apps/details?id=com.fabernovel.ratp",
    },
    "s'hail": {
        "ios_url": "https://apps.apple.com/us/app/shail/id1214681230",
        "android_url": "https://play.google.com/store/apps/details?id=com.rta.suhail",
    },
    "rta dubai": {
        "ios_url": "https://apps.apple.com/us/app/rta-dubai/id426109507",
        "android_url": "https://play.google.com/store/apps/details?id=com.rta.rtadubai",
    },
    "go taxi app": {
        "ios_url": "https://apps.apple.com/us/app/go-taxi-app-for-japan/id1254341709",
        "android_url": "https://play.google.com/store/apps/details?id=com.dena.automotive.taxibell",
    },
}


def _utc_iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _level_from_text(text: str) -> int | None:
    lowered = (text or "").lower()
    if re.search(r"level\s*4", lowered) or "do not travel" in lowered:
        return 4
    if re.search(r"level\s*3", lowered) or "reconsider travel" in lowered or "avoid non-essential travel" in lowered:
        return 3
    if re.search(r"level\s*2", lowered) or "exercise increased caution" in lowered or "high degree of caution" in lowered:
        return 2
    if re.search(r"level\s*1", lowered) or "exercise normal precautions" in lowered or "normal security precautions" in lowered:
        return 1
    if "avoid all travel" in lowered:
        return 4
    return None


def _risk_from_text(text: str | None) -> str | None:
    lowered = (text or "").lower()
    if not lowered:
        return None
    if re.search(r"level\s*4", lowered) or "do not travel" in lowered or "avoid all travel" in lowered:
        return "high"
    if re.search(r"level\s*3", lowered) or "reconsider travel" in lowered or "avoid non-essential travel" in lowered:
        return "high"
    if re.search(r"level\s*2", lowered) or "exercise increased caution" in lowered or "high degree of caution" in lowered:
        return "med"
    if re.search(r"level\s*1", lowered) or "exercise normal precautions" in lowered or "normal security precautions" in lowered:
        return "low"
    return None


def normalize_advisory_risk(
    level_value: int | str | None,
    advisory_text: str | None,
    advisory_source: str | None = None,
    *,
    is_us_destination: bool | None = None,
) -> str | None:
    level_int = None
    if isinstance(level_value, (int, float)):
        level_int = int(level_value)
    elif isinstance(level_value, str) and level_value.strip():
        match = re.search(r"\d+", level_value)
        if match:
            level_int = int(match.group(0))
        else:
            level_int = _level_from_text(level_value)

    # Prefer explicit advisory level over free-text parsing so country-level
    # risk stays consistent across report surfaces. Advisory text can include
    # subnational warnings (e.g., "do not travel" for specific regions) that
    # should not override the official country-level label.
    if level_int is not None:
        source_text = (advisory_source or "").lower()
        is_canada = "canada" in source_text or (is_us_destination is True)

        if is_canada:
            if level_int <= 0:
                return "low"
            if level_int == 1:
                return "med"
            if level_int == 2:
                return "high"
            return "high"

        if level_int <= 1:
            return "low"
        if level_int == 2:
            return "med"
        if level_int == 3:
            return "high"
        return "high"

    # Fallback: infer from text when level is missing.
    return _risk_from_text(advisory_text)


def build_travel_essentials(city_cfg: Dict[str, Any] | None, parking: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Deterministically build the Travel Essentials section.

    - Always include eSIM when `config.ESIM_CARDS_URL` is set.
    - Include parking only for US cities when a parking URL exists.

    Returns a dict: {"title": "Travel Essentials", "actions": [ ... ]}
    """
    try:
        import config
    except Exception:
        config = None

    actions: list[Dict[str, Any]] = []

    # eSIM action
    esim_url = getattr(config, "ESIM_CARDS_URL", "") if config is not None else ""
    if isinstance(esim_url, str) and esim_url.strip():
        actions.append(
            {
                "key": "esim",
                "label": getattr(config, "ESIM_CARDS_TOC_CTA", "Get an eSIM") if config is not None else "Get an eSIM",
                "blurb": getattr(config, "ESIM_CARDS_TOC_BLURB", "") if config is not None else "",
                "url": esim_url,
            }
        )

    # Parking action (US only)
    country_code = (city_cfg or {}).get("country_code", "") if isinstance(city_cfg, dict) else ""
    country_code = (country_code or "").strip().upper()
    if country_code == "US" and parking:
        url = None
        if isinstance(parking, dict):
            links = parking.get("links") or []
            if isinstance(links, list) and len(links) > 0:
                first = links[0]
                if isinstance(first, dict):
                    url = first.get("url") or url
            if not url:
                url = parking.get("url")

        if url:
            action: Dict[str, Any] = {
                "key": "parking",
                "label": "Find parking",
                "url": url,
            }
            # Keep provider and description if available
            if isinstance(parking, dict):
                if parking.get("provider"):
                    action["provider"] = parking.get("provider")
                if parking.get("description"):
                    action["description"] = parking.get("description")
            actions.append(action)

    return {"title": "Travel Essentials", "actions": actions}


def build_google_maps_category_links(city_cfg: dict, report: dict) -> dict:
    """Build generic Google Maps category links scoped to the report city label."""

    city_cfg = city_cfg if isinstance(city_cfg, dict) else {}
    report = report if isinstance(report, dict) else {}
    meta = report.get("meta") if isinstance(report.get("meta"), dict) else {}
    city_label = build_city_label(meta=meta, city_config=city_cfg, report=report)

    def _category_link(place_name: str) -> str:
        return build_google_maps_search_url(place_name, city_label) or ""

    return {
        "hospitals": _category_link("Hospitals"),
        "pharmacies": _category_link("Pharmacy"),
        "supermarkets": _category_link("Grocery store"),
        "convenience_stores": _category_link("Convenience store"),
    }


def build_important_services(city_cfg: dict, transit_list: list) -> list:
    """Build normalized important services using only city configuration."""

    city_cfg = city_cfg if isinstance(city_cfg, dict) else {}
    transit_list = transit_list if isinstance(transit_list, list) else []

    def _clean_str(value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip()

    def _normalize_url(value: Any) -> str:
        text = _clean_str(value)
        if not text:
            return ""
        if "://" not in text:
            text = f"https://{text}"
        try:
            parsed = urlparse(text)
            if not parsed.netloc:
                return ""
            normalized = parsed._replace(
                scheme=(parsed.scheme or "https").lower(),
                netloc=parsed.netloc.lower(),
            ).geturl()
            return normalized.rstrip("/")
        except Exception:
            return ""

    def _default_store_links(label: str) -> Dict[str, str]:
        query = quote_plus(label)
        return {
            "ios_url": f"https://apps.apple.com/us/iphone/search?term={query}",
            "android_url": f"https://play.google.com/store/search?c=apps&q={query}",
        }

    def _is_app_like(category: str, label: str, item: Optional[dict] = None) -> bool:
        category_key = (category or "").strip().lower()
        label_key = (label or "").strip().lower()
        if isinstance(item, dict):
            if _clean_str(item.get("ios_url")) or _clean_str(item.get("android_url")):
                return True
        if category_key in APP_LIKE_CATEGORIES:
            return True
        if "app" in category_key or "app" in label_key:
            return True
        return label_key in APP_STORE_LINKS_BY_LABEL

    def _read_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        text = _clean_str(value).lower()
        return text in {"1", "true", "yes", "on"}

    transit_websites: set[str] = set()
    for provider in transit_list:
        if not isinstance(provider, dict):
            continue
        website = provider.get("website") or provider.get("url")
        normalized = _normalize_url(website)
        if normalized:
            transit_websites.add(normalized)

    def _normalized_item(category: Any, raw: dict) -> Optional[Dict[str, Any]]:
        category_text = _clean_str(category)
        label_text = _clean_str(raw.get("label") or raw.get("name") or raw.get("title"))
        if not category_text or not label_text:
            return None

        # Deterministic, minimal filter for obvious map apps.
        if label_text.lower() in {"google maps", "google map", "apple maps", "waze"}:
            return None

        item: Dict[str, Any] = {
            "category": category_text,
            "label": label_text,
            "url": "",
            "ios_url": "",
            "android_url": "",
            "verification_status": "",
            "verification_source": "",
            "last_verified_at": "",
            "fallback_generated": False,
        }
        url_text = _clean_str(raw.get("url") or raw.get("website"))
        if url_text and _normalize_url(url_text) in transit_websites:
            return None
        if url_text:
            item["url"] = url_text

        app_links = APP_STORE_LINKS_BY_LABEL.get(label_text.lower(), {})
        app_like = _is_app_like(category_text, label_text, raw)

        ios_text = _clean_str(raw.get("ios_url"))
        android_text = _clean_str(raw.get("android_url"))
        used_generated_store_search = False

        # Precedence:
        # 1) explicit links from city config
        # 2) known APP_STORE_LINKS_BY_LABEL mappings
        # 3) generated generic store-search links (last resort only)
        if app_like:
            if not ios_text:
                ios_text = _clean_str(app_links.get("ios_url"))
            if not android_text:
                android_text = _clean_str(app_links.get("android_url"))

            if not ios_text or not android_text:
                defaults = _default_store_links(label_text)
                if not ios_text:
                    ios_text = _clean_str(defaults.get("ios_url"))
                    used_generated_store_search = True
                if not android_text:
                    android_text = _clean_str(defaults.get("android_url"))
                    used_generated_store_search = True

        if ios_text:
            item["ios_url"] = ios_text
        if android_text:
            item["android_url"] = android_text

        verification_status = _clean_str(raw.get("verification_status"))
        verification_source = _clean_str(raw.get("verification_source"))
        last_verified_at = _clean_str(raw.get("last_verified_at"))
        config_marked_fallback = _read_bool(raw.get("fallback_generated"))

        fallback_generated = bool(used_generated_store_search or config_marked_fallback)
        if fallback_generated:
            verification_status = verification_status or "generated_fallback"
            verification_source = verification_source or "store_search"
        elif _clean_str(raw.get("ios_url")) or _clean_str(raw.get("android_url")):
            verification_status = verification_status or "verified"
            verification_source = verification_source or "city_config"
        elif app_like and (ios_text or android_text):
            verification_status = verification_status or "known_mapping"
            verification_source = verification_source or "APP_STORE_LINKS_BY_LABEL"

        item["verification_status"] = verification_status
        item["verification_source"] = verification_source
        item["last_verified_at"] = last_verified_at
        item["fallback_generated"] = fallback_generated
        return item

    def _dedupe_by_category_label(entries: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
        deduped_entries: list[Dict[str, Any]] = []
        seen_keys: set[tuple[str, str]] = set()
        for entry in entries:
            category_key = _clean_str(entry.get("category")).lower()
            label_key = _clean_str(entry.get("label")).lower()
            if not category_key or not label_key:
                continue
            dedupe_key = (category_key, label_key)
            if dedupe_key in seen_keys:
                continue
            seen_keys.add(dedupe_key)
            deduped_entries.append(entry)
        return deduped_entries

    def _seed_label_from_transit_name(name: Any) -> str:
        name_text = _clean_str(name)
        if not name_text:
            return ""
        acronym_match = re.search(r"\(([^()]+)\)", name_text)
        if acronym_match:
            acronym = _clean_str(acronym_match.group(1))
            if acronym:
                name_text = acronym
        return f"{name_text} app"

    items: list[Dict[str, Any]] = []
    raw_services = city_cfg.get("important_services")

    # Format A: {"Category": [{"label": ..., "url": ...}, ...], ...}
    if isinstance(raw_services, dict):
        for category, entries in raw_services.items():
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                normalized = _normalized_item(category, entry)
                if normalized:
                    items.append(normalized)

    # Format B: [{"category": "...", "label": "...", "url": "..."}, ...]
    elif isinstance(raw_services, list):
        for entry in raw_services:
            if not isinstance(entry, dict):
                continue
            normalized = _normalized_item(entry.get("category") or entry.get("type"), entry)
            if normalized:
                items.append(normalized)

    deduped = _dedupe_by_category_label(items)

    has_local_transport_apps = any(
        _clean_str(item.get("category")).lower() == "local transportation apps"
        for item in deduped
    )
    raw_services_non_empty = isinstance(raw_services, (list, dict)) and bool(raw_services)

    if not has_local_transport_apps and raw_services_non_empty:
        transit_systems = city_cfg.get("transit_systems")
        if isinstance(transit_systems, list):
            seeded_items: list[Dict[str, Any]] = []
            for transit_system in transit_systems[:2]:
                if not isinstance(transit_system, dict):
                    continue
                seed_label = _seed_label_from_transit_name(transit_system.get("name"))
                if not seed_label:
                    continue
                seeded = _normalized_item("Local transportation apps", {"label": seed_label})
                if seeded:
                    seeded_items.append(seeded)
            if seeded_items:
                deduped.extend(seeded_items)
                deduped = _dedupe_by_category_label(deduped)

    return deduped


def build_important_services_categories(services: list[dict]) -> list[str]:
    """Return stable category ordering for important services."""

    preferred_order = [
        "Rideshare",
        "Local transportation apps",
        "Government",
        "Payments",
        "Other",
    ]

    category_lookup: Dict[str, str] = {}
    for service in services or []:
        if not isinstance(service, dict):
            continue
        category = str(service.get("category") or "").strip()
        if not category:
            continue
        key = category.lower()
        if key not in category_lookup:
            category_lookup[key] = category

    ordered: list[str] = []
    for preferred in preferred_order:
        key = preferred.lower()
        if key in category_lookup:
            ordered.append(category_lookup.pop(key))

    ordered.extend(sorted(category_lookup.values(), key=lambda value: value.lower()))
    return ordered


def make_empty_report_data(
    *,
    city_key: str,
    window_hours: int,
    generated_at: Optional[str] = None,
    coverage_status: str = "",
) -> Dict[str, Any]:
    """Return a canonical, fully-keyed report_data with empty defaults."""

    generated_at = generated_at or _utc_iso_now()

    report_data: Dict[str, Any] = {
        # Canonical sections (required)
        "executive_summary": {
            "summary": "",
            "overall_risk": "",
            "top_risk_driver": "",
            "top_actions": [],
            "whats_happening": [],
            "risk_badges": [],
            "cues": [],
        },
        "current_incidents": {
            "incidents": [],
            "sources": [],
            "coverage_status": coverage_status,
        },
        "city_essentials": {
            "hospitals": [],
            "airports": [],
            "transit": [],
            "weather_summary": None,
            "emergency_contacts": {},
            "important_services": [],
            "important_services_categories": [],
            "google_maps_category_links": {},
        },
        "supplies_services": {
            "pharmacies": [],
            "supermarkets": [],
            "convenience_stores": [],
            "rental_cars": [],
            "google_maps_category_links": {},
        },
        "opsec": {
            "baseline_tips": [],
            "contextual_tips": [],
            "driving_pack": None,
        },
        "contacts_sources": {
            "travel_advisory": {
                "text": None,
                "level": None,
                "source": None,
                "url": None,
            },
            "key_contacts": [],
            "sources": [],
        },
        "trend_data": {},
        "meta": {
            "city_key": city_key,
            "window_hours": window_hours,
            "generated_at": generated_at,
            "coverage_status": coverage_status,
            "country_code": "",
        },

        # Legacy keys (kept for existing renderer/loader compatibility)
        # These are duplicates/views over canonical sections.
        "destination": "",
        "generated_utc": "",
        "report_id": "",
        "overall_risk": "",
        "top_risk_driver": "",
        "coverage_status": coverage_status,
        "top_actions": [],
        "whats_happening": [],
        "risk_badges": [],
        "cues": [],
        "incidents": [],
        "weather_summary": None,
        "hospitals": [],
        "airports": [],
        "transit": [],
        "pharmacies": [],
        "supermarkets": [],
        "convenience_stores": [],
        "rental_cars": [],
        "important_services": [],
        "important_services_categories": [],
        "google_maps_category_links": {},
        "emergency_contacts": {},
        "opsec_section": None,
        "driving_pack": None,
        "data_coverage": "",
        "parking": None,  # SpotHero parking affiliate link for US cities
        "travel_advisory": None,
        "advisory_level": None,
        "advisory_source": None,
        "travel_advisory_url": None,
        "travel_essentials": {"title": "Travel Essentials", "actions": []},
        "country_code": "",
    }

    return report_data


def validate_report_data(report_data: Dict[str, Any]) -> None:
    """Validate report_data has the canonical structure.

    Raises:
        ValueError: if required keys are missing or structurally invalid.
    """

    if not isinstance(report_data, dict):
        raise ValueError("report_data must be a dict")

    missing = [k for k in REQUIRED_TOP_LEVEL_KEYS if k not in report_data]
    if missing:
        raise ValueError(f"report_data missing required keys: {', '.join(missing)}")

    if not isinstance(report_data.get("meta"), dict):
        raise ValueError("report_data.meta must be an object")

    meta = report_data["meta"]
    for meta_key in ("city_key", "window_hours", "generated_at", "coverage_status"):
        if meta_key not in meta:
            raise ValueError(f"report_data.meta missing required key: {meta_key}")

    # Minimal checks for travel_essentials to avoid regressions while keeping flexibility
    te = report_data.get("travel_essentials")
    if te is None:
        raise ValueError("report_data.travel_essentials is required")
    if not isinstance(te, dict):
        raise ValueError("report_data.travel_essentials must be an object")
    actions = te.get("actions")
    if actions is None or not isinstance(actions, list):
        raise ValueError("report_data.travel_essentials.actions must be a list")


def build_report_data(
    city_key: str,
    window_hours: int,
    *,
    datastore: Any = None,
    generated_at: Optional[str] = None,
) -> Dict[str, Any]:
    """Build canonical report_data from database-only inputs.

    This must not call external APIs; it loads from Supabase tables only.
    """

    from storage import get_datastore
    from report_loader import ReportDataLoader

    datastore = datastore or get_datastore()
    generated_at = generated_at or _utc_iso_now()

    loader = ReportDataLoader(datastore=datastore)
    vm = loader.load_report_data(city_key, lookback_hours=window_hours)

    report = make_empty_report_data(
        city_key=city_key,
        window_hours=window_hours,
        generated_at=generated_at,
        coverage_status=vm.coverage_status or "",
    )

    # Build canonical travel_essentials (deterministic, no AI)
    try:
        import config

        city_cfg = (getattr(config, "CITIES", {}).get(city_key, {}) or {})
    except Exception:
        city_cfg = {}

    report["travel_essentials"] = build_travel_essentials(city_cfg, getattr(vm, "parking", None))
    report["country_code"] = (city_cfg.get("country_code") or "").strip().upper()
    if isinstance(report.get("meta"), dict):
        report["meta"]["country_code"] = report["country_code"]

    # Fill canonical sections
    report["city_essentials"]["hospitals"] = vm.hospitals or []
    report["city_essentials"]["airports"] = vm.airports or []
    report["city_essentials"]["transit"] = vm.transit or []
    report["city_essentials"]["weather_summary"] = vm.weather_summary
    report["city_essentials"]["emergency_contacts"] = vm.emergency_contacts or {}

    report["supplies_services"]["pharmacies"] = vm.pharmacies or []
    report["supplies_services"]["supermarkets"] = vm.supermarkets or []
    report["supplies_services"]["convenience_stores"] = vm.convenience_stores or []
    report["supplies_services"]["rental_cars"] = vm.rental_cars or []

    # Deterministic distilled water guidance (canonical, DB-first)
    try:
        DM_WEBSITE_BY_COUNTRY = {"DE": "https://www.dm.de/", "CZ": "https://www.dm.cz/"}
        WALMART_URL = "https://www.walmart.com"
        cc = (report.get("country_code") or "").strip().upper()
        distilled_note = (
            "For medical equipment (CPAP machines, humidifiers, etc.): Distilled water may be difficult to find in some regions. "
            "Check major pharmacies and supermarkets. In emergencies, bottled water with low mineral content can be used short term, but consult equipment manufacturer guidelines."
        )
        distilled = {"title": "Distilled water availability", "note": distilled_note, "links": []}

        # Country-specific guidance
        if cc == "US":
            distilled["note"] = distilled["note"] + " In the United States, distilled water is widely available in supermarkets, pharmacies, and big box retailers such as Walmart."
            distilled["links"].append({"name": "Walmart", "website": WALMART_URL})
        elif cc == "ES":
            distilled["note"] = distilled["note"] + " In Spain, distilled water (agua destilada) is most commonly found in pharmacies (farmacias). Ask at the counter; availability in supermarkets varies."
        else:
            dm_url = DM_WEBSITE_BY_COUNTRY.get(cc)
            if dm_url:
                distilled["note"] = distilled["note"] + " In this country, dm drogerie markt may also carry distilled water. Check dm: " + dm_url
                distilled["links"].append({"name": "dm", "website": dm_url})

        report["supplies_services"]["distilled_water"] = distilled
    except Exception:
        # Best-effort only; do not fail report building on issues here
        pass

    # Top-level fields (for ReportContext schema)
    report["parking"] = vm.parking  # SpotHero parking for US cities

    # AI content may be stored in city_reports already; vm pulls it via ReportDataLoader
    exec_sum = report["executive_summary"]
    exec_sum["top_actions"] = vm.top_actions or []
    exec_sum["whats_happening"] = vm.whats_happening or []
    exec_sum["risk_badges"] = vm.risk_badges or []
    exec_sum["cues"] = vm.cues or []

    report["current_incidents"]["incidents"] = vm.incidents or []

    # Travel advisory (if any)
    advisory_url = getattr(vm, "travel_advisory_url", None)
    report["contacts_sources"]["travel_advisory"] = {
        "text": vm.travel_advisory,
        "level": vm.advisory_level,
        "source": vm.advisory_source,
        "url": advisory_url,
    }
    report["travel_advisory"] = vm.travel_advisory
    report["advisory_level"] = vm.advisory_level
    report["advisory_source"] = vm.advisory_source
    report["travel_advisory_url"] = advisory_url or report.get("travel_advisory_url")

    report["trend_data"] = vm.trend_data or {}

    # ------------------------------------------------------------------
    # DB-first enforcement for emergency + transit snapshots
    # ------------------------------------------------------------------
    missing_snapshots: list[str] = []

    # Emergency contacts must come from city_context_snapshots.context
    ctx_snapshot = None
    try:
        ctx_snapshot = datastore.get_city_context(city_key)
    except Exception:
        ctx_snapshot = None

    ctx = ctx_snapshot.context if ctx_snapshot and isinstance(getattr(ctx_snapshot, "context", None), dict) else {}
    if not ctx:
        missing_snapshots.append("city_context_snapshots")

    try:
        import config
        from emergency_numbers import normalize_emergency_contacts

        city_cfg = (getattr(config, "CITIES", {}).get(city_key, {}) or {})
        cc = (city_cfg.get("country_code") or (ctx.get("resolved_location") or {}).get("country_code") or "").strip().upper()
        resolved = resolve_emergency_numbers(city_cfg, ctx if isinstance(ctx, dict) else {}, cc)
        report["city_essentials"]["emergency_contacts"] = normalize_emergency_contacts(resolved)
        if not report.get("country_code"):
            report["country_code"] = cc
        if isinstance(report.get("meta"), dict) and not report["meta"].get("country_code"):
            report["meta"]["country_code"] = report["country_code"]
    except Exception:
        report["city_essentials"]["emergency_contacts"] = {}

    # Transit providers must come from transit_snapshots.transit['providers']
    tr_snapshot = None
    try:
        tr_snapshot = datastore.get_transit_snapshot(city_key)
    except Exception:
        tr_snapshot = None

    providers = None
    if tr_snapshot and isinstance(getattr(tr_snapshot, "transit", None), dict):
        p = tr_snapshot.transit.get("providers")
        if isinstance(p, list):
            providers = p

    if providers is not None:
        report["city_essentials"]["transit"] = providers
    else:
        missing_snapshots.append("transit_snapshots")
        # Fallback only if snapshot missing
        try:
            import config

            systems = (getattr(config, "CITIES", {}).get(city_key, {}) or {}).get("transit_systems") or []
            fallback_providers = []
            for s in systems:
                if not isinstance(s, dict) or not s.get("name"):
                    continue
                fallback_providers.append(
                    {
                        "name": s.get("name"),
                        "modes": s.get("modes", []),
                        "website": s.get("website", ""),
                    }
                )
            report["city_essentials"]["transit"] = fallback_providers
        except Exception:
            report["city_essentials"]["transit"] = []

    # Generic category links for facilities (Google Maps search URLs by city label).
    report["google_maps_category_links"] = build_google_maps_category_links(city_cfg, report)
    report["city_essentials"]["google_maps_category_links"] = {
        "hospitals": report["google_maps_category_links"].get("hospitals"),
    }
    report["supplies_services"]["google_maps_category_links"] = {
        "pharmacies": report["google_maps_category_links"].get("pharmacies"),
        "supermarkets": report["google_maps_category_links"].get("supermarkets"),
        "convenience_stores": report["google_maps_category_links"].get("convenience_stores"),
    }

    report["important_services"] = build_important_services(
        city_cfg,
        report.get("city_essentials", {}).get("transit", []),
    )
    report["important_services_categories"] = build_important_services_categories(report["important_services"])
    report["city_essentials"]["important_services"] = report["important_services"]
    report["city_essentials"]["important_services_categories"] = report["important_services_categories"]

    if missing_snapshots:
        # Record reason without changing schema.
        meta = report.get("meta") if isinstance(report.get("meta"), dict) else {}
        meta["coverage_status"] = "Partial"
        meta["coverage_reason"] = "missing snapshot"
        meta["missing_snapshots"] = missing_snapshots
        report["meta"] = meta

        msg = report.get("data_coverage") or ""
        suffix = f"Partial: missing snapshot ({', '.join(missing_snapshots)})."
        report["data_coverage"] = (msg + " " + suffix).strip() if msg else suffix

    # Meta / legacy keys for compatibility
    report["destination"] = vm.destination or ""
    report["generated_utc"] = vm.generated_utc or ""
    report["report_id"] = vm.report_id or ""
    report["top_actions"] = exec_sum["top_actions"]
    report["whats_happening"] = exec_sum["whats_happening"]
    report["risk_badges"] = exec_sum["risk_badges"]
    report["cues"] = exec_sum["cues"]
    report["incidents"] = report["current_incidents"]["incidents"]
    report["weather_summary"] = report["city_essentials"]["weather_summary"]
    report["hospitals"] = report["city_essentials"]["hospitals"]
    report["airports"] = report["city_essentials"]["airports"]
    report["transit"] = report["city_essentials"]["transit"]
    report["pharmacies"] = report["supplies_services"]["pharmacies"]
    report["supermarkets"] = report["supplies_services"]["supermarkets"]
    report["convenience_stores"] = report["supplies_services"]["convenience_stores"]
    report["rental_cars"] = report["supplies_services"]["rental_cars"]
    report["emergency_contacts"] = report["city_essentials"]["emergency_contacts"]
    report["opsec_section"] = vm.opsec
    report["driving_pack"] = vm.driving_pack
    report["data_coverage"] = vm.coverage.summary_message if getattr(vm, "coverage", None) else ""

    validate_report_data(report)
    return report


def apply_overrides(report_data: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    """Shallow-merge overrides into report_data and re-validate."""
    merged = dict(report_data)
    merged.update(overrides or {})
    validate_report_data(merged)
    return merged


def apply_overrides_with_allowlist(
    canonical: Dict[str, Any],
    ai_overrides: Dict[str, Any],
    *,
    verbose: bool = False,
) -> Dict[str, Any]:
    """Smart merge: canonical takes precedence for DB-backed lists, AI for narrative sections.
    
    Strategy:
    - DB-backed facility lists (hospitals, airports, transit, pharmacies, supermarkets,
      convenience_stores, rental_cars, emergency_contacts) ALWAYS come from canonical.
      Only fallback to AI if canonical list is empty (prevents regressions).
    - Narrative sections (overall_risk, top_risk_driver, top_actions, whats_happening,
      risk_badges, cues, opsec, driving_pack, incidents) can use AI overrides.
    - Metadata and other fields use AI overrides when present.
    
    Args:
        canonical: Base data from build_report_data() (database-sourced)
        ai_overrides: AI-generated context from synthesis
        verbose: If True, print merge decisions
        
    Returns:
        Merged dict with canonical data protected
    """
    
    # Start with canonical as base
    merged = dict(canonical)
    
    # Define facility list keys that MUST come from canonical (DB-backed)
    FACILITY_LISTS = [
        "hospitals",
        "airports",
        "transit", 
        "pharmacies",
        "supermarkets",
        "convenience_stores",
        "rental_cars",
        "emergency_contacts",
    ]
    
    # Define narrative keys that CAN use AI overrides
    NARRATIVE_ALLOWLIST = [
        "overall_risk",
        "top_risk_driver",
        "top_actions",
        "whats_happening",
        "risk_badges",
        "cues",
        "incidents",
        "opsec_section",
        "driving_pack",
        "data_coverage",
        "weather_summary",
        "destination",
        "generated_utc",
        "report_id",
        "travel_advisory",
        "advisory_level",
        "advisory_source",
        "travel_advisory_url",
        "key_contacts",
        "sources",
        "opsec",
    ]
    
    # Also allow AI overrides for canonical section nested structures
    SECTION_OVERRIDES = [
        "executive_summary",
        "current_incidents",
        "contacts_sources",
        "trend_data",
        "meta",
    ]
    
    # Handle facility lists: use canonical if non-empty, else fallback to AI
    for key in FACILITY_LISTS:
        canonical_val = canonical.get(key)
        ai_val = ai_overrides.get(key)
        
        # For lists: use canonical if it has items
        if isinstance(canonical_val, list):
            if len(canonical_val) > 0:
                merged[key] = canonical_val
                if verbose and ai_val:
                    print(f"  âœ“ {key}: using canonical ({len(canonical_val)} items)")
            elif ai_val:
                merged[key] = ai_val
                if verbose:
                    print(f"  âš  {key}: canonical empty, falling back to AI ({len(ai_val) if isinstance(ai_val, list) else 'dict'})")
        # For dicts: use canonical if it has keys
        elif isinstance(canonical_val, dict):
            if len(canonical_val) > 0:
                merged[key] = canonical_val
                if verbose and ai_val:
                    print(f"  âœ“ {key}: using canonical (dict)")
            elif ai_val:
                merged[key] = ai_val
                if verbose:
                    print(f"  âš  {key}: canonical empty, falling back to AI")
    
    # Handle narrative sections: AI can override
    advisory_keys = {"travel_advisory", "advisory_level", "advisory_source", "travel_advisory_url"}
    for key in NARRATIVE_ALLOWLIST:
        if key in ai_overrides:
            ai_value = ai_overrides[key]
            if key in advisory_keys and (ai_value is None or (isinstance(ai_value, str) and not ai_value.strip())):
                if verbose:
                    print(f"  -> {key}: keeping canonical (AI override empty)")
                continue
            if key in advisory_keys:
                canonical_value = canonical.get(key)
                if canonical_value is not None and (not isinstance(canonical_value, str) or canonical_value.strip()):
                    if verbose:
                        print(f"  -> {key}: keeping canonical official advisory value")
                    continue
            if key == "advisory_level" and isinstance(ai_value, str):
                canonical_level = canonical.get("advisory_level")
                canonical_text = canonical_level.strip() if isinstance(canonical_level, str) else ""
                ai_text = ai_value.strip()
                if canonical_text and "level" in canonical_text.lower() and ai_text and "level" not in ai_text.lower():
                    if verbose:
                        print("  -> advisory_level: keeping canonical Level X label")
                    continue
            merged[key] = ai_value
            if verbose:
                val_preview = str(ai_value)[:50]
                print(f"  -> {key}: using AI override ({val_preview}...)")

    # Handle section overrides (nested structures)
    for key in SECTION_OVERRIDES:
        if key in ai_overrides:
            # For these, we do a deeper merge to preserve canonical facility data
            if key == "city_essentials":
                # Preserve canonical facilities, allow AI weather_summary
                canonical_section = canonical.get(key, {})
                ai_section = ai_overrides[key]
                merged_section = dict(canonical_section)
                if "weather_summary" in ai_section:
                    merged_section["weather_summary"] = ai_section["weather_summary"]
                merged[key] = merged_section
            elif key == "supplies_services":
                # Preserve all canonical facility lists, but allow AI to add
                # narrative/non-facility keys (e.g., distilled_water) so that
                # helpful notes are not dropped. Canonical lists (pharmacies,
                # supermarkets, convenience_stores, rental_cars) remain DB-owned.
                canonical_section = canonical.get(key, {}) or {}
                ai_section = ai_overrides.get(key) or {}
                merged_section = dict(canonical_section)
                # Allow AI to add non-facility keys into supplies_services
                FACILITY_KEYS = {"pharmacies", "supermarkets", "convenience_stores", "rental_cars"}
                if isinstance(ai_section, dict):
                    for sub_k, sub_v in ai_section.items():
                        if sub_k not in FACILITY_KEYS:
                            merged_section[sub_k] = sub_v
                            if verbose:
                                print(f"  â†’ supplies_services.{sub_k}: added from AI override")
                merged[key] = merged_section
            else:
                # For other sections, allow AI override
                merged[key] = ai_overrides[key]
    
    # Add any additional fields from AI that aren't in canonical
    for key, value in ai_overrides.items():
        if key not in merged and key not in FACILITY_LISTS:
            merged[key] = value

    def _is_display_advisory_label(value: object) -> bool:
        if not isinstance(value, str):
            return False
        label = value.strip().lower()
        if not label:
            return False
        return "level" in label or label.startswith("exercise ") or label.startswith("avoid ")

    def _parse_advisory_level_value(value: object, *, is_canada: bool) -> int | None:
        if isinstance(value, (int, float)):
            return int(value)
        if not isinstance(value, str) or not value.strip():
            return None

        lowered = value.strip().lower()
        match = re.search(r"\d+", lowered)
        if match:
            return int(match.group(0))

        if is_canada:
            if "normal security precautions" in lowered:
                return 0
            if "high degree of caution" in lowered:
                return 1
            if "avoid non essential travel" in lowered or "avoid non-essential travel" in lowered:
                return 2
            if "avoid all travel" in lowered:
                return 3
            return None

        if "exercise normal precautions" in lowered:
            return 1
        if "exercise increased caution" in lowered:
            return 2
        if "reconsider travel" in lowered:
            return 3
        if "do not travel" in lowered:
            return 4
        return _level_from_text(lowered)

    def _advisory_level_display(level_value: int | None, *, is_canada: bool) -> str | None:
        if level_value is None:
            return None
        if is_canada:
            mapping = {
                0: "Exercise normal security precautions",
                1: "Exercise a high degree of caution",
                2: "Avoid non essential travel",
                3: "Avoid all travel",
            }
            level_value = max(0, min(level_value, 3))
            return mapping.get(level_value)

        mapping = {
            1: "Level 1: Exercise Normal Precautions",
            2: "Level 2: Exercise Increased Caution",
            3: "Level 3: Reconsider Travel",
            4: "Level 4: Do not travel",
        }
        level_value = max(1, min(level_value, 4))
        return mapping.get(level_value)

    def _normalize_advisory_level(
        advisory_level: object,
        advisory_text: str,
        advisory_source: str,
    ) -> tuple[str | None, int | None]:
        source_text = (advisory_source or "").lower()
        is_canada = "canada" in source_text

        if _is_display_advisory_label(advisory_level):
            label = str(advisory_level).strip()
            level_value = _parse_advisory_level_value(label, is_canada=is_canada)
            return label, level_value

        level_value = _parse_advisory_level_value(advisory_level, is_canada=is_canada)
        if level_value is None:
            level_value = _parse_advisory_level_value(advisory_text, is_canada=is_canada)

        label = _advisory_level_display(level_value, is_canada=is_canada)
        return label, level_value

    contacts = merged.get("contacts_sources")
    if not isinstance(contacts, dict):
        contacts = {}
        merged["contacts_sources"] = contacts

    travel = contacts.get("travel_advisory")
    if not isinstance(travel, dict):
        travel = {}
    contacts["travel_advisory"] = travel

    advisory_text_value = merged.get("travel_advisory")
    if advisory_text_value is None or (isinstance(advisory_text_value, str) and not advisory_text_value.strip()):
        advisory_text_value = travel.get("text")
    if isinstance(advisory_text_value, str) and advisory_text_value.strip():
        advisory_text_value = advisory_text_value.strip()
        merged["travel_advisory"] = advisory_text_value

    advisory_source_value = merged.get("advisory_source")
    if advisory_source_value is None or (isinstance(advisory_source_value, str) and not advisory_source_value.strip()):
        advisory_source_value = travel.get("source")
    if isinstance(advisory_source_value, str) and advisory_source_value.strip():
        advisory_source_value = advisory_source_value.strip()
        merged["advisory_source"] = advisory_source_value

    advisory_url_value = merged.get("travel_advisory_url")
    if advisory_url_value is None or (isinstance(advisory_url_value, str) and not advisory_url_value.strip()):
        advisory_url_value = travel.get("url")
    if isinstance(advisory_url_value, str) and advisory_url_value.strip():
        advisory_url_value = advisory_url_value.strip()
        merged["travel_advisory_url"] = advisory_url_value

    advisory_level_candidate = merged.get("advisory_level")
    if advisory_level_candidate is None or (isinstance(advisory_level_candidate, str) and not advisory_level_candidate.strip()):
        advisory_level_candidate = travel.get("level")

    advisory_text_for_level = " ".join(
        part
        for part in [
            advisory_text_value if isinstance(advisory_text_value, str) else "",
            advisory_level_candidate if isinstance(advisory_level_candidate, str) else "",
        ]
        if part and part.strip()
    )
    level_label, level_value = _normalize_advisory_level(
        advisory_level_candidate,
        advisory_text_for_level,
        merged.get("advisory_source", ""),
    )
    if level_label:
        merged["advisory_level"] = level_label

    # Keep canonical advisory fields populated and use display label strings.
    travel["text"] = merged.get("travel_advisory")
    travel["level"] = merged.get("advisory_level")
    travel["source"] = merged.get("advisory_source")
    if merged.get("travel_advisory_url"):
        travel["url"] = merged.get("travel_advisory_url")

    advisory_text_for_risk = " ".join(
        part
        for part in [
            merged.get("travel_advisory"),
            merged.get("advisory_level"),
            travel.get("text"),
        ]
        if isinstance(part, str) and part.strip()
    )

    normalized_risk = normalize_advisory_risk(
        level_value,
        advisory_text_for_risk,
        advisory_source=merged.get("advisory_source"),
    )
    if normalized_risk:
        merged["overall_risk"] = normalized_risk

    exec_sum = merged.get("executive_summary")
    if isinstance(exec_sum, dict):
        if merged.get("overall_risk"):
            exec_sum["overall_risk"] = merged.get("overall_risk")
        if not exec_sum.get("top_risk_driver") and merged.get("top_risk_driver"):
            exec_sum["top_risk_driver"] = merged.get("top_risk_driver")

    # Validate and return
    # Normalize recency strings in incidents to a strict format
    def _normalize_recency_for_list(inc_list: list):
        from datetime import datetime, timezone

        if not isinstance(inc_list, list):
            return
        today = datetime.now(timezone.utc).date()

        for inc in inc_list:
            if not isinstance(inc, dict):
                continue
            # Prefer canonical 'date' field (YYYY-MM-DD); fallback to published/published_at
            date_text = inc.get('date') or inc.get('published') or inc.get('timestamp') or ""
            try:
                # Expect YYYY-MM-DD or ISO formats
                if isinstance(date_text, str) and len(date_text) >= 10:
                    d = date_text[:10]
                    y, m, day = int(d[0:4]), int(d[5:7]), int(d[8:10])
                    item_date = datetime(y, m, day, tzinfo=timezone.utc).date()
                    delta = (item_date - today).days
                    if delta == 0:
                        rec = "today"
                    elif delta > 0:
                        rec = f"in {delta} day" if delta == 1 else f"in {delta} days"
                    else:
                        days = abs(delta)
                        rec = f"{days} day ago" if days == 1 else f"{days} days ago"
                else:
                    rec = None
            except Exception:
                rec = None

            # Overwrite recency with canonical value or remove if unknown
            if rec:
                inc['recency'] = rec
            else:
                # Ensure recency is either absent or empty string (keep empty for templates)
                inc['recency'] = ""

    # Normalize for both canonical and legacy locations
    try:
        # current_incidents.incidents
        curr = merged.get('current_incidents') or {}
        _normalize_recency_for_list(curr.get('incidents', []))
        # legacy top-level incidents list
        _normalize_recency_for_list(merged.get('incidents', []))
    except Exception:
        pass

    canonical_cov = canonical.get("coverage_status") or ""
    if canonical_cov:
        resolved_cov = canonical_cov
        prefix = "⚠️ Partial coverage: missing "
        if isinstance(canonical_cov, str) and canonical_cov.startswith(prefix):
            missing = [part.strip() for part in canonical_cov[len(prefix):].split(",") if part.strip()]

            incidents = merged.get("incidents")
            if isinstance(incidents, list) and len(incidents) > 0 and "incidents" in missing:
                missing = [section for section in missing if section != "incidents"]

            weather_value = merged.get("weather_summary")
            weather_present = False
            if isinstance(weather_value, dict):
                weather_present = any(
                    isinstance(v, str) and v.strip()
                    or (v is not None and not isinstance(v, str))
                    for v in weather_value.values()
                )
            elif isinstance(weather_value, str):
                weather_present = bool(weather_value.strip())
            elif weather_value is not None:
                weather_present = True

            if weather_present and "weather" in missing:
                missing = [section for section in missing if section != "weather"]

            if not missing:
                resolved_cov = "✅ Coverage complete"
            else:
                resolved_cov = f"{prefix}{', '.join(sorted(missing))}"

        merged["coverage_status"] = resolved_cov
        meta = merged.get("meta") if isinstance(merged.get("meta"), dict) else {}
        meta["coverage_status"] = resolved_cov
        merged["meta"] = meta

        curr = merged.get("current_incidents") if isinstance(merged.get("current_incidents"), dict) else {}
        curr["coverage_status"] = resolved_cov
        merged["current_incidents"] = curr

    # Protect important services and category-link fields from AI override drift.
    if not isinstance(merged.get("city_essentials"), dict):
        merged["city_essentials"] = {}
    if not isinstance(merged.get("supplies_services"), dict):
        merged["supplies_services"] = {}

    canonical_city_essentials = (
        canonical.get("city_essentials") if isinstance(canonical.get("city_essentials"), dict) else {}
    )
    canonical_supplies_services = (
        canonical.get("supplies_services") if isinstance(canonical.get("supplies_services"), dict) else {}
    )

    merged["important_services"] = canonical.get("important_services", []) or []
    merged["important_services_categories"] = canonical.get("important_services_categories", []) or []
    merged["google_maps_category_links"] = canonical.get("google_maps_category_links", {}) or {}
    merged["city_essentials"]["important_services"] = (
        canonical_city_essentials.get("important_services", []) or []
    )
    merged["city_essentials"]["important_services_categories"] = (
        canonical_city_essentials.get("important_services_categories", []) or []
    )
    merged["city_essentials"]["google_maps_category_links"] = (
        canonical_city_essentials.get("google_maps_category_links", {}) or {}
    )
    merged["supplies_services"]["google_maps_category_links"] = (
        canonical_supplies_services.get("google_maps_category_links", {}) or {}
    )

    validate_report_data(merged)
    return merged


def check_website_regression(
    canonical: Dict[str, Any],
    merged: Dict[str, Any],
    *,
    verbose: bool = True,
) -> list[str]:
    """Check if any facilities with websites in canonical lost them in merge.
    
    Returns list of warning messages for any regressions found.
    """
    warnings = []
    
    FACILITY_CATEGORIES = [
        ("hospitals", "Hospital"),
        ("airports", "Airport"),
        ("pharmacies", "Pharmacy"),
        ("supermarkets", "Supermarket"),
        ("convenience_stores", "Convenience Store"),
        ("rental_cars", "Rental Car"),
        ("transit", "Transit"),
    ]
    
    for list_key, category_name in FACILITY_CATEGORIES:
        canonical_list = canonical.get(list_key, [])
        merged_list = merged.get(list_key, [])
        
        if not isinstance(canonical_list, list) or not isinstance(merged_list, list):
            continue
        
        # Build lookup by name
        merged_by_name = {item.get("name", "").lower(): item for item in merged_list if isinstance(item, dict)}
        
        for canonical_item in canonical_list:
            if not isinstance(canonical_item, dict):
                continue
                
            name = canonical_item.get("name", "")
            canonical_website = canonical_item.get("website", "")
            
            if not canonical_website or not name:
                continue
            
            # Check if same facility in merged has lost website
            merged_item = merged_by_name.get(name.lower())
            if merged_item:
                merged_website = merged_item.get("website", "")
                if not merged_website:
                    msg = f"WARN: Website lost in merge for {name} ({category_name}): canonical had {canonical_website}"
                    warnings.append(msg)
                    if verbose:
                        print(f"  âš  {msg}")
    
    return warnings
