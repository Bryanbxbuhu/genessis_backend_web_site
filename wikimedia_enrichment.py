import os
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests

_ENRICHMENT_FLAGS = {
    "minneapolis": "MINNEAPOLIS_WIKIMEDIA_ENRICHMENT_ENABLED",
    "barcelona": "BARCELONA_WIKIMEDIA_ENRICHMENT_ENABLED",
    "berlin": "BERLIN_WIKIMEDIA_ENRICHMENT_ENABLED",
    "dubrovnik": "DUBROVNIK_WIKIMEDIA_ENRICHMENT_ENABLED",
    "los-angeles": "LOS_ANGELES_WIKIMEDIA_ENRICHMENT_ENABLED",
    "dubai": "DUBAI_WIKIMEDIA_ENRICHMENT_ENABLED",
    "mexico-city": "MEXICO_CITY_WIKIMEDIA_ENRICHMENT_ENABLED",
}

_BLOCKED_DOMAINS = {
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "tiktok.com",
    "yelp.com",
    "tripadvisor.com",
    "foursquare.com",
    "wikipedia.org",
    "wikidata.org",
    "wikimedia.org",
}

_CACHE: Dict[str, Tuple[Optional[str], Optional[float], Optional[str]]] = {}


def _env_flag_enabled(name: str) -> bool:
    raw = os.getenv(name, "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _normalize_city_key(value: str) -> str:
    normalized = (value or "").strip().lower()
    return re.sub(r"[\s_]+", "-", normalized)


def is_wikimedia_enrichment_enabled(city_key: str) -> bool:
    normalized = _normalize_city_key(city_key)
    if not normalized:
        return False
    client_id = os.getenv("WIKIMEDIA_CLIENT_ID")
    client_secret = os.getenv("WIKIMEDIA_CLIENT_SECRET")
    if not (client_id and client_secret):
        return False

    if _env_flag_enabled("WIKIMEDIA_ENRICHMENT_ENABLED"):
        allowlist_raw = os.getenv("WIKIMEDIA_ENRICHMENT_CITY_ALLOWLIST", "")
        allowlist = {item.strip().lower() for item in allowlist_raw.split(",") if item.strip()}
        if allowlist and normalized not in allowlist:
            return False
        return True

    flag_name = _ENRICHMENT_FLAGS.get(normalized)
    if not flag_name:
        return False
    return _env_flag_enabled(flag_name)


def _request_headers() -> Dict[str, str]:
    client_id = os.getenv("WIKIMEDIA_CLIENT_ID", "unknown")
    return {"User-Agent": f"OSINT-Project Wikimedia enrichment ({client_id})"}


def _build_city_hint(city_key: str) -> Optional[str]:
    normalized = _normalize_city_key(city_key)
    if not normalized:
        return None
    try:
        import config
        from helpers.country_metadata import get_country_name_en
    except Exception:
        return None
    city_config = (getattr(config, "CITIES", {}) or {}).get(normalized) or {}
    display_name = (city_config.get("display_name") or city_config.get("name") or "").strip()
    if not display_name:
        display_name = normalized
    country_code = (city_config.get("country_code") or "").strip().upper()
    country_name = get_country_name_en(country_code) if country_code else ""
    if country_name and country_name.lower() not in display_name.lower():
        return f"{display_name} {country_name}".strip()
    return display_name or None


def _normalize_host(value: str) -> str:
    try:
        parsed = urlparse(value)
    except Exception:
        return ""
    host = (parsed.netloc or "").lower().strip()
    if host.startswith("www."):
        host = host[4:]
    return host


def _is_http_url(value: str) -> bool:
    try:
        parsed = urlparse(value)
    except Exception:
        return False
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _is_blocked_domain(value: str) -> bool:
    host = _normalize_host(value)
    if not host:
        return True
    for domain in _BLOCKED_DOMAINS:
        if host == domain or host.endswith(f".{domain}"):
            return True
    return False


def _urls_equivalent(left: str, right: str) -> bool:
    left_host = _normalize_host(left)
    right_host = _normalize_host(right)
    if not left_host or not right_host:
        return False
    return left_host == right_host


def _simple_confidence(place_name: str, label: str) -> Optional[float]:
    left = set(re.findall(r"[a-z0-9]+", (place_name or "").lower()))
    right = set(re.findall(r"[a-z0-9]+", (label or "").lower()))
    if not left or not right:
        return None
    return round(len(left & right) / len(left | right), 2)


def _wikidata_search(query: str) -> Optional[Dict[str, Any]]:
    url = "https://www.wikidata.org/w/api.php"
    params = {
        "action": "wbsearchentities",
        "search": query,
        "language": "en",
        "format": "json",
        "limit": 1,
    }
    try:
        resp = requests.get(url, params=params, headers=_request_headers(), timeout=10)
        resp.raise_for_status()
        payload = resp.json()
    except Exception:
        return None
    results = payload.get("search") if isinstance(payload, dict) else None
    if not isinstance(results, list) or not results:
        return None
    first = results[0]
    return first if isinstance(first, dict) else None


def _wikidata_official_website(entity_id: str) -> Optional[str]:
    if not entity_id:
        return None
    url = "https://www.wikidata.org/w/api.php"
    params = {
        "action": "wbgetentities",
        "ids": entity_id,
        "props": "claims",
        "format": "json",
    }
    try:
        resp = requests.get(url, params=params, headers=_request_headers(), timeout=10)
        resp.raise_for_status()
        payload = resp.json()
    except Exception:
        return None
    entities = payload.get("entities") if isinstance(payload, dict) else None
    if not isinstance(entities, dict):
        return None
    entity = entities.get(entity_id) if isinstance(entities.get(entity_id), dict) else None
    if not entity:
        return None
    claims = entity.get("claims")
    if not isinstance(claims, dict):
        return None
    websites = claims.get("P856")
    if not isinstance(websites, list):
        return None
    for claim in websites:
        if not isinstance(claim, dict):
            continue
        mainsnak = claim.get("mainsnak")
        if not isinstance(mainsnak, dict):
            continue
        datavalue = mainsnak.get("datavalue")
        if not isinstance(datavalue, dict):
            continue
        value = datavalue.get("value")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _resolve_redirect_once(url: str) -> Optional[str]:
    try:
        resp = requests.get(url, allow_redirects=False, headers=_request_headers(), timeout=10)
    except Exception:
        return None
    if resp.is_redirect:
        location = resp.headers.get("Location")
        if location:
            return urljoin(url, location)
    return url


def _lookup_official_website(
    place_name: str,
    city_hint: Optional[str],
) -> Tuple[Optional[str], Optional[float], Optional[str]]:
    name_key = (place_name or "").strip().lower()
    if not name_key:
        return None, None, None
    hint_key = (city_hint or "").strip().lower()
    cache_key = f"{name_key}|{hint_key}" if hint_key else name_key
    if cache_key in _CACHE:
        return _CACHE[cache_key]

    search_queries = []
    if city_hint:
        search_queries.append(f"{place_name} {city_hint}".strip())
    search_queries.append(place_name)
    deduped_queries = []
    seen = set()
    for query in search_queries:
        if not query:
            continue
        lowered = query.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        deduped_queries.append(query)
    search_queries = deduped_queries
    result = None
    for query in search_queries:
        result = _wikidata_search(query)
        if result:
            break
    if not result:
        _CACHE[cache_key] = (None, None, None)
        return _CACHE[cache_key]

    entity_id = result.get("id") if isinstance(result, dict) else None
    label = result.get("label") if isinstance(result, dict) else None
    website = _wikidata_official_website(entity_id or "")
    confidence = _simple_confidence(place_name, label or "") if label else None
    _CACHE[cache_key] = (website, confidence, label)
    return _CACHE[cache_key]


def enrich_places_with_wikimedia(
    city_key: str,
    places: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not is_wikimedia_enrichment_enabled(city_key):
        return places
    city_hint = _build_city_hint(city_key)

    for place in places:
        if not isinstance(place, dict):
            continue
        name = (place.get("name") or "").strip()
        if not name:
            continue
        website, confidence, _label = _lookup_official_website(name, city_hint)
        if not website:
            continue
        if not _is_http_url(website) or _is_blocked_domain(website):
            continue

        final_url = _resolve_redirect_once(website) or website
        if not _is_http_url(final_url) or _is_blocked_domain(final_url):
            continue

        existing = (place.get("website") or "").strip()
        if existing:
            if not _urls_equivalent(existing, final_url):
                place["wikimedia_conflict"] = True
                print(
                    "   WARN Wikimedia conflict:",
                    f"{name} existing={existing} candidate={final_url}",
                )
            continue

        place["wikimedia_candidate_website"] = website
        place["wikimedia_candidate_website_final"] = final_url
        place["wikimedia_confidence"] = confidence

    return places
