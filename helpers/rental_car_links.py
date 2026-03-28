"""
Canonical rental car provider links by country, with validation and allowlisting.
"""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import re
import requests


_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
_TRACKING_KEYS = {
    "gclid",
    "fbclid",
    "msclkid",
    "igshid",
    "mc_eid",
    "mc_cid",
}
_TRACKING_PREFIXES = ("utm_",)
_PREFER_GLOBAL_COUNTRIES = {"PT"}
_COUNTRY_TLD_OVERRIDES = {
    "GB": "UK",
}


def _budget_global(country_iso2: str, _: str) -> Optional[str]:
    if not country_iso2:
        return None
    return f"https://www.budget.com/en/locations/{country_iso2.lower()}"


def _avis_global(country_iso2: str, _: str) -> Optional[str]:
    if not country_iso2:
        return None
    return f"https://www.avis.com/en/locations/{country_iso2.lower()}"


def _enterprise_global(country_iso2: str, _: str) -> Optional[str]:
    if not country_iso2:
        return None
    slug = country_iso2.lower()
    if slug == "gb":
        slug = "uk"
    return f"https://www.enterprise.com/en/car-rental-locations/{slug}.html"


def _hertz_global(_: str, country_name_en: str) -> Optional[str]:
    slug = _slugify_country(country_name_en)
    if not slug:
        return None
    return f"https://www.hertz.com/us/en/location/{slug}"


PROVIDER_ORDER = [
    "enterprise",
    "avis",
    "budget",
    "hertz",
    "europcar",
]


PROVIDER_CONFIG: Dict[str, Dict[str, object]] = {
    "budget": {
        "label": "Budget",
        "fallback_url": "https://www.budget.com",
        "domains": {"budget.com", "budget.fr", "budget.es", "budget.pt", "budget.co.uk"},
        "global_patterns": [_budget_global],
        "country_overrides": {
            "FR": ["https://www.budget.fr"],
            "ES": ["https://www.budget.es"],
            "PT": ["https://www.budget.pt"],
            "GB": ["https://www.budget.co.uk"],
        },
    },
    "avis": {
        "label": "Avis",
        "fallback_url": "https://www.avis.com",
        "domains": {"avis.com", "avis.fr", "avis.es", "avis.pt", "avis.co.uk"},
        "global_patterns": [_avis_global],
        "country_overrides": {
            "FR": ["https://www.avis.fr"],
            "ES": ["https://www.avis.es"],
            "PT": ["https://www.avis.pt"],
            "GB": ["https://www.avis.co.uk"],
        },
    },
    "enterprise": {
        "label": "Enterprise",
        "fallback_url": "https://www.enterprise.com",
        "domains": {"enterprise.com", "enterprise.fr", "enterprise.es", "enterprise.pt"},
        "global_patterns": [_enterprise_global],
        "country_overrides": {
            "FR": ["https://www.enterprise.fr/en/home.html"],
            "ES": ["https://www.enterprise.es"],
            "PT": ["https://www.enterprise.pt"],
            "GB": ["https://www.enterprise.com/en/car-rental-locations/uk.html"],
        },
    },
    "hertz": {
        "label": "Hertz",
        "fallback_url": "https://www.hertz.com",
        "domains": {"hertz.com", "hertz.co.uk"},
        "global_patterns": [_hertz_global],
        "country_overrides": {
            "GB": ["https://www.hertz.co.uk"],
        },
    },
    "europcar": {
        "label": "Europcar",
        "fallback_url": "https://www.europcar.com",
        "domains": {"europcar.com", "europcar.fr", "europcar.es", "europcar.pt", "europcar.co.uk"},
        "global_patterns": [],
        "country_overrides": {
            "FR": ["https://www.europcar.fr"],
            "ES": ["https://www.europcar.es"],
            "PT": ["https://www.europcar.pt"],
            "GB": ["https://www.europcar.co.uk"],
        },
    },
}


def resolve_rental_car_links(country_iso2: str, country_name_en: str) -> List[Dict[str, str]]:
    """
    Resolve canonical rental car links for a country, validating each provider URL.

    Returns a list of dicts with keys: provider, label, url.
    """
    return _resolve_rental_car_links(country_iso2, country_name_en, validate=True)


def resolve_rental_car_links_unvalidated(country_iso2: str, country_name_en: str) -> List[Dict[str, str]]:
    """
    Resolve canonical rental car links without network validation.
    Useful for report rendering where external calls are undesirable.
    """
    return _resolve_rental_car_links(country_iso2, country_name_en, validate=False)


def detect_rental_car_provider(name: str) -> Optional[str]:
    """Detect a known rental car provider from a name."""
    if not name:
        return None

    normalized = name.lower()
    normalized = re.sub(r"[^a-z0-9\s]", " ", normalized)
    normalized = re.sub(
        r"\b(rent|rental|car|cars|rentacar|rent a car|rent-a-car)\b",
        " ",
        normalized,
    )
    normalized = re.sub(r"\s+", " ", normalized).strip()

    for provider in PROVIDER_ORDER:
        if provider in normalized:
            return provider
    return None


def dedupe_rental_car_entries(
    entries: Iterable[Dict[str, Optional[str]]],
    country_iso2: str,
    country_name_en: str,
) -> List[Dict[str, str]]:
    """
    Deduplicate rental car entries by provider, then by canonical URL.
    Unknown providers are dropped to enforce allowlisted links.
    """
    entries = list(entries or [])
    country_iso2 = (country_iso2 or "").strip().upper()

    resolved = resolve_rental_car_links_unvalidated(country_iso2, country_name_en)
    provider_urls = {item["provider"]: item["url"] for item in resolved}
    provider_labels = {item["provider"]: item["label"] for item in resolved}

    best_by_provider: Dict[str, Dict[str, Optional[str]]] = {}
    for entry in entries:
        provider = detect_rental_car_provider(entry.get("name", "") or "")
        if not provider:
            continue
        current = best_by_provider.get(provider)
        if current is None or _entry_score(entry, provider, country_iso2) > _entry_score(current, provider, country_iso2):
            best_by_provider[provider] = entry

    result: List[Dict[str, str]] = []
    for provider in PROVIDER_ORDER:
        if provider not in best_by_provider:
            continue
        entry = best_by_provider[provider]
        website = entry.get("website") or ""
        if _should_prefer_country_override(provider, website, country_iso2):
            website = provider_urls.get(provider, "") or website
        if not is_provider_url_allowed(provider, website, country_iso2):
            website = provider_urls.get(provider, "") or fallback_url_for_provider(provider)
        website = _normalize_url(website) if website else ""

        result.append(
            {
                "name": provider_labels.get(provider, provider.title()),
                "address": entry.get("address") or entry.get("notes") or "",
                "website": website,
            }
        )

    # Deduplicate by canonical URL host
    seen_hosts = set()
    deduped: List[Dict[str, str]] = []
    for item in result:
        host = _host_key(item.get("website", ""))
        if host and host in seen_hosts:
            continue
        if host:
            seen_hosts.add(host)
        deduped.append(item)

    return deduped


def allowed_domains_for_provider(provider: str) -> set[str]:
    config = PROVIDER_CONFIG.get(provider, {})
    return set(config.get("domains", set()))


def fallback_url_for_provider(provider: str) -> str:
    config = PROVIDER_CONFIG.get(provider, {})
    return str(config.get("fallback_url", "") or "")


def is_provider_url_allowed(provider: str, url: str, country_iso2: str) -> bool:
    """Check if a URL is allowlisted for a provider and matches country constraints."""
    if not url:
        return False
    allowed_domains = allowed_domains_for_provider(provider)
    host = _extract_host(url)
    if not _host_matches_allowed(host, allowed_domains):
        return False
    return _is_country_compatible_host(host, country_iso2)


def _resolve_rental_car_links(
    country_iso2: str,
    country_name_en: str,
    *,
    validate: bool,
) -> List[Dict[str, str]]:
    country_iso2 = (country_iso2 or "").strip().upper()
    country_name_en = (country_name_en or "").strip()

    results: List[Dict[str, str]] = []
    for provider in PROVIDER_ORDER:
        config = PROVIDER_CONFIG.get(provider, {})
        label = str(config.get("label", provider.title()))
        url = _resolve_provider_url(provider, country_iso2, country_name_en, validate=validate)
        results.append({"provider": provider, "label": label, "url": url})
    return results


def _resolve_provider_url(
    provider: str,
    country_iso2: str,
    country_name_en: str,
    *,
    validate: bool,
) -> str:
    config = PROVIDER_CONFIG.get(provider, {})
    allowed_domains = allowed_domains_for_provider(provider)

    for candidate in _candidate_urls(provider, country_iso2, country_name_en):
        if not candidate:
            continue
        if validate:
            validated = _validate_url(candidate, allowed_domains)
            if validated:
                return validated
        else:
            normalized = _normalize_url(candidate)
            if _host_matches_allowed(_extract_host(normalized), allowed_domains):
                return normalized

    return _normalize_url(fallback_url_for_provider(provider))


def _candidate_urls(provider: str, country_iso2: str, country_name_en: str) -> List[str]:
    config = PROVIDER_CONFIG.get(provider, {})
    candidates: List[str] = []

    for builder in config.get("global_patterns", []):
        url = builder(country_iso2, country_name_en)
        if url:
            candidates.append(url)

    overrides = config.get("country_overrides", {})
    for url in overrides.get(country_iso2, []):
        if url not in candidates:
            candidates.append(url)

    return candidates


def _validate_url(url: str, allowed_domains: Iterable[str]) -> Optional[str]:
    try:
        session = requests.Session()
        session.headers.update({"User-Agent": _USER_AGENT})

        response = None
        try:
            response = session.head(url, allow_redirects=True, timeout=6)
            if not (200 <= response.status_code < 400):
                response = None
        except Exception:
            response = None

        if response is None:
            try:
                response = session.get(url, allow_redirects=True, timeout=6)
                if not (200 <= response.status_code < 400):
                    return None
            except Exception:
                return None

        final_url = response.url or url
        final_url = _normalize_url(final_url)
        if not _host_matches_allowed(_extract_host(final_url), allowed_domains):
            return None
        return final_url
    except Exception:
        return None


def _normalize_url(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url.strip())
    if not parsed.scheme:
        parsed = urlparse(f"https://{url.strip()}")
    query = _strip_tracking_params(parsed.query)
    return urlunparse(
        (
            "https",
            parsed.netloc.lower(),
            parsed.path or "",
            "",
            query,
            "",
        )
    )


def _strip_tracking_params(query: str) -> str:
    if not query:
        return ""
    filtered = []
    for key, value in parse_qsl(query, keep_blank_values=True):
        key_lower = key.lower()
        if key_lower in _TRACKING_KEYS or key_lower.startswith(_TRACKING_PREFIXES):
            continue
        filtered.append((key, value))
    return urlencode(filtered, doseq=True)


def _extract_host(url: str) -> str:
    parsed = urlparse(url if "://" in url else f"https://{url}")
    return (parsed.hostname or "").lower()


def _host_matches_allowed(host: str, allowed_domains: Iterable[str]) -> bool:
    if not host:
        return False
    host = host.lower()
    if host.startswith("www."):
        host = host[4:]
    for domain in allowed_domains:
        domain = domain.lower()
        if host == domain or host.endswith(f".{domain}"):
            return True
    return False


def _is_country_compatible_host(host: str, country_iso2: str) -> bool:
    country_iso2 = (country_iso2 or "").strip().upper()
    if not host or not country_iso2:
        return True
    if host.startswith("www."):
        host = host[4:]
    suffix = host.split(".")[-1]
    if len(suffix) == 2 and suffix.isalpha():
        target = _COUNTRY_TLD_OVERRIDES.get(country_iso2, country_iso2)
        return suffix.upper() == target
    return True


def _host_is_global(host: str) -> bool:
    if host.startswith("www."):
        host = host[4:]
    suffix = host.split(".")[-1]
    return suffix in {"com", "net", "org"}


def _host_key(url: str) -> str:
    host = _extract_host(url)
    if host.startswith("www."):
        host = host[4:]
    return host


def _should_prefer_country_override(provider: str, website: str, country_iso2: str) -> bool:
    country_iso2 = (country_iso2 or "").strip().upper()
    if not country_iso2 or not website:
        return False
    if country_iso2 in _PREFER_GLOBAL_COUNTRIES:
        return False
    overrides = PROVIDER_CONFIG.get(provider, {}).get("country_overrides", {})
    if not overrides or not overrides.get(country_iso2):
        return False
    return _host_is_global(_extract_host(website))


def _slugify_country(country_name_en: str) -> str:
    if not country_name_en:
        return ""
    text = country_name_en.lower().strip()
    text = re.sub(r"[^a-z0-9\s-]", "", text)
    text = re.sub(r"\s+", "-", text)
    return text.strip("-")


def _entry_score(entry: Dict[str, Optional[str]], provider: str, country_iso2: str) -> tuple:
    website = entry.get("website") or ""
    allowed = is_provider_url_allowed(provider, website, country_iso2)
    global_preferred = False
    if country_iso2 in _PREFER_GLOBAL_COUNTRIES and website:
        global_preferred = _host_is_global(_extract_host(website))
    return (
        1 if allowed else 0,
        1 if global_preferred else 0,
        1 if website else 0,
        len(entry.get("address") or entry.get("notes") or ""),
    )
