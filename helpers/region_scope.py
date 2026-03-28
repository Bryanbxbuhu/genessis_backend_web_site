from __future__ import annotations

from typing import Dict, List, Optional


REGION_CITY_KEYS: Dict[str, List[str]] = {
    "americas": [
        "miami",
        "new-york",
        "san-francisco",
        "los-angeles",
        "minneapolis",
        "mexico-city",
    ],
    "europe": [
        "lisbon",
        "prague",
        "dubrovnik",
        "paris",
        "madrid",
        "barcelona",
        "sevilla",
        "london",
        "rome",
        "berlin",
        "dubai",
        "moscow",
    ],
    "asia": [
        "tokyo",
        "delhi",
    ],
}

REGION_ALIASES = {
    "us": "americas",
    "america": "americas",
    "americas": "americas",
    "europe": "europe",
    "asia": "asia",
}


def normalize_region(region: Optional[str]) -> Optional[str]:
    if region is None:
        return None
    value = str(region).strip().lower()
    if not value:
        return None
    normalized = REGION_ALIASES.get(value)
    if normalized:
        return normalized
    valid = ", ".join(sorted(REGION_CITY_KEYS.keys()))
    raise ValueError(f"Invalid region '{region}'. Valid values: {valid}")


def select_city_keys_for_region(cities: Dict[str, Dict], region: Optional[str]) -> List[str]:
    normalized_region = normalize_region(region)

    enabled_city_keys = [
        city_key
        for city_key, city_cfg in (cities or {}).items()
        if isinstance(city_cfg, dict) and city_cfg.get("enabled", True)
    ]
    if normalized_region is None:
        return enabled_city_keys

    requested = REGION_CITY_KEYS.get(normalized_region, [])
    enabled_set = set(enabled_city_keys)
    return [city_key for city_key in requested if city_key in enabled_set]
