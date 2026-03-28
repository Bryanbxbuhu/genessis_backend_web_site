"""Report enrichment for Google Maps search links.

Adds google_maps_url to all place objects in a report before saving to database.
This ensures the stored report JSON includes Map links for every place.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

from helpers.location_label import build_city_label
from helpers.google_maps import build_google_maps_search_url


def _place_name(p: Dict[str, Any]) -> str:
    """Extract place name from a place dict."""
    return str(p.get("name") or p.get("title") or p.get("brand") or "").strip()


def _enrich_list(items: Any, city_label: str) -> Any:
    """Add google_maps_url to each place in a list.
    
    Args:
        items: List of place dicts
        city_label: City label for Google Maps query scoping
        
    Returns:
        List with google_maps_url added to each place dict
    """
    if not isinstance(items, list):
        return items
    out = []
    for p in items:
        if not isinstance(p, dict):
            out.append(p)
            continue
        if str(p.get("google_maps_url") or "").strip():
            out.append(p)
            continue
        name = _place_name(p)
        url = build_google_maps_search_url(name, city_label) if name else None
        if url:
            pp = dict(p)
            pp["google_maps_url"] = url
            out.append(pp)
        else:
            out.append(p)
    return out


def enrich_report_places_with_maps(
    report: Dict[str, Any],
    city_config: Optional[Dict[str, Any]] = None,
    resolved_location: Optional[Dict[str, Any]] = None,
    city_row: Optional[Dict[str, Any]] = None,
    place_keys: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """Enrich report with Google Maps search URLs for all places.
    
    This function modifies the report dict in place and adds google_maps_url
    to each place object in the specified place lists.
    
    Args:
        report: Report dict with meta and place lists
        city_config: City configuration from config.CITIES
        resolved_location: Nominatim geocoding result
        city_row: Database city record
        place_keys: List of keys to enrich (defaults to common place types)
        
    Returns:
        The enriched report dict (same object, modified in place)
        
    Example:
        >>> report = {"meta": {"city": "Barcelona", "country": "Spain"}, 
        ...           "hospitals": [{"name": "Hospital Clinic"}]}
        >>> enrich_report_places_with_maps(report)
        >>> report["hospitals"][0]["google_maps_url"]
        'https://www.google.com/maps/search/?api=1&query=Hospital%20Clinic%20in%20Barcelona%2C%20Spain'
    """
    if not isinstance(report, dict):
        return report

    meta = report.get("meta") if isinstance(report.get("meta"), dict) else {}
    city_label = build_city_label(meta=meta, city_config=city_config, report=report)

    # Default place keys to enrich
    keys = list(place_keys) if place_keys else [
        "hospitals",
        "pharmacies",
        "supermarkets",
        "convenience_stores",
        "airports",
        "medical_facilities",
        "clinics",
        "urgent_care",
        "police",
        "fire",
        "embassies",
        "consulates",
        "shelters",
    ]

    for k in keys:
        if k in report:
            report[k] = _enrich_list(report.get(k), city_label)

    return report
