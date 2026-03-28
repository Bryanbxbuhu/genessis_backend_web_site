from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from copy import deepcopy

from helpers.location_label import build_city_label
from helpers.google_maps import build_google_maps_search_url

NAME_KEYS: Tuple[str, ...] = ("name", "title", "brand", "system", "label")
GEO_KEYS: Tuple[str, ...] = ("website", "address", "iata", "lat", "lon", "longitude", "latitude")


def _place_name(item: Dict[str, Any]) -> str:
    for k in NAME_KEYS:
        v = item.get(k)
        if v is not None:
            s = str(v).strip()
            if s:
                return s

    iata = str(item.get("iata") or "").strip()
    if iata:
        return f"{iata} airport"

    return ""


def _looks_like_place_list(key: str, items: List[Any]) -> bool:
    if not items:
        return False
    if not all(isinstance(x, dict) for x in items):
        return False

    key_l = (key or "").lower()

    strong_keys = {
        "airports",
        "hospitals",
        "pharmacies",
        "supermarkets",
        "grocery_stores",
        "convenience_stores",
        "transit",
        "public_transport",
        "transportation",
        "rental_cars",
        "car_rentals",
        "car_rental",
        "rideshare",
        "taxis",
        "rail_stations",
        "bus_stations",
        "ports",
        "embassies",
        "consulates",
        "police_stations",
        "clinics",
        "urgent_care",
        "medical_facilities",
    }
    if key_l in strong_keys:
        return True

    if "store" in key_l or "airport" in key_l or "hospital" in key_l or "pharmacy" in key_l:
        return True
    if "transit" in key_l or "transport" in key_l or "rental" in key_l:
        return True

    for it in items:
        has_name = any(str(it.get(k) or "").strip() for k in NAME_KEYS) or str(it.get("iata") or "").strip()
        has_geo = any(str(it.get(k) or "").strip() for k in GEO_KEYS)
        if has_name and has_geo:
            return True

    return False


def _enrich_list(items: List[Any], city_label: str) -> List[Any]:
    out: List[Any] = []
    for it in items:
        if not isinstance(it, dict):
            out.append(it)
            continue

        if str(it.get("google_maps_url") or "").strip():
            out.append(it)
            continue

        name = _place_name(it)
        url = build_google_maps_search_url(name, city_label) if name else None
        if url:
            it = dict(it)
            it["google_maps_url"] = url
        out.append(it)
    return out


def enrich_report_with_google_maps_urls(report: Dict[str, Any], meta: Optional[Dict[str, Any]] = None, city_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not isinstance(report, dict):
        return report

    r = deepcopy(report)
    city_label = build_city_label(meta=meta or r.get("meta") or {}, city_config=city_config or {}, report=r)

    def walk(obj: Any) -> Any:
        if isinstance(obj, dict):
            new_obj: Dict[str, Any] = {}
            for k, v in obj.items():
                if isinstance(v, list) and _looks_like_place_list(str(k), v):
                    new_obj[k] = _enrich_list(v, city_label)
                else:
                    new_obj[k] = walk(v)
            return new_obj

        if isinstance(obj, list):
            return [walk(x) for x in obj]

        return obj

    return walk(r)
