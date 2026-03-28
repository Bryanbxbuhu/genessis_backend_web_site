from __future__ import annotations

from typing import Optional
from urllib.parse import quote


def build_google_maps_search_url(place_name: Optional[str], city_label: Optional[str]) -> Optional[str]:
    name = (place_name or "").strip()
    if not name:
        return None

    city = (city_label or "").strip()
    query = f"{name} in {city}" if city else name
    return f"https://www.google.com/maps/search/?api=1&query={quote(query, safe='')}"
