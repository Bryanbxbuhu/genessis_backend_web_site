from __future__ import annotations

from typing import Any, Dict, Optional


def build_city_label(meta: Optional[Dict[str, Any]] = None, city_config: Optional[Dict[str, Any]] = None, report: Optional[Dict[str, Any]] = None) -> str:
    meta = meta or {}
    city_config = city_config or {}

    def pick(*vals: Any) -> str:
        for v in vals:
            if v is None:
                continue
            s = str(v).strip()
            if s:
                return s
        return ""

    city = pick(
        meta.get("city_name"),
        meta.get("city"),
        meta.get("display_name"),
        city_config.get("display_name"),
        city_config.get("city"),
        city_config.get("name"),
    )

    admin1 = pick(
        meta.get("admin1"),
        meta.get("region"),
        meta.get("state"),
        meta.get("province"),
        meta.get("administrative_area_level_1"),
        city_config.get("admin1"),
        city_config.get("region"),
        city_config.get("state"),
        city_config.get("province"),
    )

    country = pick(
        meta.get("country_name"),
        meta.get("country"),
        city_config.get("country_name"),
        city_config.get("country"),
        city_config.get("country_display"),
    )

    label = ", ".join([p for p in [city, admin1, country] if p])

    if label:
        return label

    if isinstance(report, dict):
        dest = str(report.get("destination") or "").strip()
        if dest:
            return dest

    return ""
