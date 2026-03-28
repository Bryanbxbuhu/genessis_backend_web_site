"""
Emergency contact defaults and resolution rules.
"""

from copy import deepcopy
from typing import Dict


COUNTRY_DEFAULTS = {
    "US": {"general": "911", "police": "911", "ambulance": "911", "fire": "911"},
    "JP": {"police": "110", "ambulance": "119", "fire": "119"},
    "GB": {"general": "999", "police": "999", "ambulance": "999", "fire": "999"},
    "IN": {
        "general": "112",
        "police": "112",
        "ambulance": "112",
        "fire": "112",
        "other": [
            {"label": "Police (legacy)", "number": "100"},
            {"label": "Fire (legacy)", "number": "101"},
            {"label": "Ambulance (legacy)", "number": "102"},
            {"label": "Tourist helpline", "number": "1363"},
            {"label": "Women in distress", "number": "1091"},
        ],
    },
    "FR": {"general": "112", "police": "17", "ambulance": "15", "fire": "18"},
    "ES": {"general": "112", "police": "091", "ambulance": "061", "fire": "080"},
    "PT": {"general": "112"},
    "CZ": {"general": "112", "police": "158", "ambulance": "155", "fire": "150"},
    "RU": {
        "general": "112",
        "police": "102",
        "ambulance": "103",
        "fire": "101",
        "other": [{"label": "Gas emergency", "number": "104"}],
    },
}


def resolve_emergency_numbers(city_cfg: dict, snapshot: dict, country_code: str) -> dict:
    cfg_nums = (city_cfg or {}).get("emergency_numbers") or {}
    snap_nums = (snapshot or {}).get("emergency_numbers") or {}
    defaults = COUNTRY_DEFAULTS.get((country_code or "").upper(), {})

    merged: Dict[str, str] = {}
    for key in ["general", "police", "ambulance", "fire"]:
        value = cfg_nums.get(key) or snap_nums.get(key) or defaults.get(key)
        if value:
            merged[key] = str(value).strip()

    if "general" not in merged:
        if merged.get("police") and merged.get("ambulance") and merged.get("fire"):
            if merged["police"] == merged["ambulance"] == merged["fire"]:
                merged["general"] = merged["police"]

    other = None
    if isinstance(cfg_nums, dict) and isinstance(cfg_nums.get("other"), list):
        other = cfg_nums.get("other")
    elif isinstance(snap_nums, dict) and isinstance(snap_nums.get("other"), list):
        other = snap_nums.get("other")
    elif isinstance(defaults, dict) and isinstance(defaults.get("other"), list):
        other = defaults.get("other")

    if other is not None:
        merged["other"] = deepcopy(other)

    return merged
