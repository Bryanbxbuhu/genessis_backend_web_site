"""
Emergency contact mappings by country code.
"""

from copy import deepcopy
from typing import Dict, Optional

from helpers.emergency_numbers import COUNTRY_DEFAULTS


def get_emergency_contacts(country_code: Optional[str]) -> Dict[str, object]:
    """
    Get emergency contact numbers for a country.

    Returns a dict with general/police/ambulance/fire plus optional "other" list.
    """
    code = (country_code or "").strip().upper()
    defaults = deepcopy(COUNTRY_DEFAULTS.get(code, {}))
    if isinstance(defaults, dict):
        defaults.setdefault("other", [])
    return normalize_emergency_contacts(defaults or {})


def apply_emergency_overrides(
    base_contacts: Dict[str, object],
    overrides: Optional[Dict[str, str]],
) -> Dict[str, object]:
    """Apply city-specific overrides on top of country defaults."""
    if not overrides:
        return deepcopy(base_contacts)

    merged = deepcopy(base_contacts)
    for key in ["general", "police", "ambulance", "fire"]:
        value = overrides.get(key)
        if isinstance(value, str) and value.strip():
            merged[key] = value.strip()
    return merged


def normalize_emergency_contacts(contacts: Optional[Dict[str, object]]) -> Dict[str, object]:
    """Normalize emergency contacts to the report schema."""
    if not isinstance(contacts, dict):
        return {}

    normalized = deepcopy(contacts)
    other = normalized.get("other")
    if isinstance(other, list):
        normalized_other = []
        for entry in other:
            if not isinstance(entry, dict):
                continue
            entry_copy = dict(entry)
            if "label" not in entry_copy and "name" in entry_copy:
                label = entry_copy.pop("name")
                if isinstance(label, str) and label.strip():
                    entry_copy["label"] = label
            normalized_other.append(entry_copy)
        normalized["other"] = normalized_other
    return normalized
