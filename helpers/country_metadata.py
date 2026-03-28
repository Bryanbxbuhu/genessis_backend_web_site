"""
Country metadata utilities.
"""

from typing import Dict


COUNTRY_NAME_EN: Dict[str, str] = {
    "US": "United States",
    "MX": "Mexico",
    "HR": "Croatia",
    "FR": "France",
    "GB": "United Kingdom",
    "PT": "Portugal",
    "ES": "Spain",
    "CZ": "Czech Republic",
    "DE": "Germany",
    "JP": "Japan",
    "RU": "Russia",
    "AE": "United Arab Emirates",
}


def get_country_name_en(country_code: str) -> str:
    """Return the English country name for an ISO2 country code."""
    code = (country_code or "").strip().upper()
    return COUNTRY_NAME_EN.get(code, "")
