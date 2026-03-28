"""
Signal source fetchers for OSINT Project.

This package contains modules for fetching data from various external sources:
- GDELT Project (global news events)
- NWS (National Weather Service alerts)
- Open-Meteo (weather forecasts)
"""

from .gdelt_geo import fetch_gdelt_geo
from .nws_alerts import fetch_nws_alerts
from .open_meteo import fetch_open_meteo_forecast

__all__ = [
    "fetch_gdelt_geo",
    "fetch_nws_alerts",
    "fetch_open_meteo_forecast",
]
