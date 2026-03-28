from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from pydantic import BaseModel, StrictBool, StrictStr, ValidationError, validator


CONFIG_DIR_ENV = "OSINT_CONFIG_DIR"
ALLOWED_SOURCE_TYPES = {"rss", "json", "api"}


class CityModel(BaseModel):
    name: StrictStr
    country_code: Optional[StrictStr] = None
    timezone: Optional[StrictStr] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    population: Optional[int] = None
    aliases: List[StrictStr] = []
    transit_systems: List[Dict[str, Any]] = []
    emergency_numbers: Optional[Dict[str, Any]] = None
    language: Optional[StrictStr] = None
    enabled: Optional[StrictBool] = True

    class Config:
        extra = "allow"


class SourceModel(BaseModel):
    source_key: StrictStr
    name: StrictStr
    type: StrictStr
    url: StrictStr
    enabled: Optional[StrictBool] = True
    city_key: Optional[StrictStr] = None
    fallback_url: Optional[StrictStr] = None
    tags: Optional[List[StrictStr]] = None

    @validator("type")
    def _type_allowed(cls, value: str) -> str:
        if value not in ALLOWED_SOURCE_TYPES:
            raise ValueError(f"type must be one of {sorted(ALLOWED_SOURCE_TYPES)}")
        return value

    @validator("url")
    def _url_non_empty(cls, value: str, values: Dict[str, Any]) -> str:
        if not value or not value.strip():
            raise ValueError("url must be non-empty")
        src_type = values.get("type")
        if src_type == "rss":
            parsed = urlparse(value)
            if parsed.scheme not in {"http", "https"}:
                raise ValueError("rss url must be http or https")
        return value

    @validator("fallback_url")
    def _fallback_url_scheme(cls, value: Optional[str]) -> Optional[str]:
        if not value:
            return value
        parsed = urlparse(value)
        if parsed.scheme not in {"http", "https"}:
            raise ValueError("fallback_url must be http or https")
        return value


def _model_to_dict(model: BaseModel) -> Dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


def _resolve_config_dir() -> Path:
    override = os.getenv(CONFIG_DIR_ENV)
    if override:
        return Path(override).expanduser().resolve()
    return Path(__file__).resolve().parent / "config_data"


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {path}: {exc}") from exc


def _load_cities(config_dir: Path) -> Dict[str, Dict[str, Any]]:
    cities_dir = config_dir / "cities"
    if not cities_dir.exists():
        raise FileNotFoundError(f"Missing config directory: {cities_dir}")

    cities: Dict[str, Dict[str, Any]] = {}
    for path in sorted(cities_dir.glob("*.json")):
        city_key = path.stem
        if city_key in cities:
            raise ValueError(f"Duplicate city_key detected: {city_key}")
        payload = _load_json(path)
        if not isinstance(payload, dict):
            raise ValueError(f"City config must be an object in {path}")
        try:
            model = CityModel(**payload)
        except ValidationError as exc:
            raise ValueError(f"Invalid city config in {path}:\n{exc}") from exc
        cities[city_key] = _model_to_dict(model)
    return cities


def _resolve_city_key(file_key: str, entry: Dict[str, Any]) -> Optional[str]:
    if file_key == "global":
        return entry.get("city_key")
    explicit = entry.get("city_key")
    if explicit in (None, ""):
        return file_key
    if explicit != file_key:
        raise ValueError(f"Source city_key '{explicit}' does not match file '{file_key}.json'")
    return explicit


def _load_sources(config_dir: Path, cities: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    sources_dir = config_dir / "sources"
    if not sources_dir.exists():
        raise FileNotFoundError(f"Missing config directory: {sources_dir}")

    sources: List[Dict[str, Any]] = []
    seen_keys: set[str] = set()
    for path in sorted(sources_dir.glob("*.json")):
        file_key = path.stem
        payload = _load_json(path)
        if not isinstance(payload, list):
            raise ValueError(f"Sources config must be a list in {path}")
        for entry in payload:
            if not isinstance(entry, dict):
                raise ValueError(f"Source entry must be an object in {path}")
            entry = dict(entry)
            entry["city_key"] = _resolve_city_key(file_key, entry)
            try:
                model = SourceModel(**entry)
            except ValidationError as exc:
                raise ValueError(f"Invalid source config in {path}:\n{exc}") from exc
            data = _model_to_dict(model)
            source_key = data.get("source_key")
            if source_key in seen_keys:
                raise ValueError(f"Duplicate source_key detected: {source_key}")
            seen_keys.add(source_key)
            city_key = data.get("city_key")
            if city_key and city_key not in cities:
                raise ValueError(f"Unknown city_key '{city_key}' for source '{source_key}'")
            sources.append(data)
    return sources


def load_registry(config_dir: Optional[Path] = None) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    """Load CITIES and SOURCES from config_data with validation."""
    config_dir = config_dir or _resolve_config_dir()
    cities = _load_cities(config_dir)
    sources = _load_sources(config_dir, cities)
    return cities, sources


def derive_rss_feeds(
    sources: List[Dict[str, Any]],
    cities: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """Derive RSS_FEEDS from SOURCES for rss and json feed types."""
    feeds: Dict[str, Dict[str, Any]] = {}
    for source in sources:
        source_type = source.get("type")
        if source_type not in {"rss", "json"}:
            continue
        city_key = source.get("city_key")
        location = "Global"
        if city_key:
            location = (cities.get(city_key, {}) or {}).get("name", city_key)
        feeds[source["source_key"]] = {
            "name": source.get("name", ""),
            "url": source.get("url", ""),
            "enabled": bool(source.get("enabled", True)),
            "location": location,
        }
        if source_type != "rss":
            feeds[source["source_key"]]["type"] = source_type
    return feeds
