import json
import os
from pathlib import Path
from dotenv import load_dotenv

from config_registry import derive_rss_feeds, load_registry

# Load local development secrets from .env.local only.
# Intentionally do not load .env to avoid split-secret files and accidental commits.
env_local_file = Path(__file__).parent / ".env.local"
if env_local_file.exists():
    load_dotenv(env_local_file, override=False)

# API keys and secrets (environment only; no hardcoded fallbacks)
_IN_CI = os.getenv("GITHUB_ACTIONS") == "true" or os.getenv("CI") == "true"


def _load_secret(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if _IN_CI and not value:
        raise RuntimeError(
            f"Missing required secret '{name}' in CI environment. "
            f"Set {name} in repository/workflow secrets."
        )
    return value


OPENAI_API_KEY = _load_secret("OPENAI_API_KEY")

# OpenAI Model Configuration
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5-mini")

# Supabase Configuration (primary storage backend)
# These should be set as environment variables for security
# SUPABASE_URL: Your Supabase project URL (e.g., https://your-project.supabase.co)
# SUPABASE_SERVICE_ROLE_KEY: Service role key for server-side operations
SUPABASE_URL = _load_secret("SUPABASE_URL") or None
SUPABASE_SERVICE_ROLE_KEY = _load_secret("SUPABASE_SERVICE_ROLE_KEY") or None

# RapidAPI Configuration (Google Places API for website lookup)
# Used as fallback for missing facility websites after Nominatim and cross-city lookup
# ⚠️ QUOTA LIMIT: 100 calls per day - use sparingly!
# Get free key at: https://rapidapi.com/google-maps-apis-google-maps-apis-default/api/google-maps-api3
RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST", "google-maps-api3.p.rapidapi.com")
RAPIDAPI_KEY = _load_secret("RAPIDAPI_KEY")
GOOGLE_PLACES_ENABLED = os.getenv("GOOGLE_PLACES_ENABLED", "False").lower() == "true"
GOOGLE_PLACES_MAX_CALLS_PER_DAY = 100  # Hard limit from RapidAPI free tier

# Partner Affiliate URLs (SpotHero parking, hardcoded referrals)
# Parking links are intentionally hardcoded and must NOT be resolved from environment
# or the database to preserve referral parameters exactly.
SPOTHERO_PARKING_SEARCH_URL = "https://spothero.app.link/ts1p2NqSe1?$3p=a_hasoffers&$affiliate_json=http%3A%2F%2Ftracking.spothero.com%2FSH41%26format%3Djson"
SPOTHERO_PARKING_HOME_URL = "https://spothero.app.link/rvcE7nrf19?$3p=a_hasoffers&$affiliate_json=http%3A%2F%2Ftracking.spothero.com%2FSH42%26format%3Djson"
ESIM_CARDS_URL = "https://alosim.com/?_ef_transaction_id=&oid=9&affid=1442"
ESIM_CARDS_TOC_CTA = "Get an eSIM"
ESIM_CARDS_TOC_BLURB = "Activate mobile data in minutes. Avoid roaming fees."

# Stable ordering of SpotHero links used in reports. Keep as literal strings.
SPOTHERO_PARKING_LINKS = [
    {"label": "Find parking", "url": SPOTHERO_PARKING_SEARCH_URL},
]


# Registry-backed city and source configuration.
CITIES, SOURCES = load_registry()
RSS_FEEDS = derive_rss_feeds(SOURCES, CITIES)

# Default city for quick reports
DEFAULT_CITY = "miami"

# Trend analysis configuration
TREND_CONFIG = {
    "lookback_days": 30,  # Number of days to analyze for trends (max: DB retention window)
    "moving_average_window": 7,  # Days for moving average calculation
    "crime_percentile_medium": 60,  # P60 threshold for MEDIUM risk
    "crime_percentile_high": 90,  # P90 threshold for HIGH risk
}

# Ingestion filtering configuration
# Enable keyword-based filtering at ingestion time to avoid storing unrelated items.
# Can be overridden via environment variables.
def _load_bool_from_env(name: str, fallback: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(fallback)
    text = str(raw).strip().lower()
    if text in ("1", "true", "yes", "on"):  # common truthy values
        return True
    if text in ("0", "false", "no", "off"):
        return False
    return bool(fallback)

# When True, only ingest feed items that match travel keywords/thresholds
FILTER_BY_KEYWORDS = _load_bool_from_env("FILTER_BY_KEYWORDS", True)

# When True, fetch advisories specific to a city's country (US State Dept for non-CA,
# Canada advisories for CA). When False, keep current behavior (global advisories).
FILTER_COUNTRY_ADVISORIES = _load_bool_from_env("FILTER_COUNTRY_ADVISORIES", True)

# When True, enable optional website scraping for curated_places backfill.
ENABLE_WEBSITE_SCRAPER = _load_bool_from_env("ENABLE_WEBSITE_SCRAPER", False)

# Ingest-time relevance thresholds (used across sources)
def _load_float_from_env(name: str, fallback: float) -> float:
    raw = os.getenv(name)
    try:
        return float(raw) if raw is not None else float(fallback)
    except Exception:
        return float(fallback)

def _load_int_from_env(name: str, fallback: int) -> int:
    raw = os.getenv(name)
    try:
        return int(raw) if raw is not None else int(fallback)
    except Exception:
        return int(fallback)

INGEST_MIN_SCORE = _load_float_from_env("INGEST_MIN_SCORE", 0.5)
INGEST_MIN_KEYWORDS = _load_int_from_env("INGEST_MIN_KEYWORDS", 2)
INGEST_MIN_STRONG = _load_int_from_env("INGEST_MIN_STRONG", 1)


def _parse_list_value(raw: str) -> list:
    raw = (raw or "").strip()
    if not raw:
        return []
    if raw.startswith("["):
        try:
            values = json.loads(raw)
            if isinstance(values, list):
                return [str(v).strip() for v in values if str(v).strip()]
        except Exception:
            pass
    return [item.strip() for item in raw.split(",") if item.strip()]


def _load_list_from_env(name: str, fallback: list) -> list:
    raw = os.getenv(name)
    if raw is None:
        return list(fallback)
    return _parse_list_value(raw)


EXTRA_TRAVEL_KEYWORDS = _load_list_from_env("EXTRA_TRAVEL_KEYWORDS", [])

EXTRA_STRONG_TRAVEL_KEYWORDS = _load_list_from_env("EXTRA_STRONG_TRAVEL_KEYWORDS", [])

# Locale-specific keyword packs (travel, strong, critical)
# Format: { "locale": { "travel": [...], "strong": [...], "critical": [...] } }
EXTRA_LOCALE_KEYWORDS = {
    "es": {
        "travel": [
            "huelga", "paro", "aeropuerto", "vuelo", "cancelación", "cancelacion",
            "retraso", "cierre", "corte de tráfico", "corte de trafico",
            "metro", "tren", "renfe", "manifestación", "manifestacion",
            "protesta", "disturbios", "incendio", "evacuación", "evacuacion",
        ],
        "strong": [
            "tiroteo", "ataque terrorista", "explosión", "explosión", "bomba",
            "rehenes", "estado de emergencia", "aeropuerto cerrado",
            "metro suspendido", "asalto armado",
        ],
        "critical": [
            "ataque terrorista", "bomba", "explosión", "explosión", "tiroteo",
            "evacuación", "evacuacion", "estado de emergencia",
        ],
    },
    "it": {
        "travel": [
            "aeroporto", "volo", "ritardo", "cancellazione", "chiusura", "sciopero",
            "metro", "treno", "autobus", "stazione", "traffico", "incidente", "strada",
            "manifestazione", "protesta", "disordini", "allerta", "evacuazione",
            "maltempo", "temporale", "alluvione", "ondata di calore", "incendio",
            "polizia", "furto", "rapina", "aggressione",
        ],
        "strong": [
            "attentato", "esplosione", "sparatoria", "allarme bomba", "evacuazione",
            "stato di emergenza", "aeroporto chiuso", "metro sospesa",
        ],
        "critical": [
            "attentato", "bomba", "esplosione", "sparatoria", "stato di emergenza",
            "evacuazione", "allerta rossa",
        ],
    },
    "pt": {
        "travel": [],
        "strong": [],
        "critical": [],
    },
    "fr": {
        "travel": [],
        "strong": [],
        "critical": [],
    },
    "de": {
        "travel": [
            "flughafen", "flug", "verspaetung", "verspätung", "ausfall", "annullierung",
            "streik", "bahn", "zug", "bahnhof", "verkehr", "stau", "unfall", "sperrung",
            "evakuierung", "polizei", "festnahme", "raub", "diebstahl", "taschendiebstahl",
            "angriff", "messerangriff", "anschlag", "terror", "bombe", "explosion",
            "demonstration", "protest", "kundgebung", "ausschreitungen",
            "unwetter", "sturm", "hochwasser", "ueberschwemmung", "überschwemmung",
            "brand", "stromausfall", "warnung", "alarm", "ausbruch", "quarantaene", "quarantäne"
        ],
        "strong": [
            "schiesserei", "schießerei", "anschlag", "bombe", "explosion",
            "evakuierung", "ausnahmezustand", "streik", "flughafen geschlossen"
        ],
        "critical": [
            "anschlag", "bombe", "explosion", "schiesserei", "schießerei",
            "evakuierung", "ausnahmezustand", "flughafen geschlossen"
        ],
    },
    "ja": {
        "travel": [
            "空港", "フライト", "遅延", "欠航", "運休", "電車", "地下鉄", "新幹線", "交通規制", "通行止め",
            "事故", "火災", "停電", "断水", "地震", "余震", "津波", "台風", "洪水", "土砂崩れ",
            "警報", "注意報", "避難", "感染", "食中毒", "デモ", "抗議",
        ],
        "strong": [
            "緊急事態", "避難指示", "津波警報", "大地震", "テロ", "爆発", "発砲", "刃物", "無差別",
        ],
    },
    "ar": {
        "travel": [
            "مطار", "رحلة", "طيران", "تأشيرة", "جواز", "مترو", "طريق", "مرور", "ازدحام", "تحويلة",
            "إغلاق", "تأخير", "إلغاء", "شرطة", "جمارك", "هجرة", "إقامة",
        ],
        "strong": [
            "انفجار", "حريق", "حادث", "إطلاق نار", "هجوم", "احتجاج", "تظاهرة", "شغب", "إضراب",
            "فيضانات", "عاصفة", "زلزال", "انقطاع كهرباء", "إخلاء", "تحذير",
        ],
        "critical": [
            "إرهاب", "تفجير", "قنبلة", "رهائن", "حظر تجول", "حالة طوارئ", "إغلاق المطار", "اشتباكات", "تحذير سفر",
        ],
    },
    "ru": {
        "travel": [
            "аэропорт", "рейс", "вылет", "прилет", "задержка", "отмена", "регистрация", "посадка",
            "метро", "мцк", "мцд", "поезд", "вокзал", "электричка", "автобус",
            "перекрытие", "пробка", "дтп", "авария",
            "пожар", "эвакуация", "взрыв", "стрельба",
            "митинг", "протест", "демонстрация", "забастовка",
            "шторм", "снегопад", "гололед", "наводнение",
            "карантин", "вспышка", "эпидемия",
        ],
        "strong": [
            "теракт", "взрыв", "стрельба", "эвакуация", "массовые беспорядки", "чрезвычайное положение",
        ],
        "critical": [
            "теракт", "взрыв", "массовая стрельба", "чрезвычайное положение", "эвакуация",
        ],
    },
}

CURATED_PLACE_NAME_DENYLIST = {
    "pharmacy": {"create"},
}

# Travel relevance scoring mode
# "keywords" (default): Traditional keyword-based filtering (supports English, Portuguese, French, Spanish)
# "semantic_multilingual": Language-agnostic semantic similarity using multilingual embeddings (50+ languages)
TRAVEL_RELEVANCE_MODE = os.getenv("TRAVEL_RELEVANCE_MODE", "keywords")

# Semantic relevance model and threshold (only used when TRAVEL_RELEVANCE_MODE is "semantic_multilingual")
SEMANTIC_MODEL_NAME = os.getenv(
    "SEMANTIC_MODEL_NAME",
    "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
)
SEMANTIC_RELEVANCE_THRESHOLD = _load_float_from_env("SEMANTIC_RELEVANCE_THRESHOLD", 0.33)

# Weather forecast caching
# Only re-fetch if latest forecast is older than this many hours
WEATHER_FORECAST_TTL_HOURS = _load_float_from_env("WEATHER_FORECAST_TTL_HOURS", 6)
# Retention period for old weather forecasts (auto-cleanup)
WEATHER_FORECAST_RETENTION_DAYS = _load_float_from_env("WEATHER_FORECAST_RETENTION_DAYS", 7)


