import calendar
import re
import unicodedata
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import config

try:
    from unidecode import unidecode
    HAS_UNIDECODE = True
except ImportError:
    HAS_UNIDECODE = False
    def unidecode(text):
        """Fallback if unidecode not installed"""
        return text

# -----------------------------
# Keyword sets
# -----------------------------

BASE_TRAVEL_KEYWORDS = [
    # Transportation disruptions
    "airport",
    "flight",
    "airline",
    "cancel",
    "cancellation",
    "delay",
    "delayed",
    "grounded",
    "metro",
    "subway",
    "train",
    "rail",
    "station",
    "transit",
    "transport",
    "traffic",
    "road closure",
    "road closed",
    "highway",
    "bridge closed",
    # Safety & Security
    "robbery",
    "theft",
    "pickpocket",
    "assault",
    "mugging",
    "carjacking",
    "shooting",
    "stabbing",
    "attack",
    "terror",
    "bomb",
    "explosion",
    "kidnap",
    "violence",
    "gang",
    "crime wave",
    "murder",
    "arrest",
    "police",
    # Civil unrest
    "protest",
    "riot",
    "demonstration",
    "curfew",
    "strike",
    "evacuation",
    "civil unrest",
    "looting",
    "clash",
    # Weather & Natural disasters
    "hurricane",
    "storm",
    "tornado",
    "flood",
    "flooding",
    "wildfire",
    "earthquake",
    "tsunami",
    "heat wave",
    "extreme heat",
    "blizzard",
    "landslide",
    "tropical storm",
    # Health emergencies
    "outbreak",
    "epidemic",
    "disease",
    "virus",
    "infection",
    "contamination",
    "food poisoning",
    "quarantine",
    "health alert",
    # Border & Entry
    "border",
    "border closure",
    "passport",
    "visa",
    "entry ban",
    "travel ban",
    "immigration",
    "customs",
    # Infrastructure failures
    "power outage",
    "blackout",
    "water shortage",
    "gas leak",
    "building collapse",
    "fire",
]

BASE_STRONG_TRAVEL_KEYWORDS = [
    # Critical transportation
    "airport closed",
    "airport shutdown",
    "flight cancel",
    "flight canceled",
    "metro shutdown",
    "transit strike",
    "rail closure",
    "station evacuated",
    # Critical safety
    "shooting",
    "stabbing",
    "terror",
    "attack",
    "bomb",
    "explosion",
    "armed",
    "robbery",
    "carjacking",
    "assault",
    "murder",
    "hostage",
    # Critical weather / emergency
    "hurricane",
    "tornado",
    "earthquake",
    "tsunami",
    "flood",
    "evacuation",
    "emergency",
    # Critical unrest
    "riot",
    "curfew",
    "martial law",
    "state of emergency",
    # Critical health
    "outbreak",
    "epidemic",
    "quarantine",
    "health emergency",
    # Critical closures
    "border closed",
    "travel ban",
    "entry ban",
    "lockdown",
]

BASE_CRITICAL_KEYWORDS = [
    "terror",
    "attack",
    "bomb",
    "explosion",
    "shooting",
    "mass shooting",
    "earthquake",
    "tsunami",
    "hurricane",
    "evacuation",
    "state of emergency",
    "martial law",
    "lockdown",
    "airport closed",
    "metro shutdown",
    "rail closure",
]

BASE_CATEGORY_SIGNAL_KEYWORDS = [
    "airport",
    "flight",
    "airline",
    "metro",
    "subway",
    "rail",
    "train",
    "station",
    "transit",
    "bus",
    "road",
    "highway",
    "bridge",
    "strike",
    "protest",
    "riot",
    "curfew",
    "evacuation",
    "emergency",
    "hurricane",
    "storm",
    "tornado",
    "flood",
    "wildfire",
    "earthquake",
    "tsunami",
    "outbreak",
    "epidemic",
    "health",
    "alert",
    "advisory",
    "border",
    "visa",
    "passport",
    "entry",
]

# Optional locale packs (kept small + high-signal).
# These are used only if enabled by config or via `compute_travel_relevance(..., locale="fr")`.
FR_TRAVEL_KEYWORDS = [
    # Transport
    "aeroport",
    "vol",
    "annulation",
    "retard",
    "greve",
    "sncf",
    "ratp",
    "rer",
    "metro",
    "gare",
    "circulation",
    "embouteillage",
    "route fermee",
    # Safety / crime
    "vol",
    "agression",
    "attaque",
    "arme",
    "couteau",
    "fusillade",
    "braquage",
    "cambriolage",
    "escroquerie",
    "arnaque",
    "pickpocket",
    "police",
    # Unrest
    "manifestation",
    "emeute",
    "affrontement",
]

FR_STRONG_TRAVEL_KEYWORDS = [
    "fusillade",
    "attaque",
    "explosion",
    "bombe",
    "otage",
    "evacuation",
    "etat d urgence",
    "greve",
    "aeroport ferme",
    "metro interrompu",
    "rer interrompu",
]

FR_CRITICAL_KEYWORDS = [
    "attaque",
    "bombe",
    "explosion",
    "fusillade",
    "evacuation",
    "etat d urgence",
    "aeroport ferme",
    "metro interrompu",
]

# Portuguese locale pack
PT_TRAVEL_KEYWORDS = [
    # Transport
    "aeroporto",
    "voo",
    "cancelamento",
    "atraso",
    "greve",
    "metro",
    "comboio",
    "autocarro",
    "estacao",
    "transito",
    "transporte",
    "engarrafamento",
    "estrada fechada",
    # Safety / crime
    "roubo",
    "assalto",
    "ataque",
    "arma",
    "faca",
    "tiroteio",
    "crime",
    "policia",
    "violencia",
    # Unrest
    "manifestacao",
    "protesto",
    "tumulto",
    "confronto",
    # Weather
    "tempestade",
    "inundacao",
    "alerta",
    "emergencia",
    # Health
    "surto",
    "epidemia",
    "doenca",
    "quarentena",
]

PT_STRONG_TRAVEL_KEYWORDS = [
    "tiroteio",
    "ataque terrorista",
    "explosao",
    "bomba",
    "refem",
    "evacuacao",
    "estado de emergencia",
    "greve",
    "aeroporto encerrado",
    "aeroporto fechado",
    "metro suspenso",
    "assalto armado",
]

PT_CRITICAL_KEYWORDS = [
    "ataque terrorista",
    "bomba",
    "explosao",
    "tiroteio",
    "evacuacao",
    "estado de emergencia",
    "aeroporto encerrado",
    "metro suspenso",
]

# Spanish keyword packs
ES_TRAVEL_KEYWORDS = [
    # Transport
    "aeropuerto",
    "vuelo",
    "cancelacion",
    "cancelación",
    "retraso",
    "huelga",
    "paro",
    "metro",
    "tren",
    "renfe",
    "estacion",
    "transito",
    "transporte",
    "trafico",
    "cierre",
    "corte de trafico",
    # Safety / crime
    "robo",
    "asalto",
    "ataque",
    "arma",
    "cuchillo",
    "tiroteo",
    "crimen",
    "policia",
    "violencia",
    # Unrest
    "manifestacion",
    "protesta",
    "disturbios",
    "enfrentamiento",
    # Weather
    "tormenta",
    "inundacion",
    "alerta",
    "emergencia",
    # Health
    "brote",
    "epidemia",
    "enfermedad",
    "cuarentena",
]

ES_STRONG_TRAVEL_KEYWORDS = [
    "tiroteo",
    "ataque terrorista",
    "explosion",
    "explosión",
    "bomba",
    "rehenes",
    "evacuacion",
    "evacuación",
    "estado de emergencia",
    "huelga",
    "aeropuerto cerrado",
    "aeropuerto encerrado",
    "metro suspendido",
    "asalto armado",
    "incendio",
    "alarma",
]

ES_CRITICAL_KEYWORDS = [
    "ataque terrorista",
    "bomba",
    "explosion",
    "explosión",
    "tiroteo",
    "evacuacion",
    "evacuación",
    "estado de emergencia",
    "aeropuerto cerrado",
    "metro suspendido",
]

# German keyword packs
DE_TRAVEL_KEYWORDS = [
    "flughafen",
    "flug",
    "verspaetung",
    "verspätung",
    "ausfall",
    "annullierung",
    "streik",
    "bahn",
    "zug",
    "s bahn",
    "u bahn",
    "bus",
    "bahnhof",
    "station",
    "verkehr",
    "stau",
    "unfall",
    "sperrung",
    "strassensperrung",
    "straßensperrung",
    "evakuierung",
    "polizei",
    "festnahme",
    "raub",
    "diebstahl",
    "taschendiebstahl",
    "ueberfall",
    "überfall",
    "angriff",
    "messerangriff",
    "schuesse",
    "schüsse",
    "schiesserei",
    "schießerei",
    "anschlag",
    "terror",
    "bombe",
    "explosion",
    "demonstration",
    "protest",
    "kundgebung",
    "ausschreitungen",
    "randale",
    "unwetter",
    "sturm",
    "hochwasser",
    "ueberschwemmung",
    "überschwemmung",
    "waldbrand",
    "brand",
    "stromausfall",
    "ausnahmezustand",
    "warnung",
    "alarm",
    "gesundheitsamt",
    "ausbruch",
    "quarantaene",
    "quarantäne",
]

DE_STRONG_TRAVEL_KEYWORDS = [
    "schiesserei",
    "schießerei",
    "anschlag",
    "bombe",
    "explosion",
    "evakuierung",
    "ausnahmezustand",
    "streik",
    "flughafen geschlossen",
    "u bahn eingestellt",
    "s bahn eingestellt",
    "bahnhof evakuiert",
]

DE_CRITICAL_KEYWORDS = [
    "anschlag",
    "bombe",
    "explosion",
    "schiesserei",
    "schießerei",
    "evakuierung",
    "ausnahmezustand",
    "flughafen geschlossen",
    "u bahn eingestellt",
    "s bahn eingestellt",
]


# -----------------------------
# Normalization helpers
# -----------------------------

_WORD_CHARS_RE = re.compile(r"[^\w]+", re.UNICODE)


def strip_html(text: str) -> str:
    """Remove HTML tags from text."""
    return re.sub(r'<[^>]+>', '', text or '')


def normalize_text(value: str) -> str:
    """
    Lowercase, remove accents, keep [a-z0-9] + spaces, collapse whitespace.
    """
    text = value or ""
    text = strip_html(text)
    text = unicodedata.normalize("NFKC", text)
    # Remove combining marks (fold accents) for matching while preserving Unicode letters
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    # Convert underscores to spaces then remove non-word characters (keeps Unicode letters/digits)
    text = text.replace("_", " ")
    text = _WORD_CHARS_RE.sub(" ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_text_keep_accents(value: str) -> str:
    """
    Lowercase and normalize, but keep accents (for Portuguese text).
    Applies NFKC normalization and strips HTML.
    """
    text = value or ""
    text = strip_html(text)
    text = unicodedata.normalize("NFKC", text)
    # Keep accents (do not strip combining marks), but normalize spacing and remove non-word punctuation
    text = text.lower()
    text = text.replace("_", " ")
    text = _WORD_CHARS_RE.sub(" ", text)
    return re.sub(r"\s+", " ", text).strip()


def fold_accents(text: str) -> str:
    """
    Remove accents for fuzzy matching (São -> Sao, Lisboa stays lisboa).
    Uses unidecode if available, otherwise falls back to basic normalization.
    """
    if HAS_UNIDECODE:
        return unidecode(text).lower()
    else:
        # Fallback: basic accent removal
        text = unicodedata.normalize("NFKD", text)
        return "".join(ch for ch in text if not unicodedata.combining(ch)).lower()


def _normalize_keyword_list(values: Iterable[str]) -> List[str]:
    normalized: List[str] = []
    for value in values or []:
        if value is None:
            continue
        keyword = normalize_text(str(value))
        if keyword and keyword not in normalized:
            normalized.append(keyword)
    return normalized


def _extend_keywords(base: List[str], extra: Iterable[str]) -> List[str]:
    if not extra:
        return list(base)
    combined = list(base)
    for keyword in _normalize_keyword_list(extra):
        if keyword not in combined:
            combined.append(keyword)
    return combined


# -----------------------------
# Build keyword lists with config overrides
# -----------------------------

_TRAVEL_BASE = _normalize_keyword_list(BASE_TRAVEL_KEYWORDS)
_STRONG_BASE = _normalize_keyword_list(BASE_STRONG_TRAVEL_KEYWORDS)
_CRITICAL_BASE = _normalize_keyword_list(BASE_CRITICAL_KEYWORDS)
_CATEGORY_SIGNALS = _normalize_keyword_list(BASE_CATEGORY_SIGNAL_KEYWORDS)

TRAVEL_KEYWORDS = _extend_keywords(_TRAVEL_BASE, getattr(config, "EXTRA_TRAVEL_KEYWORDS", []))
STRONG_TRAVEL_KEYWORDS = _extend_keywords(_STRONG_BASE, getattr(config, "EXTRA_STRONG_TRAVEL_KEYWORDS", []))

# Fix: critical should extend from EXTRA_CRITICAL (not EXTRA_STRONG)
CRITICAL_TRAVEL_KEYWORDS = _extend_keywords(_CRITICAL_BASE, getattr(config, "EXTRA_CRITICAL_TRAVEL_KEYWORDS", []))

CATEGORY_SIGNAL_KEYWORDS = _CATEGORY_SIGNALS

# Optional locale extras from config (kept backwards compatible)
EXTRA_LOCALE_KEYWORDS: Dict[str, Dict[str, Sequence[str]]] = getattr(config, "EXTRA_LOCALE_KEYWORDS", {})


def _build_locale_keywords(locale: Optional[str]) -> Tuple[List[str], List[str], List[str]]:
    """
    Returns (regular, strong, critical) keyword lists for the requested locale.
    Defaults to English base lists if no locale is provided.
    """
    if not locale:
        return TRAVEL_KEYWORDS, STRONG_TRAVEL_KEYWORDS, CRITICAL_TRAVEL_KEYWORDS

    loc = normalize_text(locale).replace(" ", "")
    if loc.startswith("fr"):
        reg = _extend_keywords(TRAVEL_KEYWORDS, FR_TRAVEL_KEYWORDS)
        strong = _extend_keywords(STRONG_TRAVEL_KEYWORDS, FR_STRONG_TRAVEL_KEYWORDS)
        critical = _extend_keywords(CRITICAL_TRAVEL_KEYWORDS, FR_CRITICAL_KEYWORDS)
    elif loc.startswith("pt"):
        reg = _extend_keywords(TRAVEL_KEYWORDS, PT_TRAVEL_KEYWORDS)
        strong = _extend_keywords(STRONG_TRAVEL_KEYWORDS, PT_STRONG_TRAVEL_KEYWORDS)
        critical = _extend_keywords(CRITICAL_TRAVEL_KEYWORDS, PT_CRITICAL_KEYWORDS)
    elif loc.startswith("es"):
        reg = _extend_keywords(TRAVEL_KEYWORDS, ES_TRAVEL_KEYWORDS)
        strong = _extend_keywords(STRONG_TRAVEL_KEYWORDS, ES_STRONG_TRAVEL_KEYWORDS)
        critical = _extend_keywords(CRITICAL_TRAVEL_KEYWORDS, ES_CRITICAL_KEYWORDS)
    elif loc.startswith("de"):
        reg = _extend_keywords(TRAVEL_KEYWORDS, DE_TRAVEL_KEYWORDS)
        strong = _extend_keywords(STRONG_TRAVEL_KEYWORDS, DE_STRONG_TRAVEL_KEYWORDS)
        critical = _extend_keywords(CRITICAL_TRAVEL_KEYWORDS, DE_CRITICAL_KEYWORDS)
    else:
        reg, strong, critical = TRAVEL_KEYWORDS, STRONG_TRAVEL_KEYWORDS, CRITICAL_TRAVEL_KEYWORDS

    # Allow config to inject locale packs without editing this module
    pack = EXTRA_LOCALE_KEYWORDS.get(locale) or EXTRA_LOCALE_KEYWORDS.get(loc) or {}
    reg = _extend_keywords(reg, pack.get("travel", []))
    strong = _extend_keywords(strong, pack.get("strong", []))
    critical = _extend_keywords(critical, pack.get("critical", []))
    return reg, strong, critical


# -----------------------------
# Matching helpers (avoid substring false positives)
# -----------------------------

def _compile_word_regex(keywords: Sequence[str]) -> Dict[str, re.Pattern]:
    """
    For single-token keywords, compile a word-boundary regex.
    Phrases (contain space) are not compiled and should be checked via substring.
    """
    compiled: Dict[str, re.Pattern] = {}
    for kw in keywords:
        if " " in kw:
            continue
        compiled[kw] = re.compile(rf"\b{re.escape(kw)}\b")
    return compiled


_WORD_RE_TRAVEL = _compile_word_regex(TRAVEL_KEYWORDS)
_WORD_RE_STRONG = _compile_word_regex(STRONG_TRAVEL_KEYWORDS)
_WORD_RE_CRITICAL = _compile_word_regex(CRITICAL_TRAVEL_KEYWORDS)


def _match_keywords(text_blob: str, keywords: Sequence[str], word_regex: Dict[str, re.Pattern]) -> List[str]:
    """
    Match keywords against normalized text_blob.
    - single words use word boundary regex to avoid false positives (e.g., "rail" in "trailer")
    - phrases use substring match
    """
    matches: List[str] = []
    for kw in keywords:
        if not kw:
            continue
        if " " in kw:
            if kw in text_blob:
                matches.append(kw)
        else:
            rx = word_regex.get(kw)
            if rx and rx.search(text_blob):
                matches.append(kw)
    return matches


def _extract_categories(categories: Optional[Iterable]) -> List[str]:
    results: List[str] = []
    for raw in categories or []:
        if isinstance(raw, dict):
            value = raw.get("term") or raw.get("label") or raw.get("value")
        else:
            value = raw
        text = normalize_text(str(value or ""))
        if text and text not in results:
            results.append(text)
    return results


# -----------------------------
# Relevance scoring
# -----------------------------

DEFAULT_EXCLUDE_SIGNALS = [
    "opinion",
    "editorial",
    "sports",
    "entertainment",
    "celebrity",
    "horoscope",
]


def should_exclude_item(*, url: str = "", title: str = "", summary: str = "") -> bool:
    """
    Cheap filter to avoid obvious non-travel-impact items.
    Keep it conservative to avoid breaking existing behavior.
    """
    blob = normalize_text(f"{url} {title} {summary}")
    for bad in getattr(config, "EXCLUDE_SIGNALS", DEFAULT_EXCLUDE_SIGNALS):
        bad_norm = normalize_text(bad)
        if bad_norm and bad_norm in blob:
            return True
    return False


def compute_travel_relevance(
    *,
    title: str,
    summary: str,
    content: str = "",
    categories: Optional[Iterable] = None,
    url: str = "",
    locale: Optional[str] = None,
) -> dict:
    """
    Returns:
      score, matched_keywords, regular_matches, strong_matches, category_matches,
      is_critical, reason, excluded
    """
    if should_exclude_item(url=url, title=title, summary=summary):
        return {
            "score": 0.0,
            "matched_keywords": [],
            "regular_matches": [],
            "strong_matches": [],
            "category_matches": [],
            "is_critical": False,
            "reason": "excluded",
            "excluded": True,
        }

    loc_normalized = normalize_text(locale or "").replace(" ", "") if locale else ""
    use_accent_folding = loc_normalized.startswith(("pt", "es", "fr", "de"))

    text_blob = normalize_text(f"{title} {summary} {content}")
    if use_accent_folding:
        text_blob_folded = fold_accents(f"{title} {summary} {content}")
    else:
        text_blob_folded = text_blob

    travel_kw, strong_kw, critical_kw = _build_locale_keywords(locale)

    # Use compiled regex for the global lists; for locale-merged lists, compile on the fly
    if locale:
        word_re_travel = _compile_word_regex(travel_kw)
        word_re_strong = _compile_word_regex(strong_kw)
        word_re_critical = _compile_word_regex(critical_kw)
    else:
        word_re_travel = _WORD_RE_TRAVEL
        word_re_strong = _WORD_RE_STRONG
        word_re_critical = _WORD_RE_CRITICAL

    # Match against both normal and folded text for better coverage
    regular_matches = _match_keywords(text_blob, travel_kw, word_re_travel)
    if use_accent_folding:
        regular_matches_folded = _match_keywords(text_blob_folded, travel_kw, word_re_travel)
        regular_matches = list(dict.fromkeys(regular_matches + regular_matches_folded))
    
    strong_matches = _match_keywords(text_blob, strong_kw, word_re_strong)
    if use_accent_folding:
        strong_matches_folded = _match_keywords(text_blob_folded, strong_kw, word_re_strong)
        strong_matches = list(dict.fromkeys(strong_matches + strong_matches_folded))

    category_matches: List[str] = []
    for cat in _extract_categories(categories):
        if any(sig in cat for sig in CATEGORY_SIGNAL_KEYWORDS):
            if cat not in category_matches:
                category_matches.append(cat)

    # Scoring: strong weighs more than regular; category adds small boost
    score = float(len(regular_matches) + len(strong_matches) * 2)

    if category_matches:
        score += min(len(category_matches), 4) * 0.5

    is_critical = any(_match_keywords(text_blob, [kw], word_re_critical) for kw in critical_kw)
    if is_critical:
        score += 1.5

    matched = list(dict.fromkeys(regular_matches + strong_matches + category_matches))

    reason_parts = []
    if strong_matches:
        reason_parts.append(f"strong={len(strong_matches)}")
    if regular_matches:
        reason_parts.append(f"regular={len(regular_matches)}")
    if category_matches:
        reason_parts.append("category_signal")
    if is_critical:
        reason_parts.append("critical")

    return {
        "score": score,
        "matched_keywords": matched,
        "regular_matches": regular_matches,
        "strong_matches": strong_matches,
        "category_matches": category_matches,
        "is_critical": bool(is_critical),
        "reason": ", ".join(reason_parts) if reason_parts else "no_signals",
        "excluded": False,
    }


def is_critical_event(
    *,
    title: str,
    summary: str,
    content: str = "",
    categories: Optional[Iterable] = None,
    locale: Optional[str] = None,
) -> bool:
    text_blob = normalize_text(f"{title} {summary} {content}")
    _, _, critical_kw = _build_locale_keywords(locale)
    word_re_critical = _compile_word_regex(critical_kw)

    # Critical keywords
    for kw in critical_kw:
        if " " in kw:
            if kw in text_blob:
                return True
        else:
            rx = word_re_critical.get(kw)
            if rx and rx.search(text_blob):
                return True

    # Category signals
    for cat in _extract_categories(categories):
        if any(sig in cat for sig in CATEGORY_SIGNAL_KEYWORDS):
            return True

    return False


# -----------------------------
# Publish time parsing with URL fallback
# -----------------------------

def _extract_date_from_url(url: Optional[str], now: datetime) -> Optional[datetime]:
    """
    Extract date from URL patterns like YYYY-MM-DD, YYYY/MM/DD, DD-MM-YYYY, DD/MM/YYYY.
    Returns UTC midnight datetime if found and within sanity window.
    """
    if not url:
        return None

    patterns = [
        # YYYY-MM-DD or YYYY/MM/DD
        r"(?P<y>20\d{2})[/-](?P<m>0[1-9]|1[0-2])[/-](?P<d>0[1-9]|[12]\d|3[01])",
        # DD-MM-YYYY or DD/MM/YYYY
        r"(?P<d>0[1-9]|[12]\d|3[01])[/-](?P<m>0[1-9]|1[0-2])[/-](?P<y>20\d{2})",
    ]

    for pat in patterns:
        m = re.search(pat, url)
        if not m:
            continue
        y = int(m.group("y"))
        mo = int(m.group("m"))
        d = int(m.group("d"))

        # sanity window to avoid random number matches
        if y < now.year - 10 or y > now.year + 1:
            continue

        try:
            # date only, keep UTC midnight
            return datetime(y, mo, d, 0, 0, 0, tzinfo=timezone.utc)
        except Exception:
            continue

    return None


def parse_published_at(
    entry,
    fetched_at: datetime,
    *,
    preferred_fields: Optional[Iterable[str]] = None,
    allow_future_skew_minutes: int = 60,
) -> Optional[datetime]:
    """
    Resolve published_at for RSS/Atom entries. Returns UTC datetime or None.

    NO FALLBACKS:
      - If no valid date is found, returns None (caller must skip or treat as data error).
      - Does NOT return fetched_at as a substitute.

    Future guard:
      - If parsed timestamp is too far in the future relative to fetched_at, returns None.
    """
    preferred_fields = tuple(preferred_fields or ("pubDate", "published", "updated"))

    def _get_value(key: str):
        if entry is None:
            return None
        if isinstance(entry, dict):
            return entry.get(key)
        if hasattr(entry, "get"):
            try:
                return entry.get(key)
            except Exception:
                return None
        return getattr(entry, key, None)

    def _coerce_dt(value) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            dt = value
        else:
            text = str(value).strip()
            if not text:
                return None

            # RFC 2822 / email date
            try:
                dt = parsedate_to_datetime(text)
            except Exception:
                dt = None

            # Common compact / ISO patterns
            if dt is None:
                for fmt in ("%Y%m%dT%H%M%SZ", "%Y%m%d%H%M%S", "%Y%m%d"):
                    try:
                        dt = datetime.strptime(text, fmt)
                        break
                    except ValueError:
                        dt = None

            if dt is None:
                try:
                    dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
                except ValueError:
                    return None

        if dt.tzinfo is None:
            # treat as UTC only if feed fails to specify tz; better than local-time ambiguity
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    # 1) Preferred raw fields
    for field in preferred_fields:
        candidate = _coerce_dt(_get_value(field))
        if candidate:
            return _reject_future(candidate, fetched_at, allow_future_skew_minutes)

    # 2) Dublin Core
    candidate = _coerce_dt(_get_value("dc:date"))
    if candidate:
        return _reject_future(candidate, fetched_at, allow_future_skew_minutes)

    # 3) Parsed tuples (feedparser style)
    for field in ("published_parsed", "updated_parsed"):
        parsed_value = _get_value(field)
        if parsed_value:
            try:
                dt = datetime.fromtimestamp(calendar.timegm(parsed_value), tz=timezone.utc)
                return _reject_future(dt, fetched_at, allow_future_skew_minutes)
            except Exception:
                # Don't try localtime conversion; treat as invalid to avoid silent skew
                continue

    # 4) URL fallback (covers Le Parisien style links with embedded dates)
    url = _get_value("link") or _get_value("url") or _get_value("guid") or _get_value("id")
    inferred = _extract_date_from_url(url, fetched_at)
    if inferred:
        return _reject_future(inferred, fetched_at, allow_future_skew_minutes)

    return None


def _reject_future(published_at: datetime, fetched_at: datetime, allow_future_skew_minutes: int) -> Optional[datetime]:
    if fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=timezone.utc)
    fetched_at = fetched_at.astimezone(timezone.utc)

    # Allow slight future skew due to feed timezone quirks
    if published_at > fetched_at + timedelta(minutes=allow_future_skew_minutes):
        return None
    return published_at
