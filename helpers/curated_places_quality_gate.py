"""
Curated places quality gate: normalize, verify, and optionally enrich websites.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple, Iterable
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode, unquote
from html import unescape
from html.parser import HTMLParser
import re
import unicodedata

import requests
import tldextract

import config
from storage.normalize import is_aggregator_domain
from helpers.google_places import get_google_places


DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

REQUEST_TIMEOUT = (5, 15)

TRACKING_PARAM_KEYS = {
    "fbclid",
    "gclid",
    "igshid",
    "mc_cid",
    "mc_eid",
    "msclkid",
    "s_kwcid",
    "vero_id",
}

TRACKING_PARAM_PREFIXES = (
    "utm_",
    "pk_",
    "ref_",
)

BLOCKED_REDIRECT_DOMAINS = {
    "validate.perfdrive.com",
    "perfdrive.com",
    "validate.perfdrive.net",
}

DIRECTORY_SEGMENTS = {
    "directory",
    "doctor",
    "doctors",
    "find-a-clinic",
    "find-a-doctor",
    "find-a-location",
    "find-a-provider",
    "finder",
    "location",
    "locations",
    "physician",
    "physicians",
    "provider",
    "providers",
    "search",
    "service-search",
    "service_search",
    "services",
}

DEFAULT_403_ALLOWLIST = {
    "airport": {
        "aena.es",
        "aena.aero",
        "adp.com",
        "heathrow.com",
        "gatwickairport.com",
        "aeroports-paris.com",
        "schiphol.nl",
    },
    "rental_car": {
        "alamo.com",
        "avis.com",
        "budget.com",
        "dollar.com",
        "enterprise.com",
        "europcar.com",
        "hertz.com",
        "nationalcar.com",
        "sixt.com",
        "thrifty.com",
    },
}

NAME_STOPWORDS = {
    "hospital",
    "hospitals",
    "clinic",
    "clinics",
    "medical",
    "health",
    "centre",
    "center",
    "university",
    "college",
    "general",
    "trust",
    "nhs",
    "saint",
    "st",
    "royal",
    "urgent",
    "care",
    "pharmacy",
    "pharmacies",
    "supermarket",
    "supermarkets",
    "convenience",
    "store",
    "stores",
    "airport",
    "airports",
    "international",
    "rental",
    "rent",
    "car",
    "cars",
    "the",
    "and",
    "of",
    "de",
    "del",
    "la",
    "las",
    "los",
    "el",
    "y",
    "da",
    "do",
    "das",
    "dos",
}


@dataclass
class VerificationResult:
    status: str
    url: Optional[str]
    reason: str
    domain: Optional[str]
    identity_ok: bool
    hard_ok: bool


class _SignalParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._in_title = False
        self._in_h1 = False
        self._seen_h1 = False
        self.title = ""
        self.h1 = ""
        self.og_site_name = ""

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        attrs_dict = {k.lower(): (v or "") for k, v in attrs}
        if tag.lower() == "title":
            self._in_title = True
        elif tag.lower() == "h1" and not self._seen_h1:
            self._in_h1 = True
        elif tag.lower() == "meta" and not self.og_site_name:
            prop = (attrs_dict.get("property") or attrs_dict.get("name") or "").lower()
            if prop in {"og:site_name", "og:site-name"}:
                self.og_site_name = attrs_dict.get("content", "") or ""

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "title":
            self._in_title = False
        elif tag.lower() == "h1":
            self._in_h1 = False
            if self.h1.strip():
                self._seen_h1 = True

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self.title += data
        if self._in_h1 and not self._seen_h1:
            self.h1 += data


def _normalize_text(text: str) -> str:
    text = unescape(text or "")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _name_tokens(name: str) -> list[str]:
    text = _normalize_text(name or "")
    tokens = re.split(r"[^a-z0-9]+", text)
    filtered = [t for t in tokens if t and t not in NAME_STOPWORDS and len(t) >= 3]
    return filtered


def _strip_tracking_params(query: str) -> str:
    if not query:
        return ""
    kept = []
    for key, value in parse_qsl(query, keep_blank_values=True):
        key_lower = key.lower()
        if key_lower in TRACKING_PARAM_KEYS:
            continue
        if any(key_lower.startswith(prefix) for prefix in TRACKING_PARAM_PREFIXES):
            continue
        kept.append((key, value))
    return urlencode(kept, doseq=True)


def _normalize_url(raw: str) -> Optional[str]:
    if not raw:
        return None
    text = str(raw).strip()
    if not text:
        return None
    if "://" not in text:
        text = f"https://{text}"
    parsed = urlparse(text)
    host = (parsed.netloc or "").strip()
    if not host:
        return None
    if "@" in host:
        host = host.split("@", 1)[1]
    if ":" in host:
        host_name, port = host.rsplit(":", 1)
        if port in {"80", "443"}:
            host = host_name
    host = re.sub(r"^(?:www\.){2,}", "www.", host.lower())
    path = parsed.path or ""
    if path == "/":
        path = ""
    elif path.endswith("/"):
        path = path[:-1]
    query = _strip_tracking_params(parsed.query or "")
    normalized = urlunparse(("https", host, path, "", query, ""))
    return normalized


def _extract_domain(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    parsed = urlparse(url if "://" in url else f"https://{url}")
    host = (parsed.hostname or "").strip().lower()
    if not host:
        return None
    ext = tldextract.extract(host)
    if ext.domain and ext.suffix:
        return f"{ext.domain}.{ext.suffix}".lower()
    return host


def _is_blocked_redirect_domain(domain: Optional[str]) -> bool:
    if not domain:
        return False
    for blocked in BLOCKED_REDIRECT_DOMAINS:
        if domain == blocked or domain.endswith(f".{blocked}"):
            return True
    return False


def _recover_perfdrive_target(url: str) -> Optional[str]:
    parsed = urlparse(url)
    if not parsed.netloc:
        return None
    domain = _extract_domain(url)
    if not domain or "perfdrive" not in domain:
        return None
    params = dict(parse_qsl(parsed.query or "", keep_blank_values=True))
    target = params.get("ssc") or ""
    target = unquote(target.strip())
    if target.lower().startswith("http"):
        return target
    return None


def _is_directory_url(url: str) -> bool:
    parsed = urlparse(url if "://" in url else f"https://{url}")
    host = (parsed.hostname or "").lower().strip()
    if host in {"nhs.uk", "www.nhs.uk"}:
        path = (parsed.path or "").lower()
        if not path or path == "/":
            return True
        if path.startswith("/services") or path.startswith("/service-search") or "/service-search" in path:
            return True
    path = (parsed.path or "").lower()
    segments = [segment for segment in path.split("/") if segment]
    return any(segment in DIRECTORY_SEGMENTS for segment in segments)


def _load_403_allowlist(category: str) -> set[str]:
    allowlist = {}
    try:
        allowlist = getattr(config, "CURATED_PLACE_403_ALLOWLIST", {}) or {}
    except Exception:
        allowlist = {}
    merged = set(DEFAULT_403_ALLOWLIST.get(category, set()))
    if isinstance(allowlist, dict):
        merged.update({str(d).lower() for d in allowlist.get(category, []) if str(d).strip()})
    return merged


def _is_allowlisted_403(domain: Optional[str], category: str) -> bool:
    if not domain:
        return False
    allowlist = _load_403_allowlist(category)
    return any(domain == allowed or domain.endswith(f".{allowed}") for allowed in allowlist)


def _fetch_url(session: requests.Session, method: str, url: str) -> Tuple[Optional[requests.Response], Optional[str]]:
    headers = {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    try:
        resp = session.request(
            method,
            url,
            headers=headers,
            allow_redirects=True,
            timeout=REQUEST_TIMEOUT,
        )
        return resp, None
    except requests.exceptions.TooManyRedirects:
        return None, "redirect_loop"
    except requests.exceptions.RequestException:
        return None, "request_error"


def _check_identity(name: str, html_text: str) -> Tuple[bool, str]:
    tokens = _name_tokens(name)
    if not tokens:
        return False, "name_tokens_missing"
    if not html_text:
        return False, "signals_missing"
    parser = _SignalParser()
    try:
        parser.feed(html_text)
    except Exception:
        return False, "signals_parse_failed"
    signals_text = _normalize_text(" ".join([parser.title, parser.og_site_name, parser.h1]))
    if not signals_text:
        return False, "signals_missing"
    if any(token in signals_text for token in tokens):
        return True, ""
    return False, "identity_mismatch"


def verify_place_website(raw_url: Optional[str], category: str, name: str) -> VerificationResult:
    normalized = _normalize_url(raw_url or "")
    if not normalized:
        return VerificationResult("reject", None, "invalid_url", None, False, False)
    if is_aggregator_domain(normalized):
        return VerificationResult("reject", normalized, "aggregator_domain", _extract_domain(normalized), False, False)
    if _is_directory_url(normalized):
        return VerificationResult("reject", normalized, "directory_url", _extract_domain(normalized), False, False)

    session = requests.Session()
    response, error = _fetch_url(session, "HEAD", normalized)
    used_get = False
    if error == "redirect_loop":
        return VerificationResult("reject", None, "redirect_loop", None, False, False)
    if response is None or response.status_code in {400, 403, 405}:
        response, error = _fetch_url(session, "GET", normalized)
        used_get = True
    if error == "redirect_loop":
        return VerificationResult("reject", None, "redirect_loop", None, False, False)
    if response is None:
        return VerificationResult("reject", None, "unreachable", None, False, False)

    status_code = int(response.status_code)
    if status_code in {404, 410}:
        return VerificationResult("reject", normalized, "not_found", _extract_domain(normalized), False, False)
    if status_code >= 500:
        return VerificationResult("reject", normalized, "server_error", _extract_domain(normalized), False, False)
    if 300 <= status_code < 400:
        return VerificationResult("reject", normalized, "redirect_loop", _extract_domain(normalized), False, False)
    if status_code not in {200, 204, 403}:
        return VerificationResult("reject", normalized, f"status_{status_code}", _extract_domain(normalized), False, False)

    warnings = []
    final_url = _normalize_url(response.url or normalized)
    final_domain = _extract_domain(final_url)
    redirect_warning = None
    if final_domain and _is_blocked_redirect_domain(final_domain):
        for hist in reversed(response.history or []):
            hist_norm = _normalize_url(hist.url)
            hist_domain = _extract_domain(hist_norm)
            if hist_norm and not _is_blocked_redirect_domain(hist_domain):
                final_url = hist_norm
                final_domain = hist_domain
                redirect_warning = "blocked_redirect_history"
                break
        if _is_blocked_redirect_domain(final_domain):
            recovered = _recover_perfdrive_target(response.url or "")
            if recovered:
                recovered_norm = _normalize_url(recovered)
                recovered_domain = _extract_domain(recovered_norm)
                if recovered_norm and not _is_blocked_redirect_domain(recovered_domain):
                    final_url = recovered_norm
                    final_domain = recovered_domain
                    redirect_warning = "blocked_redirect_recovered"
        if _is_blocked_redirect_domain(final_domain):
            final_url = normalized
            final_domain = _extract_domain(normalized)
            redirect_warning = "blocked_redirect_fallback"
    if redirect_warning:
        warnings.append(redirect_warning)

    if final_url and is_aggregator_domain(final_url):
        return VerificationResult("reject", final_url, "aggregator_domain", final_domain, False, False)
    if final_url and _is_directory_url(final_url):
        return VerificationResult("reject", final_url, "directory_url", final_domain, False, False)

    if status_code == 403:
        if _is_allowlisted_403(final_domain, category):
            warnings.append("forbidden_allowlisted")
        else:
            return VerificationResult("reject", final_url, "forbidden", final_domain, False, False)

    if not used_get:
        response, error = _fetch_url(session, "GET", response.url or normalized)
        if error == "redirect_loop":
            return VerificationResult("reject", final_url, "redirect_loop", final_domain, False, False)
        if response is None:
            warnings.append("html_unreachable")
    content_type = (response.headers.get("Content-Type") or "").lower() if response else ""
    html_text = ""
    if response is not None and "html" in content_type:
        try:
            html_text = response.text[:200000]
        except Exception:
            html_text = ""
    else:
        if response is not None:
            warnings.append("non_html")

    identity_ok, identity_reason = _check_identity(name, html_text)
    if not identity_ok and identity_reason:
        warnings.append(identity_reason)

    status = "ok" if not warnings else "warn"
    reason = ";".join(warnings) if warnings else "verified"
    return VerificationResult(status, final_url, reason, final_domain, identity_ok, True)


def _google_places_lookup(name: str, lat: Optional[float], lon: Optional[float], category: str) -> Optional[str]:
    if not name or lat is None or lon is None:
        return None
    try:
        google = get_google_places()
        return google.lookup_place_website(name=name, latitude=lat, longitude=lon, category=category)
    except Exception:
        return None


def quality_gate_curated_place(
    city_key: str,
    category: str,
    name: str,
    website: Optional[str],
    city_lat: Optional[float],
    city_lon: Optional[float],
) -> Tuple[Optional[str], str, str, Optional[str]]:
    candidate_result = None
    candidate_status = "missing"
    candidate_reason = ""
    if website:
        candidate_result = verify_place_website(website, category, name)
        candidate_status = candidate_result.status
        candidate_reason = candidate_result.reason

    candidate_suspicious = candidate_status in {"warn", "reject", "missing"}

    google_result = None
    google_url = None
    if candidate_suspicious:
        google_url = _google_places_lookup(name, city_lat, city_lon, category)
        if google_url:
            google_result = verify_place_website(google_url, category, name)

    if candidate_status == "ok":
        return candidate_result.url, "ok", "candidate_verified", "osm"

    if google_result and google_result.status == "ok":
        candidate_domain = candidate_result.domain if candidate_result else None
        google_domain = google_result.domain
        if candidate_domain and google_domain and candidate_domain == google_domain:
            reason_bits = ["google_domain_match"]
            if candidate_reason:
                reason_bits.append(f"candidate:{candidate_reason}")
            return google_result.url, "ok", ";".join(reason_bits), "google_places"
        if candidate_result and candidate_result.identity_ok and candidate_result.hard_ok:
            reason_bits = ["domain_disagreement"]
            if candidate_reason:
                reason_bits.append(f"candidate:{candidate_reason}")
            return candidate_result.url, "warn", ";".join(reason_bits), "osm"
        reason_bits = []
        if candidate_reason:
            reason_bits.append(f"candidate:{candidate_reason}")
        reason_bits.append("google_ok")
        return google_result.url, "ok", ";".join(reason_bits), "google_places"

    if candidate_status in {"missing", "warn"}:
        if candidate_result:
            final_url = candidate_result.url
        elif google_result and google_result.status != "reject":
            final_url = google_result.url
        else:
            final_url = None
        reason_bits = []
        if candidate_reason:
            reason_bits.append(f"candidate:{candidate_reason}")
        if google_result:
            reason_bits.append(f"google:{google_result.reason}")
        else:
            reason_bits.append("google_missing")
        return final_url, "warn", ";".join(reason_bits) or "suspicious", "osm" if final_url else None

    if candidate_status == "reject":
        if google_result and google_result.status == "warn":
            reason = f"candidate:{candidate_reason};google:{google_result.reason}"
            return google_result.url, "warn", reason, "google_places"
        reason = candidate_reason or "candidate_reject"
        return None, "reject", reason, None

    return None, "warn", "unhandled_state", None
