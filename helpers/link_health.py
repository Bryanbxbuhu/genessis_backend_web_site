from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict
from urllib.parse import urlparse, urlunparse

import requests


DEFAULT_TIMEOUT_SECONDS = 5


@dataclass(frozen=True)
class LinkValidationResult:
    ok: bool
    status_code: int
    final_url: str


def _normalize_http_url(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    parsed = urlparse(text)
    scheme = (parsed.scheme or "").lower()
    if scheme not in {"http", "https"}:
        return ""
    host = (parsed.netloc or "").strip().lower()
    if not host:
        return ""
    return urlunparse(
        (
            scheme,
            host,
            parsed.path or "",
            parsed.params or "",
            parsed.query or "",
            parsed.fragment or "",
        )
    )


def _status_ok(status_code: int) -> bool:
    return 200 <= int(status_code or 0) < 400


def validate_url(
    url: str,
    *,
    session: requests.Session | None = None,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
) -> LinkValidationResult:
    normalized = _normalize_http_url(url)
    if not normalized:
        return LinkValidationResult(ok=False, status_code=0, final_url="")

    owns_session = session is None
    request_session = session or requests.Session()

    try:
        head_status = 0
        head_final = normalized
        try:
            response = request_session.head(normalized, allow_redirects=True, timeout=timeout)
            head_status = int(getattr(response, "status_code", 0) or 0)
            head_final = _normalize_http_url(getattr(response, "url", "")) or normalized
            if _status_ok(head_status):
                return LinkValidationResult(ok=True, status_code=head_status, final_url=head_final)
        except Exception:
            head_status = 0

        try:
            response = request_session.get(normalized, allow_redirects=True, timeout=timeout)
            get_status = int(getattr(response, "status_code", 0) or 0)
            get_final = _normalize_http_url(getattr(response, "url", "")) or normalized
            return LinkValidationResult(
                ok=_status_ok(get_status),
                status_code=get_status,
                final_url=get_final,
            )
        except Exception:
            return LinkValidationResult(ok=False, status_code=head_status, final_url=head_final)
    finally:
        if owns_session:
            request_session.close()


def _government_fallback_candidates(url: str) -> list[str]:
    normalized = _normalize_http_url(url)
    if not normalized:
        return []

    parsed = urlparse(normalized)
    candidates: list[str] = []

    if not (parsed.path or "").endswith("/"):
        trailing_path = f"{parsed.path}/" if parsed.path else "/"
        candidates.append(urlunparse(parsed._replace(path=trailing_path)))

    root_scheme = parsed.scheme
    if parsed.scheme == "http":
        https_url = urlunparse(parsed._replace(scheme="https"))
        candidates.append(https_url)
        root_scheme = "https"

    candidates.append(urlunparse(parsed._replace(scheme=root_scheme, path="/", params="", query="", fragment="")))

    deduped: list[str] = []
    seen: set[str] = {normalized}
    for candidate in candidates:
        candidate_normalized = _normalize_http_url(candidate)
        if not candidate_normalized or candidate_normalized in seen:
            continue
        seen.add(candidate_normalized)
        deduped.append(candidate_normalized)
    return deduped


def sanitize_government_service_links(
    report_data: Dict[str, Any],
    *,
    session: requests.Session | None = None,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    logger: Callable[[str], None] | None = None,
) -> Dict[str, int]:
    stats = {"checked": 0, "rewritten": 0, "removed": 0}
    if not isinstance(report_data, dict):
        return stats

    services = report_data.get("important_services")
    if not isinstance(services, list):
        return stats

    log = logger or (lambda msg: print(msg))
    owns_session = session is None
    request_session = session or requests.Session()

    try:
        for item in services:
            if not isinstance(item, dict):
                continue

            category = str(item.get("category") or "").strip().lower()
            if category != "government":
                continue

            original_url = str(item.get("url") or "").strip()
            if not original_url:
                continue

            label = str(item.get("label") or "Government service").strip() or "Government service"
            stats["checked"] += 1

            primary_result = validate_url(original_url, session=request_session, timeout=timeout)
            normalized_original = _normalize_http_url(original_url)
            if primary_result.ok:
                final_url = primary_result.final_url or normalized_original or original_url
                if final_url and final_url != original_url:
                    item["url"] = final_url
                    stats["rewritten"] += 1
                    log(f"Warning: Government link rewritten for {label}: {original_url} -> {final_url}")
                continue

            replacement = ""
            for candidate in _government_fallback_candidates(original_url):
                result = validate_url(candidate, session=request_session, timeout=timeout)
                if result.ok:
                    replacement = result.final_url or candidate
                    break

            if replacement:
                item["url"] = replacement
                stats["rewritten"] += 1
                log(f"Warning: Government link rewritten for {label}: {original_url} -> {replacement}")
            else:
                item["url"] = ""
                stats["removed"] += 1
                log(f"Warning: Government link removed for {label}: {original_url}")

        city_essentials = report_data.get("city_essentials")
        if isinstance(city_essentials, dict):
            city_essentials["important_services"] = services
    finally:
        if owns_session:
            request_session.close()

    return stats

