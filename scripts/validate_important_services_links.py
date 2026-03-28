#!/usr/bin/env python3
import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
DEFAULT_TIMEOUT = 8
DEFAULT_RETRIES = 2

TRACKING_KEYS = {
    "gclid",
    "fbclid",
    "msclkid",
    "igshid",
    "mc_eid",
    "mc_cid",
}
TRACKING_PREFIXES = ("utm_",)
CORE_APP_CATEGORIES = {"rideshare", "local transportation apps"}
URL_FIELDS = ("url", "ios_url", "android_url")


@dataclass
class CheckResult:
    ok: bool
    status: int
    final_url: str
    note: str


def resolve_config_dir() -> Path:
    try:
        from config_registry import CONFIG_DIR_ENV
        override = os.getenv(CONFIG_DIR_ENV)
    except Exception:
        override = None

    if override:
        return Path(override).expanduser().resolve()

    return Path(__file__).resolve().parents[1] / "config_data"


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def dump_json(path: Path, payload: Any) -> None:
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    if not text.endswith("\n"):
        text += "\n"
    path.write_text(text, encoding="utf-8")


def strip_tracking_params(query: str) -> str:
    if not query:
        return ""
    filtered: List[Tuple[str, str]] = []
    for k, v in parse_qsl(query, keep_blank_values=True):
        kl = (k or "").lower()
        if kl in TRACKING_KEYS:
            continue
        if any(kl.startswith(prefix) for prefix in TRACKING_PREFIXES):
            continue
        filtered.append((k, v))
    return urlencode(filtered, doseq=True)


def normalize_url(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    if "://" not in raw:
        raw = f"https://{raw}"
    parsed = urlparse(raw)
    scheme = (parsed.scheme or "https").lower()
    if scheme not in {"http", "https"}:
        return ""
    host = (parsed.netloc or "").strip().lower()
    if not host:
        return ""
    query = strip_tracking_params(parsed.query)
    return urlunparse((scheme, host, parsed.path or "", "", query, ""))


def build_candidates(url: str) -> List[str]:
    url = normalize_url(url)
    if not url:
        return []
    parsed = urlparse(url)

    candidates: List[str] = []
    candidates.append(url)

    if parsed.path and parsed.path != "/":
        if parsed.path.endswith("/"):
            candidates.append(urlunparse(parsed._replace(path=parsed.path.rstrip("/"))))
        else:
            candidates.append(urlunparse(parsed._replace(path=parsed.path + "/")))

    if parsed.query or parsed.fragment:
        candidates.append(urlunparse(parsed._replace(query="", fragment="")))

    candidates.append(urlunparse(parsed._replace(path="/", query="", fragment="")))

    if parsed.scheme == "http":
        candidates.append(urlunparse(parsed._replace(scheme="https")))
    elif parsed.scheme == "https":
        candidates.append(urlunparse(parsed._replace(scheme="http")))

    seen: set[str] = set()
    uniq: List[str] = []
    for c in candidates:
        c = normalize_url(c)
        if not c:
            continue
        if c in seen:
            continue
        seen.add(c)
        uniq.append(c)
    return uniq


def same_host(a: str, b: str) -> bool:
    pa = urlparse(normalize_url(a))
    pb = urlparse(normalize_url(b))
    return (pa.netloc or "").lower() == (pb.netloc or "").lower()


def fetch_url(session: requests.Session, url: str, timeout: int, retries: int) -> CheckResult:
    last_err = ""
    for attempt in range(retries + 1):
        try:
            try:
                r = session.head(url, allow_redirects=True, timeout=timeout)
                status = int(getattr(r, "status_code", 0) or 0)
                final_url = normalize_url(getattr(r, "url", "") or url)
                if status == 405 or status == 501:
                    raise RuntimeError("head not allowed")
                return CheckResult(ok=True, status=status, final_url=final_url, note="head")
            except Exception:
                r = session.get(url, allow_redirects=True, timeout=timeout)
                status = int(getattr(r, "status_code", 0) or 0)
                final_url = normalize_url(getattr(r, "url", "") or url)
                return CheckResult(ok=True, status=status, final_url=final_url, note="get")
        except Exception as e:
            last_err = str(e)
            if attempt < retries:
                time.sleep(0.6 * (attempt + 1))
                continue
            break
    return CheckResult(ok=False, status=0, final_url=normalize_url(url), note=last_err or "request failed")


def classify(status: int) -> str:
    if 200 <= status < 400:
        return "ok"
    if status in {401, 403}:
        return "restricted"
    if status in {404, 410}:
        return "broken"
    if status == 0:
        return "error"
    if 400 <= status < 500:
        return "client"
    if 500 <= status < 600:
        return "server"
    return "unknown"


def iter_service_items(city_payload: Dict[str, Any]) -> Iterable[Tuple[str, Any, Dict[str, Any]]]:
    raw = city_payload.get("important_services")
    if isinstance(raw, list):
        for idx, item in enumerate(raw):
            if isinstance(item, dict):
                yield ("list", idx, item)
        return
    if isinstance(raw, dict):
        for category, entries in raw.items():
            if not isinstance(entries, list):
                continue
            for idx, item in enumerate(entries):
                if isinstance(item, dict):
                    yield ("dict", (category, idx), item)


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def iter_labeled_service_items(
    city_payload: Dict[str, Any]
) -> Iterable[Tuple[str, Any, Dict[str, Any], str, str]]:
    for container_kind, key_ref, item in iter_service_items(city_payload):
        category = _clean_text(item.get("category") or item.get("type"))
        if not category and container_kind == "dict":
            category = _clean_text((key_ref or ("", 0))[0])
        label = _clean_text(item.get("label") or item.get("name") or item.get("title"))
        yield (container_kind, key_ref, item, category, label)


def _service_key(category: str, label: str) -> Tuple[str, str]:
    return (_clean_text(category).lower(), _clean_text(label).lower())


def _has_explicit_verified_store_link(item: Dict[str, Any]) -> bool:
    ios = _clean_text(item.get("ios_url"))
    android = _clean_text(item.get("android_url"))
    if not ios and not android:
        return False
    status = _clean_text(item.get("verification_status")).lower()
    if status in {"generated_fallback", "unverified", "needs_verification"}:
        return False
    return True


def validate_city_static_rules(city_key: str, city_payload: Dict[str, Any]) -> List[str]:
    errors: List[str] = []

    explicit_verified_links: Dict[Tuple[str, str], bool] = {}
    for _, _, item, category, label in iter_labeled_service_items(city_payload):
        if not category or not label:
            continue
        key = _service_key(category, label)
        explicit_verified_links[key] = (
            explicit_verified_links.get(key, False) or _has_explicit_verified_store_link(item)
        )

        for field in URL_FIELDS:
            raw_url = _clean_text(item.get(field))
            if not raw_url:
                continue
            if not normalize_url(raw_url):
                errors.append(
                    f"{city_key}: malformed configured {field} for '{label}' ({category}): {raw_url}"
                )

    try:
        from report_data_contract import build_important_services
    except Exception as exc:
        errors.append(f"{city_key}: unable to import build_important_services ({exc})")
        return errors

    transit_list = city_payload.get("transit_systems")
    if not isinstance(transit_list, list):
        transit_list = []
    normalized_services = build_important_services(city_payload, transit_list)

    for service in normalized_services:
        if not isinstance(service, dict):
            continue
        category = _clean_text(service.get("category"))
        label = _clean_text(service.get("label"))
        if not category or not label:
            continue

        if category.lower() in CORE_APP_CATEGORIES:
            ios = _clean_text(service.get("ios_url"))
            android = _clean_text(service.get("android_url"))
            if not ios and not android:
                errors.append(
                    f"{city_key}: core app '{label}' ({category}) is missing both ios_url and android_url"
                )

            if bool(service.get("fallback_generated")) and explicit_verified_links.get(
                _service_key(category, label),
                False,
            ):
                errors.append(
                    f"{city_key}: '{label}' ({category}) is marked fallback_generated "
                    "despite explicit verified config link"
                )

    return errors


def set_item_url(city_payload: Dict[str, Any], container_kind: str, key_ref: Any, field: str, value: str) -> None:
    raw = city_payload.get("important_services")
    if container_kind == "list" and isinstance(raw, list):
        idx = int(key_ref)
        if 0 <= idx < len(raw) and isinstance(raw[idx], dict):
            raw[idx][field] = value
    if container_kind == "dict" and isinstance(raw, dict):
        category, idx = key_ref
        entries = raw.get(category)
        if isinstance(entries, list) and 0 <= idx < len(entries) and isinstance(entries[idx], dict):
            entries[idx][field] = value


def validate_and_repair_city(
    city_key: str,
    city_path: Path,
    city_payload: Dict[str, Any],
    *,
    session: requests.Session,
    timeout: int,
    retries: int,
    write: bool,
) -> Tuple[int, int, int]:
    checked = 0
    updated = 0
    broken = 0

    fields = URL_FIELDS

    for container_kind, key_ref, item in iter_service_items(city_payload):
        for field in fields:
            raw_url = (item.get(field) or "").strip()
            if not raw_url:
                continue

            checked += 1
            normalized = normalize_url(raw_url)
            if not normalized:
                broken += 1
                print(f"[BROKEN] {city_key} {item.get('label','')} {field} invalid url: {raw_url}")
                continue

            primary = fetch_url(session, normalized, timeout, retries)
            status = primary.status if primary.ok else 0
            state = classify(status)

            if state == "ok":
                final_url = primary.final_url or normalized
                if final_url and final_url != raw_url:
                    set_item_url(city_payload, container_kind, key_ref, field, final_url)
                    updated += 1
                    print(f"[FIXED] {city_key} {item.get('label','')} {field} -> {final_url}")
                else:
                    print(f"[OK] {city_key} {item.get('label','')} {field} {status}")
                continue

            if state == "restricted":
                print(f"[WARN] {city_key} {item.get('label','')} {field} restricted {status} {normalized}")
                continue

            if state == "broken":
                fixed = False
                for cand in build_candidates(normalized):
                    if not same_host(normalized, cand):
                        continue
                    res = fetch_url(session, cand, timeout, retries)
                    st = res.status if res.ok else 0
                    if 200 <= st < 400:
                        final_url = res.final_url or cand
                        set_item_url(city_payload, container_kind, key_ref, field, final_url)
                        updated += 1
                        fixed = True
                        print(f"[FIXED] {city_key} {item.get('label','')} {field} {normalized} -> {final_url}")
                        break
                if not fixed:
                    broken += 1
                    print(f"[BROKEN] {city_key} {item.get('label','')} {field} {status} {normalized}")
                continue

            print(f"[WARN] {city_key} {item.get('label','')} {field} {status} {normalized}")

    if write and updated > 0:
        dump_json(city_path, city_payload)

    return checked, updated, broken


def validate_db_reports(
    *,
    city_keys: List[str],
    timeout: int,
    retries: int,
) -> int:
    try:
        from storage.supabase_store import SupabaseStore
    except Exception as e:
        print(f"DB check unavailable: {e}")
        return 0

    try:
        store = SupabaseStore()
    except Exception as e:
        print(f"DB check skipped: {e}")
        return 0

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    broken = 0
    for city_key in city_keys:
        record = store.get_city_report(city_key)
        if not record or not isinstance(record.get("report_data"), dict):
            print(f"[WARN] db {city_key} missing report_data")
            continue
        services = record["report_data"].get("important_services") or []
        if not isinstance(services, list):
            continue
        for item in services:
            if not isinstance(item, dict):
                continue
            for field in ("url", "ios_url", "android_url"):
                raw_url = (item.get(field) or "").strip()
                if not raw_url:
                    continue
                url = normalize_url(raw_url)
                res = fetch_url(session, url, timeout, retries)
                st = res.status if res.ok else 0
                state = classify(st)
                if state == "broken":
                    broken += 1
                    print(f"[BROKEN] db {city_key} {item.get('label','')} {field} {st} {url}")
    return broken


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--city", default="", help="single city key to check")
    parser.add_argument("--write", action="store_true", help="write repaired urls back into config json")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--db", action="store_true", help="also validate urls stored in city_reports.report_data")
    parser.add_argument(
        "--strict",
        action="store_true",
        help=(
            "run strict static checks: malformed configured URLs, missing core app links, "
            "and invalid fallback_generated usage"
        ),
    )
    parser.add_argument(
        "--no-network",
        action="store_true",
        help="skip live HTTP checks and only run static checks",
    )
    args = parser.parse_args()

    try:
        from config_registry import load_registry
        cities, _ = load_registry()
    except Exception as exc:
        print(f"Failed to load city registry: {exc}")
        return 1

    config_dir = resolve_config_dir()
    cities_dir = config_dir / "cities"

    city_keys = []
    for city_key, city_cfg in (cities or {}).items():
        if not isinstance(city_cfg, dict):
            continue
        if not city_cfg.get("enabled", True):
            continue
        if args.city and city_key != args.city:
            continue
        city_keys.append(city_key)

    if not city_keys:
        print("No enabled cities selected")
        return 1

    session = None
    if not args.no_network:
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})

    total_checked = 0
    total_updated = 0
    total_broken = 0
    total_static_errors = 0

    for city_key in sorted(city_keys):
        city_path = cities_dir / f"{city_key}.json"
        if not city_path.exists():
            print(f"[WARN] missing city config file: {city_path}")
            continue
        payload = load_json(city_path)
        if not isinstance(payload, dict):
            print(f"[WARN] invalid city json object: {city_path}")
            continue

        print(f"\nChecking {city_key}")
        if args.strict:
            static_errors = validate_city_static_rules(city_key, payload)
            total_static_errors += len(static_errors)
            for error in static_errors:
                print(f"[ERROR] {error}")

        if not args.no_network:
            checked, updated, broken = validate_and_repair_city(
                city_key,
                city_path,
                payload,
                session=session,
                timeout=args.timeout,
                retries=args.retries,
                write=bool(args.write),
            )
            total_checked += checked
            total_updated += updated
            total_broken += broken

    print(f"\nSummary checked={total_checked} updated={total_updated} broken={total_broken}")
    if args.strict:
        print(f"Strict summary errors={total_static_errors}")
        total_broken += int(total_static_errors)

    if args.db:
        db_broken = validate_db_reports(city_keys=sorted(city_keys), timeout=args.timeout, retries=args.retries)
        print(f"DB summary broken={db_broken}")
        total_broken += int(db_broken)

    return 0 if total_broken == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
