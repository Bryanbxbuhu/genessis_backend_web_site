#!/usr/bin/env python3
"""Fail when a scheduled update leaves stale or missing city data."""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@dataclass(frozen=True)
class FreshnessProblem:
    resource: str
    city_key: str
    reason: str

    def __str__(self) -> str:
        return f"{self.resource}/{self.city_key}: {self.reason}"


def parse_utc_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def evaluate_city_rows(
    *,
    resource: str,
    city_keys: Sequence[str],
    rows: Iterable[Mapping[str, Any]],
    timestamp_field: str,
    max_age_hours: float,
    now: datetime | None = None,
    future_tolerance_minutes: float = 5,
) -> list[FreshnessProblem]:
    """Return missing, invalid, stale, or implausibly future-dated row problems."""
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    newest_by_city: dict[str, datetime] = {}

    for row in rows:
        city_key = str(row.get("city_key") or "").strip()
        timestamp = parse_utc_timestamp(row.get(timestamp_field))
        if city_key and timestamp:
            previous = newest_by_city.get(city_key)
            if previous is None or timestamp > previous:
                newest_by_city[city_key] = timestamp

    problems: list[FreshnessProblem] = []
    for city_key in city_keys:
        timestamp = newest_by_city.get(city_key)
        if timestamp is None:
            problems.append(FreshnessProblem(resource, city_key, "missing or invalid timestamp"))
            continue

        age_hours = (current - timestamp).total_seconds() / 3600
        if age_hours < -(future_tolerance_minutes / 60):
            problems.append(
                FreshnessProblem(resource, city_key, f"timestamp is {-age_hours:.2f}h in the future")
            )
        elif age_hours > max_age_hours:
            problems.append(
                FreshnessProblem(
                    resource,
                    city_key,
                    f"stale ({age_hours:.2f}h old; maximum is {max_age_hours:.2f}h)",
                )
            )

    return problems


def build_supabase_client() -> Any:
    """Create a read-only Supabase client without importing the app config.

    This check is a pure database read, so it deliberately avoids
    ``storage.supabase_store`` (and therefore ``config``), whose import-time
    CI guard demands unrelated secrets such as OPENAI_API_KEY and RAPIDAPI_KEY.
    """
    # Import order matters: helpers.tls pulls in truststore, which snapshots
    # ssl.SSLContext at import time. Importing supabase first leaves truststore
    # snapshotting an already-patched class and recursing on context setup.
    from helpers.tls import verified_httpx_client

    from dotenv import load_dotenv
    from supabase import ClientOptions, create_client

    # Match config.py: local secrets come from .env.local only, never .env.
    env_local_file = PROJECT_ROOT / ".env.local"
    if env_local_file.exists():
        load_dotenv(env_local_file, override=False)

    supabase_url = (os.getenv("SUPABASE_URL") or "").strip()
    supabase_key = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    if not supabase_url or not supabase_key:
        raise RuntimeError(
            "Missing Supabase credentials. Set SUPABASE_URL and "
            "SUPABASE_SERVICE_ROLE_KEY environment variables."
        )

    options = ClientOptions(httpx_client=verified_httpx_client())
    return create_client(supabase_url, supabase_key, options=options)


def fetch_rows(client: Any, table: str, fields: str, city_keys: Sequence[str]) -> list[dict[str, Any]]:
    response = client.table(table).select(fields).in_("city_key", list(city_keys)).execute()
    rows = response.data or []
    if not isinstance(rows, list):
        raise RuntimeError(f"Unexpected {table} response: expected a list")
    return [row for row in rows if isinstance(row, dict)]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cities", nargs="+", required=True, help="city keys expected in this update")
    parser.add_argument(
        "--max-age-hours",
        type=float,
        default=6,
        help="maximum allowed age for reports and weather rows (default: 6)",
    )
    args = parser.parse_args()

    city_keys = list(dict.fromkeys(key.strip() for key in args.cities if key.strip()))
    if not city_keys:
        parser.error("at least one non-empty city key is required")
    if args.max_age_hours <= 0:
        parser.error("--max-age-hours must be positive")

    client = build_supabase_client()
    report_rows = fetch_rows(client, "city_reports", "city_key,generated_at", city_keys)
    weather_rows = fetch_rows(client, "weather_forecasts", "city_key,fetched_at", city_keys)

    problems = evaluate_city_rows(
        resource="city_reports",
        city_keys=city_keys,
        rows=report_rows,
        timestamp_field="generated_at",
        max_age_hours=args.max_age_hours,
    )
    problems.extend(
        evaluate_city_rows(
            resource="weather_forecasts",
            city_keys=city_keys,
            rows=weather_rows,
            timestamp_field="fetched_at",
            max_age_hours=args.max_age_hours,
        )
    )

    if problems:
        print("ERROR: scheduled data freshness validation failed:")
        for problem in problems:
            print(f"  - {problem}")
        return 1

    print(
        f"Freshness validation passed for {len(city_keys)} cities "
        f"(maximum age: {args.max_age_hours:g}h)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
